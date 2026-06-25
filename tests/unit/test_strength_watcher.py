"""Unit tests for the strength CSV watcher (PR2, task 3.1).

Covers ``process_csv_file`` (read a CSV -> parse -> publish each record) and
``process_directory`` (scan a directory for *.csv and process each once). The
continuous ``watch_directory`` polling loop is a thin shell over
``process_directory`` and is exercised end-to-end by the Docker-gated
integration test rather than by sleeps here.

A recording publisher fake captures the parsed records so the tests prove real
parsing + publishing ran (specific field values, counts) without a Kafka broker.
"""

from __future__ import annotations

from dataclasses import dataclass

from ingestion.strength.parser import StrengthSetRecord
from ingestion.strength.watcher import (
    ProcessingSummary,
    process_csv_file,
    process_directory,
)


_HEADER = (
    "athlete_id,workout_id,exercise_id,set_number,reps,weight_kg,rpe,rir,timestamp\n"
)
_VALID_ROW_1 = (
    "athlete-123,w-001,bench-press,1,8,100,8.5,2,2024-01-15T10:30:00\n"
)
_VALID_ROW_2 = (
    "athlete-123,w-001,bench-press,2,6,105,9,1,2024-01-15T10:35:00\n"
)


@dataclass
class _RecordingPublisher:
    """Captures publish() calls + flush flag; satisfies the publisher duck type."""

    published_records: list[StrengthSetRecord]
    flushed: bool

    def __init__(self) -> None:
        self.published_records = []
        self.flushed = False

    def publish(self, record: StrengthSetRecord, **_kwargs) -> str:
        self.published_records.append(record)
        return "fake-event-id"

    def flush(self) -> None:
        self.flushed = True


# --- process_csv_file ---


def test_process_csv_file_publishes_all_valid_rows(tmp_path):
    """A fully-valid CSV publishes every row and flushes the publisher."""
    csv_path = tmp_path / "workout.csv"
    csv_path.write_text(_HEADER + _VALID_ROW_1 + _VALID_ROW_2)

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert isinstance(summary, ProcessingSummary)
    assert summary.published == 2
    assert summary.skipped == 0
    assert len(pub.published_records) == 2
    assert pub.flushed is True
    # prove real parsing ran (specific field values, not just a count)
    assert pub.published_records[0].reps == 8
    assert pub.published_records[1].weight_kg == 105.0
    assert pub.published_records[1].set_number == 2


def test_process_csv_file_skips_malformed_rows_and_keeps_valid(tmp_path):
    """Malformed rows are skipped (counted) without aborting the file; valid
    rows around them are still published."""
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(
        _HEADER
        + _VALID_ROW_1  # valid
        + "athlete-123,w-001,bench-press,2,heavy,105,9,1,2024-01-15T10:35:00\n"  # bad reps
        + ",w-001,bench-press,3,5,90,,1,2024-01-15T10:40:00\n"  # empty athlete_id
        + "athlete-123,w-001,bench-press,4,5,90,,1,2024-01-15T10:45:00\n"  # valid, rpe absent
    )

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert summary.published == 2
    assert summary.skipped == 2
    assert [r.set_number for r in pub.published_records] == [1, 4]
    # the rpe-absent valid row is preserved as None
    assert pub.published_records[1].rpe is None


def test_process_csv_file_header_only_publishes_nothing(tmp_path):
    """A header-only CSV yields zero published and zero skipped, and still flushes."""
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text(_HEADER)

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert summary.published == 0
    assert summary.skipped == 0
    assert pub.flushed is True


# --- process_directory ---


def test_process_directory_processes_all_csv_files_once(tmp_path):
    """process_directory scans the directory and processes every *.csv exactly
    once, ignoring non-CSV files."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    (tmp_path / "b.csv").write_text(_HEADER + _VALID_ROW_2)
    (tmp_path / "ignore.txt").write_text("not a csv")
    (tmp_path / "skip.json").write_text("{}")

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 2  # only the two .csv files
    assert all(isinstance(s, ProcessingSummary) for s in summaries)
    assert sum(s.published for s in summaries) == 2
    assert sum(s.skipped for s in summaries) == 0
    assert len(pub.published_records) == 2
