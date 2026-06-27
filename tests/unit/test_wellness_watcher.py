"""Unit tests for the wellness CSV watcher (PR-W1).

Mirrors tests/unit/test_strength_watcher.py structure.

Covers ``process_csv_file``, ``process_directory`` (sorted glob), and
``watch_directory`` (threading.Event shutdown).

Spec scenarios: W1-7 (sorted order), W1-8 (graceful shutdown).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from ingestion.wellness.parser import WellnessRecord
from ingestion.wellness.watcher import (
    ProcessingSummary,
    process_csv_file,
    process_directory,
    watch_directory,
)

_HEADER = (
    "athlete_id,date,hrv,sleep_hours,resting_hr,steps,body_weight_kg,"
    "energy,soreness,mood,stress,perceived_recovery\n"
)
_VALID_ROW_1 = "A1,2025-03-01,65.0,7.5,52,9000,78.5,7,3,8,4,8\n"
_VALID_ROW_2 = "A2,2025-03-02,70.0,8.0,50,10000,75.0,8,2,9,3,9\n"


@dataclass
class _RecordingPublisher:
    """Captures publish() calls + flush flag; satisfies the publisher duck type."""

    published_records: list
    flushed: bool
    call_order: list[str]  # track which file's records arrived first

    def __init__(self) -> None:
        self.published_records = []
        self.flushed = False
        self.call_order = []

    def publish(self, record: WellnessRecord, **_kwargs) -> str:
        self.published_records.append(record)
        self.call_order.append(record.athlete_id)
        return "fake-event-id"

    def flush(self) -> None:
        self.flushed = True


# --- process_csv_file ---


def test_process_csv_file_publishes_all_valid_rows(tmp_path):
    """A fully-valid wellness CSV publishes every row and flushes the publisher."""
    csv_path = tmp_path / "wellness.csv"
    csv_path.write_text(_HEADER + _VALID_ROW_1 + _VALID_ROW_2)

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert isinstance(summary, ProcessingSummary)
    assert summary.published == 2
    assert summary.skipped == 0
    assert len(pub.published_records) == 2
    assert pub.flushed is True
    # prove real parsing ran
    assert pub.published_records[0].hrv == 65.0
    assert pub.published_records[1].sleep_hours == 8.0
    assert pub.published_records[0].athlete_id == "A1"


def test_process_csv_file_skips_malformed_rows_and_keeps_valid(tmp_path):
    """Malformed rows are skipped without aborting the file."""
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(
        _HEADER
        + _VALID_ROW_1  # valid
        + ",2025-03-02,65.0,7.5,52,9000,78.5,7,3,8,4,8\n"  # empty athlete_id
        + _VALID_ROW_2  # valid
    )

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert summary.published == 2
    assert summary.skipped == 1
    assert [r.athlete_id for r in pub.published_records] == ["A1", "A2"]


def test_process_csv_file_header_only_publishes_nothing(tmp_path):
    """A header-only CSV yields zero published and zero skipped, and still flushes."""
    csv_path = tmp_path / "empty.csv"
    csv_path.write_text(_HEADER)

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert summary.published == 0
    assert summary.skipped == 0
    assert pub.flushed is True


# --- process_directory (W1-7) ---


def test_process_directory_processes_csv_files_in_sorted_order(tmp_path):
    """W1-7: a.csv MUST be processed before b.csv (sorted order)."""
    # b.csv has A2, a.csv has A1; sorted order means a.csv goes first
    (tmp_path / "b.csv").write_text(_HEADER + _VALID_ROW_2)
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 2
    assert all(isinstance(s, ProcessingSummary) for s in summaries)
    # sorted order: a.csv first -> A1 published first
    assert pub.call_order[0] == "A1"
    assert pub.call_order[1] == "A2"


def test_process_directory_ignores_non_csv_files(tmp_path):
    """Non-CSV files are ignored."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    (tmp_path / "readme.txt").write_text("not a csv")
    (tmp_path / "data.json").write_text("{}")

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 1
    assert sum(s.published for s in summaries) == 1


# --- watch_directory (W1-8) ---


def test_watch_directory_exits_when_stop_event_set(tmp_path):
    """W1-8: watch_directory exits after stop_event.set() is called."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    pub = _RecordingPublisher()
    stop_event = threading.Event()

    # Set the stop event immediately — the loop should exit after the first (or current) poll
    stop_event.set()

    # Must complete quickly (no infinite loop)
    start = time.monotonic()
    watch_directory(tmp_path, pub, poll_interval=0.05, stop_event=stop_event)
    elapsed = time.monotonic() - start

    # Should exit well within 2 seconds
    assert elapsed < 2.0
