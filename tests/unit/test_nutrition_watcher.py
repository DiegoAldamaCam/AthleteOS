"""Unit tests for the nutrition CSV watcher (PR-N1).

Mirrors tests/unit/test_recovery_watcher.py structure.

Covers ``process_csv_file``, ``process_directory`` (sorted glob), and
``watch_directory`` (threading.Event shutdown).

Spec scenarios: sc-11 (sorted file processing), sc-12 (graceful shutdown).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from ingestion.nutrition.parser import NutritionRecord
from ingestion.nutrition.watcher import (
    ProcessingSummary,
    process_csv_file,
    process_directory,
    watch_directory,
)

_HEADER = "athlete_id,date,calories,protein_g,carbs_g,fat_g,adherence_score\n"
_VALID_ROW_1 = "A1,2025-06-01,2400,150.0,300.0,80.0,0.85\n"
_VALID_ROW_2 = "A2,2025-06-02,2200,140.0,280.0,75.0,0.90\n"


@dataclass
class _RecordingPublisher:
    """Captures publish() calls + flush flag; satisfies the publisher duck type."""

    published_records: list
    flushed: bool
    call_order: list[str]

    def __init__(self) -> None:
        self.published_records = []
        self.flushed = False
        self.call_order = []

    def publish(self, record: NutritionRecord, **_kwargs) -> str:
        self.published_records.append(record)
        self.call_order.append(record.athlete_id)
        return "fake-event-id"

    def flush(self) -> None:
        self.flushed = True


# --- process_csv_file ---


def test_process_csv_file_publishes_all_valid_rows(tmp_path):
    """A fully-valid nutrition CSV publishes every row and flushes the publisher."""
    csv_path = tmp_path / "nutrition.csv"
    csv_path.write_text(_HEADER + _VALID_ROW_1 + _VALID_ROW_2)

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert isinstance(summary, ProcessingSummary)
    assert summary.published == 2
    assert summary.skipped == 0
    assert len(pub.published_records) == 2
    assert pub.flushed is True
    # prove real parsing ran
    assert pub.published_records[0].calories == 2400
    assert pub.published_records[1].protein_g == 140.0
    assert pub.published_records[0].athlete_id == "A1"


def test_process_csv_file_skips_malformed_rows_and_keeps_valid(tmp_path):
    """Malformed rows are skipped without aborting the file."""
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(
        _HEADER
        + _VALID_ROW_1  # valid
        + ",2025-06-02,2200,140.0,280.0,75.0,0.90\n"  # empty athlete_id
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


# --- process_directory (sc-11) ---


def test_process_directory_processes_csv_files_in_sorted_order(tmp_path):
    """sc-11: a.csv MUST be processed before b.csv (sorted order)."""
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


def test_process_directory_returns_summary_per_file(tmp_path):
    """sc-11 triangulation: one ProcessingSummary returned per CSV file."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    (tmp_path / "b.csv").write_text(_HEADER + _VALID_ROW_2)

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 2
    assert all(s.published == 1 for s in summaries)


def test_process_directory_ignores_non_csv_files(tmp_path):
    """Non-CSV files are ignored."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    (tmp_path / "readme.txt").write_text("not a csv")
    (tmp_path / "data.json").write_text("{}")

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 1
    assert sum(s.published for s in summaries) == 1


# --- watch_directory (sc-12) ---


def test_watch_directory_exits_when_stop_event_set(tmp_path):
    """sc-12: watch_directory exits after stop_event.set() is called."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    pub = _RecordingPublisher()
    stop_event = threading.Event()

    # Set the stop event immediately — the loop should exit quickly
    stop_event.set()

    start = time.monotonic()
    watch_directory(tmp_path, pub, poll_interval=0.05, stop_event=stop_event)
    elapsed = time.monotonic() - start

    # Should exit well within 2 seconds
    assert elapsed < 2.0


def test_watch_directory_no_further_files_processed_after_stop(tmp_path):
    """sc-12 triangulation: after stop_event is set, no new files processed."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    pub = _RecordingPublisher()
    stop_event = threading.Event()

    # Stop immediately and run
    stop_event.set()
    watch_directory(tmp_path, pub, poll_interval=0.01, stop_event=stop_event)

    # After stopping, the count should not increase further
    count_after_stop = len(pub.published_records)
    time.sleep(0.1)
    assert len(pub.published_records) == count_after_stop


def test_watch_directory_survives_exception_in_process_directory(tmp_path):
    """An exception in process_directory must not kill the watcher thread."""
    call_count = 0
    stop_event = threading.Event()

    class _ExplodingPublisher:
        def publish(self, record, **kwargs) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("simulated bad poll cycle")
            return "fake-id"

        def flush(self) -> None:
            pass

    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    pub = _ExplodingPublisher()

    def _run():
        watch_directory(tmp_path, pub, poll_interval=0.05, stop_event=stop_event)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    time.sleep(0.3)
    stop_event.set()
    t.join(timeout=3.0)

    assert not t.is_alive(), "watcher thread should have exited cleanly"
    assert call_count > 1, "loop should have run more than once (survived exception)"
