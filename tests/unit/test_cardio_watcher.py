"""Unit tests for the cardio CSV watcher (PR-C1).

Mirrors tests/unit/test_wellness_watcher.py structure exactly.

Covers ``process_csv_file``, ``process_directory`` (sorted glob), and
``watch_directory`` (threading.Event shutdown).

Spec scenarios: sc-11 (sorted file processing), sc-12 (graceful shutdown).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from ingestion.cardio.parser import CardioRecord
from ingestion.cardio.watcher import (
    ProcessingSummary,
    process_csv_file,
    process_directory,
    watch_directory,
)

_HEADER = "athlete_id,activity_type,duration_sec,timestamp,distance_km,avg_hr,tss\n"
_VALID_ROW_1 = "A1,Run,3600,2025-06-01T10:00:00,10.5,145,85.0\n"
_VALID_ROW_2 = "A2,Ride,7200,2025-06-02T08:00:00,50.0,160,120.0\n"


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

    def publish(self, record: CardioRecord, **_kwargs) -> str:
        self.published_records.append(record)
        self.call_order.append(record.athlete_id)
        return "fake-event-id"

    def flush(self) -> None:
        self.flushed = True


# --- process_csv_file ---


def test_process_csv_file_publishes_all_valid_rows(tmp_path):
    """A fully-valid cardio CSV publishes every row and flushes the publisher."""
    csv_path = tmp_path / "cardio.csv"
    csv_path.write_text(_HEADER + _VALID_ROW_1 + _VALID_ROW_2)

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert isinstance(summary, ProcessingSummary)
    assert summary.published == 2
    assert summary.skipped == 0
    assert len(pub.published_records) == 2
    assert pub.flushed is True
    # prove real parsing ran
    assert pub.published_records[0].activity_type == "Run"
    assert pub.published_records[1].duration_sec == 7200
    assert pub.published_records[0].athlete_id == "A1"


def test_process_csv_file_skips_malformed_rows_and_keeps_valid(tmp_path):
    """Malformed rows are skipped without aborting the file."""
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(
        _HEADER
        + _VALID_ROW_1                                           # valid
        + ",Run,3600,2025-06-01T10:00:00,10.5,145,85.0\n"       # empty athlete_id
        + _VALID_ROW_2                                           # valid
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


# --- sc-11: Sorted file processing ---


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


def test_process_directory_ignores_non_csv_files(tmp_path):
    """sc-11 triangulation: non-CSV files are ignored."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    (tmp_path / "readme.txt").write_text("not a csv")
    (tmp_path / "data.json").write_text("{}")

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 1
    assert sum(s.published for s in summaries) == 1


# --- sc-12: Graceful shutdown via stop_event ---


def test_watch_directory_exits_when_stop_event_set(tmp_path):
    """sc-12: watch_directory exits after stop_event.set() is called."""
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


def test_watch_directory_survives_exception_in_process_directory(tmp_path):
    """sc-12 triangulation: an exception in process_directory must not kill the watcher thread."""
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

    # Give the loop a moment to execute at least two poll cycles
    time.sleep(0.3)
    stop_event.set()
    t.join(timeout=3.0)

    assert not t.is_alive(), "watcher thread should have exited cleanly"
    # call_count > 1 proves the loop continued after the first exception
    assert call_count > 1, "loop should have run more than once (survived exception)"


def test_watch_directory_runs_loop_then_stops_cleanly(tmp_path):
    """sc-12 triangulation: watch_directory runs at least one full poll cycle before stop."""
    (tmp_path / "a.csv").write_text(_HEADER + _VALID_ROW_1)
    pub = _RecordingPublisher()
    stop_event = threading.Event()

    first_poll_done = threading.Event()

    import ingestion.cardio.watcher as _watcher_mod

    original = _watcher_mod.process_directory

    def _patched(dir_path, publisher, uuid_factory=None, now=None):
        result = original(dir_path, publisher, uuid_factory, now)
        first_poll_done.set()
        return result

    _watcher_mod.process_directory = _patched
    try:
        def _run():
            watch_directory(tmp_path, pub, poll_interval=0.05, stop_event=stop_event)

        t = threading.Thread(target=_run, daemon=True)
        t.start()

        # Wait until at least one poll ran, then stop
        assert first_poll_done.wait(timeout=3.0), "first poll cycle never ran"
        stop_event.set()
        t.join(timeout=3.0)
    finally:
        _watcher_mod.process_directory = original

    assert not t.is_alive(), "watcher thread should have exited cleanly"
    assert len(pub.published_records) >= 1, "at least one record published in running poll"
