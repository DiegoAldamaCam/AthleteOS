"""Unit tests for the planning directory watcher (PR-PL1).

Mirrors tests/unit/test_wellness_watcher.py structure.

Covers ``process_file`` (YAML/JSON dispatch), ``process_csv_file``,
``process_directory`` (sorted glob), and ``watch_directory``
(threading.Event graceful shutdown).

Spec scenario: PL1-9 (graceful shutdown via stop_event).
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

import pytest

from ingestion.planning.parser import PlanningRecord
from ingestion.planning.watcher import (
    ProcessingSummary,
    process_csv_file,
    process_directory,
    process_file,
    watch_directory,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_VALID_YAML = """\
athlete_id: A1
block_id: BLK-001
goal: Build aerobic base
start_date: "2025-06-01"
end_date: "2025-08-31"
planned_sessions_per_week: 5
weekly_volume_targets:
  strength: 3
  cardio: 2
"""

_VALID_JSON_CONTENT = json.dumps(
    {
        "athlete_id": "A2",
        "block_id": "BLK-002",
        "goal": "Endurance focus",
        "start_date": "2025-07-01",
        "end_date": "2025-09-30",
        "planned_sessions_per_week": 4,
        "weekly_volume_targets": {"cardio": 5},
    }
)

_CSV_HEADER = "athlete_id,block_id,goal,start_date,end_date,planned_sessions_per_week,weekly_volume_targets\n"
_VALID_CSV_ROW = 'A1,BLK-001,Build aerobic base,2025-06-01,2025-08-31,5,"{""strength"": 3, ""cardio"": 2}"\n'


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

    def publish(self, record: PlanningRecord, **_kwargs) -> str:
        self.published_records.append(record)
        self.call_order.append(record.athlete_id)
        return "fake-event-id"

    def flush(self) -> None:
        self.flushed = True


# ---------------------------------------------------------------------------
# process_file (YAML/JSON dispatch by extension)
# ---------------------------------------------------------------------------


def test_process_file_yaml_publishes_one_record(tmp_path):
    """process_file dispatches a .yaml file to the YAML parser and publishes."""
    yaml_path = tmp_path / "block.yaml"
    yaml_path.write_text(_VALID_YAML, encoding="utf-8")

    pub = _RecordingPublisher()
    summary = process_file(yaml_path, pub)

    assert isinstance(summary, ProcessingSummary)
    assert summary.published == 1
    assert summary.skipped == 0
    assert pub.published_records[0].athlete_id == "A1"
    assert pub.flushed is True


def test_process_file_json_publishes_one_record(tmp_path):
    """process_file dispatches a .json file to the JSON parser and publishes."""
    json_path = tmp_path / "block.json"
    json_path.write_text(_VALID_JSON_CONTENT, encoding="utf-8")

    pub = _RecordingPublisher()
    summary = process_file(json_path, pub)

    assert summary.published == 1
    assert summary.skipped == 0
    assert pub.published_records[0].athlete_id == "A2"


def test_process_file_unknown_extension_skips_gracefully(tmp_path):
    """An unknown file extension is skipped gracefully (0 published, 0 skipped)."""
    txt_path = tmp_path / "not_a_plan.txt"
    txt_path.write_text("some text", encoding="utf-8")

    pub = _RecordingPublisher()
    summary = process_file(txt_path, pub)

    assert summary.published == 0
    assert summary.skipped == 0


# ---------------------------------------------------------------------------
# process_csv_file
# ---------------------------------------------------------------------------


def test_process_csv_file_publishes_all_valid_rows(tmp_path):
    """A valid planning CSV publishes every row and flushes."""
    csv_path = tmp_path / "blocks.csv"
    # Use simple non-quoted wvt for simplicity in this test
    header = "athlete_id,block_id,goal,start_date,end_date,planned_sessions_per_week,weekly_volume_targets\n"
    row = 'A1,BLK-001,Build aerobic base,2025-06-01,2025-08-31,5,"{""strength"": 3}"\n'
    csv_path.write_text(header + row, encoding="utf-8")

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert summary.published == 1
    assert summary.skipped == 0
    assert pub.flushed is True
    assert pub.published_records[0].athlete_id == "A1"


def test_process_csv_file_skips_malformed_rows(tmp_path):
    """Malformed CSV rows are skipped, valid ones published."""
    header = "athlete_id,block_id,goal,start_date,end_date,planned_sessions_per_week,weekly_volume_targets\n"
    valid_row = 'A1,BLK-001,Goal,2025-06-01,2025-08-31,5,"{""s"": 1}"\n'
    bad_row = 'A2,BLK-002,Goal,2025-08-31,2025-06-01,5,"{""s"": 1}"\n'  # end < start
    csv_path = tmp_path / "mixed.csv"
    csv_path.write_text(header + valid_row + bad_row, encoding="utf-8")

    pub = _RecordingPublisher()
    summary = process_csv_file(csv_path, pub)

    assert summary.published == 1
    assert summary.skipped == 1


# ---------------------------------------------------------------------------
# process_directory
# ---------------------------------------------------------------------------


def test_process_directory_processes_yaml_and_json_in_sorted_order(tmp_path):
    """process_directory processes YAML/JSON files in sorted order."""
    # b.json has A2, a.yaml has A1 — sorted: a.yaml first
    (tmp_path / "b.json").write_text(_VALID_JSON_CONTENT, encoding="utf-8")
    (tmp_path / "a.yaml").write_text(_VALID_YAML, encoding="utf-8")

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 2
    # a.yaml comes first in sorted order → A1 published first
    assert pub.call_order[0] == "A1"
    assert pub.call_order[1] == "A2"


def test_process_directory_ignores_non_planning_files(tmp_path):
    """Non-YAML/JSON/CSV files are ignored."""
    (tmp_path / "a.yaml").write_text(_VALID_YAML, encoding="utf-8")
    (tmp_path / "readme.txt").write_text("not a plan", encoding="utf-8")
    (tmp_path / "data.xml").write_text("<data/>", encoding="utf-8")

    pub = _RecordingPublisher()
    summaries = process_directory(tmp_path, pub)

    assert len(summaries) == 1
    assert summaries[0].published == 1


# ---------------------------------------------------------------------------
# watch_directory — PL1-9 graceful shutdown
# ---------------------------------------------------------------------------


def test_watch_directory_exits_when_stop_event_set(tmp_path):
    """PL1-9: watch_directory exits after stop_event.set() is called."""
    (tmp_path / "a.yaml").write_text(_VALID_YAML, encoding="utf-8")
    pub = _RecordingPublisher()
    stop_event = threading.Event()

    # Set the stop event immediately — the loop should exit promptly
    stop_event.set()

    start = time.monotonic()
    watch_directory(tmp_path, pub, poll_interval=0.05, stop_event=stop_event)
    elapsed = time.monotonic() - start

    # Must exit well within 2 seconds
    assert elapsed < 2.0


def test_watch_directory_survives_exception_in_process_directory(tmp_path):
    """An exception in process_directory must not kill the watcher loop."""
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

    (tmp_path / "a.yaml").write_text(_VALID_YAML, encoding="utf-8")
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


def test_watch_directory_runs_loop_then_stops_cleanly(tmp_path):
    """watch_directory runs at least one full poll cycle before stop."""
    (tmp_path / "a.yaml").write_text(_VALID_YAML, encoding="utf-8")
    pub = _RecordingPublisher()
    stop_event = threading.Event()

    first_poll_done = threading.Event()

    import ingestion.planning.watcher as _watcher_mod
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

        assert first_poll_done.wait(timeout=3.0), "first poll cycle never ran"
        stop_event.set()
        t.join(timeout=3.0)
    finally:
        _watcher_mod.process_directory = original

    assert not t.is_alive(), "watcher thread should have exited cleanly"
    assert len(pub.published_records) >= 1, "at least one record published"
