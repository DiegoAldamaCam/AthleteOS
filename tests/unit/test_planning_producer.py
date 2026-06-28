"""Unit tests for the planning raw-envelope producer (PR-PL1).

Mirrors tests/unit/test_wellness_producer.py structure.

Covers ``build_envelope`` (PURE function) and ``PlanningPublisher.publish``.

event_time is UTC midnight epoch-ms of start_date (same pattern as wellness).

Spec scenarios: PL1-8 (raw envelope shape).
"""

from __future__ import annotations

import json
import uuid

import pytest

from ingestion.planning.parser import PlanningRecord
from ingestion.planning.producer import PlanningPublisher, build_envelope


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _record(
    athlete_id: str = "A1",
    block_id: str = "BLK-001",
    goal: str = "Build aerobic base",
    start_date: str = "2025-06-01",
    end_date: str = "2025-08-31",
    planned_sessions_per_week: int = 5,
    weekly_volume_targets: str = '{"strength": 3, "cardio": 2}',
) -> PlanningRecord:
    return PlanningRecord(
        athlete_id=athlete_id,
        block_id=block_id,
        goal=goal,
        start_date=start_date,
        end_date=end_date,
        planned_sessions_per_week=planned_sessions_per_week,
        weekly_volume_targets=weekly_volume_targets,
    )


_FIXED_UUID = "33333333-3333-4333-8333-333333333333"
_FIXED_NOW_MS = 1750000000000  # arbitrary ingest_time epoch-ms


# ---------------------------------------------------------------------------
# Task 1.11 — build_envelope shape (PL1-8)
# ---------------------------------------------------------------------------


def test_build_envelope_event_time_is_utc_midnight_epoch_ms_of_start_date():
    """PL1-8: event_time for start_date='2025-06-01' MUST be 1748736000000 (epoch-ms)."""
    record = _record(start_date="2025-06-01")
    env = build_envelope(
        record,
        source="test_source",
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    # 2025-06-01 UTC midnight = 1748736000000
    assert env["event_time"] == 1748736000000
    assert isinstance(env["event_time"], int)


def test_build_envelope_key_is_athlete_id():
    """PL1-8: Kafka key must be athlete_id (ADR-4 co-partitioning)."""
    record = _record(athlete_id="A1")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["athlete_id"] == "A1"


def test_build_envelope_has_non_empty_event_id():
    """PL1-8: event_id must be a non-empty UUID string."""
    record = _record()
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_id"] == _FIXED_UUID
    assert isinstance(env["event_id"], str)
    assert len(env["event_id"]) > 0


def test_build_envelope_ingest_time_comes_from_now_callable():
    """PL1-8: ingest_time must equal the value returned by the injectable now callable."""
    fixed_now = 9876543210000
    record = _record()
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: fixed_now,
    )

    assert env["ingest_time"] == fixed_now


def test_build_envelope_has_all_required_top_level_keys():
    """PL1-8: envelope must have exactly the required top-level keys."""
    env = build_envelope(
        _record(),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert set(env.keys()) == {
        "event_id",
        "event_time",
        "ingest_time",
        "source",
        "athlete_id",
        "payload",
    }


def test_build_envelope_payload_contains_all_7_planning_fields():
    """PL1-8: payload carries all 7 PlanningRecord fields."""
    record = _record()
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    payload = env["payload"]
    expected_keys = {
        "athlete_id",
        "block_id",
        "goal",
        "start_date",
        "end_date",
        "planned_sessions_per_week",
        "weekly_volume_targets",
    }
    assert set(payload.keys()) == expected_keys
    assert payload["athlete_id"] == "A1"
    assert payload["block_id"] == "BLK-001"
    assert payload["planned_sessions_per_week"] == 5
    assert payload["weekly_volume_targets"] == '{"strength": 3, "cardio": 2}'


def test_build_envelope_different_start_dates_produce_different_event_times():
    """PL1-8 triangulation: different start_dates must produce different epoch-ms values."""
    env1 = build_envelope(_record(start_date="2025-01-01"), now=lambda: _FIXED_NOW_MS)
    env2 = build_envelope(_record(start_date="2025-06-01"), now=lambda: _FIXED_NOW_MS)

    assert env1["event_time"] != env2["event_time"]
    # 2025-01-01 UTC midnight
    assert env1["event_time"] == 1735689600000


def test_build_envelope_default_uuid_is_valid_v4():
    """Default uuid_factory generates a valid UUID v4 string."""
    env = build_envelope(_record(), now=lambda: _FIXED_NOW_MS)
    parsed = uuid.UUID(env["event_id"])
    assert parsed.version == 4


# ---------------------------------------------------------------------------
# Task 1.13 — wvt fidelity through parser → envelope
# ---------------------------------------------------------------------------


def test_build_envelope_wvt_round_trip_from_parser():
    """PL1-7/PL1-8: wvt from parser stored in payload survives json.loads round-trip."""
    from ingestion.planning.parser import parse_yaml

    yaml_content = """\
athlete_id: A1
block_id: BLK-001
goal: Build aerobic base
start_date: "2025-06-01"
end_date: "2025-08-31"
planned_sessions_per_week: 5
weekly_volume_targets:
  strength: 3
  cardio: 2
  endurance: 1
"""
    result = parse_yaml(yaml_content)
    record = result.records[0]

    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    wvt_in_payload = env["payload"]["weekly_volume_targets"]

    original_dict = {"strength": 3, "cardio": 2, "endurance": 1}
    assert json.loads(wvt_in_payload) == original_dict


# ---------------------------------------------------------------------------
# PlanningPublisher — Kafka side-effect wrapper
# ---------------------------------------------------------------------------


class _FakeKafkaProducer:
    """Records produce() calls so we can assert topic/key/value without Kafka."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.flushed = False

    def produce(self, topic: str, value: str, key: str) -> None:
        self.calls.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_planning_publisher_uses_athlete_id_key_on_raw_planning():
    """PlanningPublisher.publish produces to raw.planning keyed by athlete_id."""
    fake = _FakeKafkaProducer()
    publisher = PlanningPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    event_id = publisher.publish(
        _record(athlete_id="A1"),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert event_id == _FIXED_UUID
    assert len(fake.calls) == 1
    topic, value, key = fake.calls[0]
    assert topic == "raw.planning"
    assert key == "A1"

    envelope = json.loads(value)
    assert envelope["event_id"] == _FIXED_UUID
    assert envelope["athlete_id"] == "A1"
    assert envelope["event_time"] == 1748736000000  # 2025-06-01 UTC midnight epoch-ms


def test_planning_publisher_flush_delegates_to_underlying_producer():
    """flush() delegates to the underlying producer."""
    fake = _FakeKafkaProducer()
    publisher = PlanningPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    publisher.flush()

    assert fake.flushed is True
