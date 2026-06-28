"""Unit tests for the recovery raw-envelope producer (PR-R1).

Mirrors tests/unit/test_wellness_producer.py structure.

Covers ``build_envelope`` (PURE function) and ``RecoveryPublisher.publish``.

event_time formula (W1-5, mirrors wellness):
  event_time = int(datetime.fromisoformat(record.date + "T00:00:00+00:00").timestamp() * 1000)

Spec scenarios: sc-7..sc-9.
"""

from __future__ import annotations

import json
import uuid

import pytest

from ingestion.recovery.parser import RecoveryRecord
from ingestion.recovery.producer import (
    DEFAULT_SOURCE,
    DEFAULT_TOPIC,
    RecoveryPublisher,
    build_envelope,
)


def _record(
    athlete_id: str = "A1",
    date: str = "2025-06-01",
    sleep_hours: float | None = 7.5,
    resting_hr: int | None = 58,
    hrv: float | None = 42.0,
    steps: int | None = 8500,
    body_weight_kg: float | None = 72.3,
) -> RecoveryRecord:
    return RecoveryRecord(
        athlete_id=athlete_id,
        date=date,
        sleep_hours=sleep_hours,
        resting_hr=resting_hr,
        hrv=hrv,
        steps=steps,
        body_weight_kg=body_weight_kg,
    )


_FIXED_UUID = "33333333-3333-4333-8333-333333333333"
_FIXED_NOW_MS = 1751000000000  # arbitrary ingest_time epoch-ms
# 2025-06-01 UTC midnight epoch-ms
_EXPECTED_EVENT_TIME_20250601 = 1748736000000


# --- Module-level constants ---


def test_default_source_is_apple_health():
    """DEFAULT_SOURCE must be 'apple_health'."""
    assert DEFAULT_SOURCE == "apple_health"


def test_default_topic_is_raw_recovery():
    """DEFAULT_TOPIC must be 'raw.recovery'."""
    assert DEFAULT_TOPIC == "raw.recovery"


# --- build_envelope: required fields present (sc-7) ---


def test_build_envelope_event_time_is_utc_midnight_epoch_ms():
    """sc-7: event_time for date='2025-06-01' MUST be the UTC midnight epoch-ms long."""
    record = _record(date="2025-06-01")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_time"] == _EXPECTED_EVENT_TIME_20250601
    assert isinstance(env["event_time"], int)


def test_build_envelope_has_correct_envelope_shape():
    """sc-7: Envelope has all required top-level fields."""
    record = _record(athlete_id="A1", date="2025-06-01")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_id"] == _FIXED_UUID
    assert env["source"] == "apple_health"
    assert env["athlete_id"] == "A1"
    assert env["event_time"] == _EXPECTED_EVENT_TIME_20250601
    assert env["ingest_time"] == _FIXED_NOW_MS
    assert "payload" in env


def test_build_envelope_exact_top_level_key_set():
    """sc-7: Raw envelope must have exactly these top-level keys — no event_type."""
    env = build_envelope(
        _record(),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert set(env.keys()) == {
        "event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"
    }


def test_build_envelope_payload_contains_all_source_fields():
    """sc-7: Payload carries all 5 Apple Health fields (no event_type in payload)."""
    env = build_envelope(_record(), uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    payload = env["payload"]

    expected_keys = {
        "athlete_id", "date", "sleep_hours", "resting_hr", "hrv", "steps", "body_weight_kg"
    }
    assert set(payload.keys()) == expected_keys


def test_build_envelope_payload_has_no_event_type():
    """sc-7: payload must NOT contain event_type — it is hardcoded by the canonicalize job."""
    env = build_envelope(_record(), uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    assert "event_type" not in env["payload"]
    assert "event_type" not in env


# --- build_envelope: injectable callables (sc-8) ---


def test_build_envelope_injectable_now_and_uuid_factory():
    """sc-8: ingest_time and event_id come from the injected callables."""
    fixed_ingest = 9999888877776666
    fixed_uuid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    env = build_envelope(
        _record(),
        now=lambda: fixed_ingest,
        uuid_factory=lambda: fixed_uuid,
    )

    assert env["ingest_time"] == fixed_ingest
    assert env["event_id"] == fixed_uuid


def test_build_envelope_different_dates_produce_different_epoch_ms():
    """sc-8 triangulation: different dates produce different epoch-ms event_time values."""
    env1 = build_envelope(_record(date="2025-01-01"), now=lambda: _FIXED_NOW_MS)
    env2 = build_envelope(_record(date="2025-06-01"), now=lambda: _FIXED_NOW_MS)

    assert env1["event_time"] != env2["event_time"]
    # 2025-01-01 UTC midnight
    assert env1["event_time"] == 1735689600000


def test_build_envelope_default_event_id_is_valid_uuid_v4():
    """With the default uuid_factory, event_id is a valid UUID v4 string."""
    env = build_envelope(_record(), now=lambda: _FIXED_NOW_MS)
    parsed = uuid.UUID(env["event_id"])
    assert parsed.version == 4


# --- build_envelope: nullable fields preserved (sc-9) ---


def test_build_envelope_nullable_fields_preserved_as_none():
    """sc-9: None values in record appear as None in payload verbatim."""
    record = _record(hrv=None, sleep_hours=None, resting_hr=None, steps=None, body_weight_kg=None)
    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)

    assert env["payload"]["hrv"] is None
    assert env["payload"]["sleep_hours"] is None
    assert env["payload"]["resting_hr"] is None
    assert env["payload"]["steps"] is None
    assert env["payload"]["body_weight_kg"] is None


def test_build_envelope_all_nullable_fields_none_roundtrips_json():
    """sc-9 triangulation: None fields round-trip through JSON as null."""
    record = _record(sleep_hours=None, resting_hr=None, hrv=None, steps=None, body_weight_kg=None)
    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)

    serialized = json.dumps(env)
    restored = json.loads(serialized)

    for field in ("sleep_hours", "resting_hr", "hrv", "steps", "body_weight_kg"):
        assert restored["payload"][field] is None, (
            f"payload.{field} should round-trip as null/None"
        )


# --- RecoveryPublisher: Kafka side-effect wrapper ---


class _FakeKafkaProducer:
    """Records produce() calls for assertions without real Kafka."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.flushed = False

    def produce(self, topic: str, value: str, key: str) -> None:
        self.calls.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_recovery_publisher_produces_to_raw_recovery_keyed_by_athlete_id():
    """RecoveryPublisher.publish produces to raw.recovery keyed by athlete_id."""
    fake = _FakeKafkaProducer()
    publisher = RecoveryPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    event_id = publisher.publish(
        _record(athlete_id="A1"),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert event_id == _FIXED_UUID
    assert len(fake.calls) == 1
    topic, value, key = fake.calls[0]
    assert topic == "raw.recovery"
    assert key == "A1"

    envelope = json.loads(value)
    assert envelope["event_id"] == _FIXED_UUID
    assert envelope["athlete_id"] == "A1"
    assert envelope["event_time"] == _EXPECTED_EVENT_TIME_20250601
    assert isinstance(envelope["event_time"], int)


def test_recovery_publisher_flush_delegates_to_underlying_producer():
    """flush() delegates to the underlying producer."""
    fake = _FakeKafkaProducer()
    publisher = RecoveryPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    publisher.flush()

    assert fake.flushed is True
