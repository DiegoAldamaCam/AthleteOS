"""Unit tests for the cardio raw-envelope producer (PR-C1).

Mirrors tests/unit/test_wellness_producer.py structure.

Covers ``build_envelope`` (PURE function) and ``CardioPublisher.publish``.

event_time is the epoch-ms long derived from the ``timestamp`` ISO datetime field (W1-5 spec).

Spec scenarios: sc-9, sc-10.
"""

from __future__ import annotations

import json
import uuid

import pytest

from ingestion.cardio.parser import CardioRecord
from ingestion.cardio.producer import CardioPublisher, build_envelope


def _record(
    athlete_id: str = "A1",
    activity_type: str = "Run",
    duration_sec: int = 3600,
    timestamp: str = "2025-06-01T10:00:00",
    distance_km: float | None = 10.5,
    avg_hr: int | None = 145,
    tss: float | None = 85.0,
) -> CardioRecord:
    return CardioRecord(
        athlete_id=athlete_id,
        activity_type=activity_type,
        duration_sec=duration_sec,
        timestamp=timestamp,
        distance_km=distance_km,
        avg_hr=avg_hr,
        tss=tss,
    )


_FIXED_UUID = "33333333-3333-4333-8333-333333333333"
_FIXED_NOW_MS = 1748880000000  # arbitrary ingest_time in epoch-ms

# 2025-06-01T10:00:00 UTC → epoch-ms = 1748772000000
_EXPECTED_EVENT_TIME = 1748772000000


# --- sc-9: Envelope shape — required fields present ---


def test_build_envelope_has_correct_top_level_keys():
    """sc-9: Envelope must have exactly the required top-level keys."""
    record = _record()
    env = build_envelope(
        record,
        source="test_source",
        now=lambda: _FIXED_NOW_MS,
        uuid_factory=lambda: _FIXED_UUID,
    )

    assert set(env.keys()) == {
        "event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"
    }


def test_build_envelope_event_time_is_epoch_ms_from_timestamp():
    """sc-9: event_time for '2025-06-01T10:00:00' UTC MUST be correct epoch-ms long."""
    record = _record(timestamp="2025-06-01T10:00:00")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_time"] == _EXPECTED_EVENT_TIME
    assert isinstance(env["event_time"], int)


def test_build_envelope_event_id_is_non_empty_string():
    """sc-9: event_id must be a non-empty string (UUID)."""
    record = _record(athlete_id="A1")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_id"] == _FIXED_UUID
    assert isinstance(env["event_id"], str)
    assert len(env["event_id"]) > 0


def test_build_envelope_payload_contains_all_source_fields():
    """sc-9: Payload carries all source fields verbatim."""
    record = _record()
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )
    payload = env["payload"]

    expected_keys = {
        "athlete_id", "activity_type", "duration_sec", "timestamp",
        "distance_km", "avg_hr", "tss",
    }
    assert set(payload.keys()) == expected_keys
    assert payload["athlete_id"] == "A1"
    assert payload["activity_type"] == "Run"
    assert payload["duration_sec"] == 3600
    assert payload["timestamp"] == "2025-06-01T10:00:00"
    assert payload["distance_km"] == 10.5
    assert payload["avg_hr"] == 145
    assert payload["tss"] == 85.0


def test_build_envelope_athlete_id_is_top_level_partition_key():
    """sc-9 triangulation: athlete_id appears both top-level and in payload."""
    record = _record(athlete_id="Z9")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["athlete_id"] == "Z9"
    assert env["payload"]["athlete_id"] == "Z9"


# --- sc-10: Injectable callables for deterministic tests ---


def test_build_envelope_injectable_now_sets_ingest_time():
    """sc-10: ingest_time comes from the injected now callable."""
    fixed_ingest = 9876543210000
    env = build_envelope(
        _record(),
        now=lambda: fixed_ingest,
        uuid_factory=lambda: _FIXED_UUID,
    )

    assert env["ingest_time"] == fixed_ingest


def test_build_envelope_injectable_uuid_factory_sets_event_id():
    """sc-10: event_id comes from the injected uuid_factory callable."""
    fixed_uuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    env = build_envelope(
        _record(),
        now=lambda: _FIXED_NOW_MS,
        uuid_factory=lambda: fixed_uuid,
    )

    assert env["event_id"] == fixed_uuid


def test_build_envelope_different_timestamps_produce_different_epoch_ms():
    """sc-10 triangulation: different timestamps produce different epoch-ms values."""
    env1 = build_envelope(_record(timestamp="2025-01-01T00:00:00"), now=lambda: _FIXED_NOW_MS)
    env2 = build_envelope(_record(timestamp="2025-06-01T10:00:00"), now=lambda: _FIXED_NOW_MS)

    assert env1["event_time"] != env2["event_time"]
    # 2025-01-01T00:00:00 UTC midnight
    assert env1["event_time"] == 1735689600000


def test_build_envelope_default_event_id_is_valid_uuid_v4():
    """With the default uuid_factory, event_id is a valid UUID v4 string."""
    env = build_envelope(_record(), now=lambda: _FIXED_NOW_MS)
    parsed = uuid.UUID(env["event_id"])
    assert parsed.version == 4


def test_build_envelope_nullable_fields_preserved_as_none_in_payload():
    """sc-10 triangulation: None values in record appear as None in payload."""
    record = _record(distance_km=None, avg_hr=None, tss=None)
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["payload"]["distance_km"] is None
    assert env["payload"]["avg_hr"] is None
    assert env["payload"]["tss"] is None


def test_build_envelope_all_nullable_none_roundtrips_json():
    """All nullable fields = None must json.dumps and round-trip successfully."""
    record = _record(distance_km=None, avg_hr=None, tss=None)
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    serialized = json.dumps(env)
    restored = json.loads(serialized)

    for field in ["distance_km", "avg_hr", "tss"]:
        assert restored["payload"][field] is None, (
            f"payload.{field} should round-trip as null/None"
        )


# --- CardioPublisher: Kafka side-effect wrapper ---


class _FakeKafkaProducer:
    """Records produce() calls so we can assert topic/key/value without Kafka."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.flushed = False

    def produce(self, topic: str, value: str, key: str) -> None:
        self.calls.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_cardio_publisher_uses_athlete_id_key_on_raw_cardio():
    """CardioPublisher.publish produces to raw.cardio keyed by athlete_id."""
    fake = _FakeKafkaProducer()
    publisher = CardioPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    event_id = publisher.publish(
        _record(athlete_id="A1"),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert event_id == _FIXED_UUID
    assert len(fake.calls) == 1
    topic, value, key = fake.calls[0]
    assert topic == "raw.cardio"
    assert key == "A1"

    envelope = json.loads(value)
    assert envelope["event_id"] == _FIXED_UUID
    assert envelope["athlete_id"] == "A1"
    assert envelope["event_time"] == _EXPECTED_EVENT_TIME


def test_cardio_publisher_flush_delegates_to_underlying_producer():
    """flush() delegates to the underlying producer."""
    fake = _FakeKafkaProducer()
    publisher = CardioPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    publisher.flush()

    assert fake.flushed is True
