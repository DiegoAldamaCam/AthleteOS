"""Unit tests for the strength raw-envelope producer (PR2, task 3.1/3.2).

Covers ``build_envelope`` (a PURE function - the raw JSON envelope builder) and
``StrengthProducer.publish`` (the thin Kafka side-effect wrapper).

Envelope shape per the event-contracts spec "Raw Topic JSON Shape":

    {
      "event_id":   "uuid-v4",
      "event_time":  "ISO-8601-string",   # derived from the CSV timestamp
      "ingest_time": "ISO-8601-string",   # wall-clock at ingestion
      "source":      "strong_csv",
      "athlete_id":  "<id>",              # also used as the Kafka message key
      "payload":     { workout_id, exercise_id, set_number, reps, weight_kg,
                       rpe, rir, timestamp }   # verbatim from CSV
    }

session_load is NOT part of this envelope - it is derived at canonicalization
(PR3). These tests therefore assert session_load is absent from the payload.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from ingestion.strength.parser import StrengthSetRecord
from ingestion.strength.producer import StrengthPublisher, build_envelope

from bootstrap._topology import RAW_TOPICS


def _record(rpe: float | None = 8.5, rir: float | None = 2.0) -> StrengthSetRecord:
    return StrengthSetRecord(
        athlete_id="athlete-123",
        workout_id="w-001",
        exercise_id="bench-press",
        set_number=1,
        reps=8,
        weight_kg=100.0,
        rpe=rpe,
        rir=rir,
        timestamp="2024-01-15 10:30:00",
    )


_FIXED_UUID = "11111111-1111-4111-8111-111111111111"
_FIXED_NOW = datetime(2024, 2, 1, 12, 0, 0, tzinfo=timezone.utc)


# --- build_envelope: shape + verbatim payload ---


def test_build_envelope_has_raw_envelope_shape_and_verbatim_payload():
    env = build_envelope(
        _record(),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW,
    )

    assert env["event_id"] == _FIXED_UUID
    assert env["source"] == "strong_csv"
    assert env["athlete_id"] == "athlete-123"
    # event_time is derived from the CSV timestamp and normalized to ISO-8601
    # (space separator -> 'T'), while the payload keeps the original verbatim.
    assert env["event_time"] == "2024-01-15T10:30:00"
    assert env["ingest_time"] == _FIXED_NOW.isoformat()
    assert env["payload"] == {
        "workout_id": "w-001",
        "exercise_id": "bench-press",
        "set_number": 1,
        "reps": 8,
        "weight_kg": 100.0,
        "rpe": 8.5,
        "rir": 2.0,
        "timestamp": "2024-01-15 10:30:00",  # original source string verbatim
    }


def test_build_envelope_payload_does_not_carry_session_load():
    """session_load is a canonicalization-layer field; it MUST NOT appear in the
    raw envelope payload (spec: 'computed at canonicalization')."""
    env = build_envelope(_record(), uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW)
    assert "session_load" not in env
    assert "session_load" not in env["payload"]


def test_build_envelope_default_event_id_is_valid_uuid_v4():
    """With the default uuid factory, event_id is a valid UUID v4 string."""
    env = build_envelope(_record(), now=lambda: _FIXED_NOW)
    parsed = uuid.UUID(env["event_id"])
    assert parsed.version == 4
    assert str(parsed) == env["event_id"]


def test_build_envelope_preserves_nullable_rpe_rir_as_none():
    """When rpe/rir are None (absent in source), payload carries None verbatim."""
    env = build_envelope(
        _record(rpe=None, rir=None),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW,
    )
    assert env["payload"]["rpe"] is None
    assert env["payload"]["rir"] is None


# --- StrengthProducer.publish: Kafka side-effect wrapper ---


class _FakeKafkaProducer:
    """Records produce() calls so we can assert topic/key/value without Kafka."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.flushed = False

    def produce(self, topic: str, value: str, key: str) -> None:
        self.calls.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_publisher_publish_uses_athlete_id_key_and_json_value_on_raw_strength():
    """publish() produces a JSON envelope to raw.strength keyed by athlete_id."""
    fake = _FakeKafkaProducer()
    publisher = StrengthPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    event_id = publisher.publish(
        _record(),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW,
    )

    assert event_id == _FIXED_UUID
    assert len(fake.calls) == 1
    topic, value, key = fake.calls[0]
    assert topic == "raw.strength"
    # topic must be part of the locked raw topology (traceability to bootstrap)
    assert "raw.strength" in RAW_TOPICS
    assert key == "athlete-123"  # co-partitioning key

    envelope = json.loads(value)
    assert envelope["event_id"] == _FIXED_UUID
    assert envelope["source"] == "strong_csv"
    assert envelope["athlete_id"] == "athlete-123"
    assert envelope["payload"]["reps"] == 8
    assert envelope["payload"]["weight_kg"] == 100.0
    assert "session_load" not in envelope


def test_publisher_flush_delegates_to_underlying_producer():
    """flush() delegates so callers can guarantee delivery before exit."""
    fake = _FakeKafkaProducer()
    publisher = StrengthPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    publisher.flush()

    assert fake.flushed is True
