"""Unit tests for the wellness raw-envelope producer (PR-W1).

Mirrors tests/unit/test_strength_producer.py structure.

Covers ``build_envelope`` (PURE function) and ``WellnessPublisher.publish``.

Critical divergence from strength (W1-5, spec-locked):
  event_time = UTC midnight epoch-ms LONG (e.g. 1740787200000 for 2025-03-01),
  NOT an ISO-8601 string like strength uses. This is intentional per spec.

Spec scenarios: W1-5, W1-6.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest

from ingestion.wellness.parser import WellnessRecord
from ingestion.wellness.producer import WellnessPublisher, build_envelope


def _record(
    athlete_id: str = "A1",
    date: str = "2025-03-01",
    hrv: float | None = 65.0,
    sleep_hours: float | None = 7.5,
    resting_hr: int | None = 52,
    steps: int | None = 9000,
    body_weight_kg: float | None = 78.5,
    energy: int | None = 7,
    soreness: int | None = 3,
    mood: int | None = 8,
    stress: int | None = 4,
    perceived_recovery: int | None = 8,
) -> WellnessRecord:
    return WellnessRecord(
        athlete_id=athlete_id,
        date=date,
        hrv=hrv,
        sleep_hours=sleep_hours,
        resting_hr=resting_hr,
        steps=steps,
        body_weight_kg=body_weight_kg,
        energy=energy,
        soreness=soreness,
        mood=mood,
        stress=stress,
        perceived_recovery=perceived_recovery,
    )


_FIXED_UUID = "22222222-2222-4222-8222-222222222222"
_FIXED_NOW_MS = 1740830400000  # arbitrary ingest_time in epoch-ms


# --- build_envelope: shape + epoch-ms event_time (W1-5) ---


def test_build_envelope_event_time_is_utc_midnight_epoch_ms():
    """W1-5: event_time for date='2025-03-01' MUST be 1740787200000 (epoch-ms LONG)."""
    record = _record(date="2025-03-01")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    # CRITICAL divergence from strength: epoch-ms integer, NOT ISO string
    assert env["event_time"] == 1740787200000
    assert isinstance(env["event_time"], int)


def test_build_envelope_has_correct_envelope_shape():
    """W1-5: Envelope has all required top-level fields."""
    record = _record(athlete_id="A1", date="2025-03-01")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_id"] == _FIXED_UUID
    assert env["source"] == "synthetic_wellness"
    assert env["athlete_id"] == "A1"
    assert env["event_time"] == 1740787200000
    assert env["ingest_time"] == _FIXED_NOW_MS
    # payload carries source fields verbatim
    assert "payload" in env
    payload = env["payload"]
    assert payload["athlete_id"] == "A1"
    assert payload["date"] == "2025-03-01"
    assert payload["hrv"] == 65.0
    assert payload["sleep_hours"] == 7.5


def test_build_envelope_payload_contains_all_source_fields():
    """W1-5: Payload is verbatim source fields."""
    record = _record()
    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    payload = env["payload"]

    expected_keys = {
        "athlete_id", "date", "hrv", "sleep_hours", "resting_hr",
        "steps", "body_weight_kg", "energy", "soreness", "mood",
        "stress", "perceived_recovery",
    }
    assert set(payload.keys()) == expected_keys


def test_build_envelope_nullable_fields_preserved_as_none():
    """W1-5: None values in record appear as None in payload verbatim."""
    record = _record(hrv=None, sleep_hours=None, perceived_recovery=None)
    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)

    assert env["payload"]["hrv"] is None
    assert env["payload"]["sleep_hours"] is None
    assert env["payload"]["perceived_recovery"] is None


def test_build_envelope_default_event_id_is_valid_uuid_v4():
    """With the default uuid_factory, event_id is a valid UUID v4 string."""
    env = build_envelope(_record(), now=lambda: _FIXED_NOW_MS)
    parsed = uuid.UUID(env["event_id"])
    assert parsed.version == 4


# --- build_envelope: injectable callables (W1-6) ---


def test_build_envelope_injectable_now_and_uuid_factory():
    """W1-6: ingest_time and event_id come from the injected callables."""
    fixed_ingest = 9999999999999
    fixed_uuid = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    env = build_envelope(
        _record(),
        now=lambda: fixed_ingest,
        uuid_factory=lambda: fixed_uuid,
    )

    assert env["ingest_time"] == fixed_ingest
    assert env["event_id"] == fixed_uuid


def test_build_envelope_different_dates_produce_different_epoch_ms():
    """W1-6 triangulation: different dates produce different epoch-ms values."""
    env1 = build_envelope(_record(date="2025-01-01"), now=lambda: _FIXED_NOW_MS)
    env2 = build_envelope(_record(date="2025-03-01"), now=lambda: _FIXED_NOW_MS)

    assert env1["event_time"] != env2["event_time"]
    # 2025-01-01 UTC midnight
    assert env1["event_time"] == 1735689600000


# --- WellnessPublisher: Kafka side-effect wrapper ---


class _FakeKafkaProducer:
    """Records produce() calls so we can assert topic/key/value without Kafka."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.flushed = False

    def produce(self, topic: str, value: str, key: str) -> None:
        self.calls.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_wellness_publisher_uses_athlete_id_key_on_raw_wellness():
    """WellnessPublisher.publish produces to raw.wellness keyed by athlete_id."""
    fake = _FakeKafkaProducer()
    publisher = WellnessPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    event_id = publisher.publish(
        _record(athlete_id="A1"),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert event_id == _FIXED_UUID
    assert len(fake.calls) == 1
    topic, value, key = fake.calls[0]
    assert topic == "raw.wellness"
    assert key == "A1"

    envelope = json.loads(value)
    assert envelope["event_id"] == _FIXED_UUID
    assert envelope["athlete_id"] == "A1"
    assert envelope["event_time"] == 1740787200000  # epoch-ms, not ISO string


def test_wellness_publisher_flush_delegates_to_underlying_producer():
    """flush() delegates to the underlying producer."""
    fake = _FakeKafkaProducer()
    publisher = WellnessPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    publisher.flush()

    assert fake.flushed is True
