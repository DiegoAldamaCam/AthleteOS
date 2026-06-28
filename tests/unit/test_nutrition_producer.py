"""Unit tests for the nutrition raw-envelope producer (PR-N1).

Mirrors tests/unit/test_recovery_producer.py structure.

Covers ``build_envelope`` (PURE function) and ``NutritionPublisher.publish``.

event_time formula (W1-5, mirrors recovery/wellness):
  event_time = int(datetime.fromisoformat(record.date + "T00:00:00+00:00").timestamp() * 1000)

Spec scenarios: sc-8..sc-10.

CRITICAL (sc-8, ADR-N2): payload key MUST be ``adherence_score`` (source-faithful).
The rename adherence_score -> nutrition_adherence happens ONLY in the
canonicalize transform (PR-N2), NOT here.
"""

from __future__ import annotations

import json
import uuid

import pytest

from ingestion.nutrition.parser import NutritionRecord
from ingestion.nutrition.producer import (
    DEFAULT_SOURCE,
    DEFAULT_TOPIC,
    NutritionPublisher,
    build_envelope,
)


def _record(
    athlete_id: str = "A1",
    date: str = "2025-06-01",
    calories: int | None = 2400,
    protein_g: float | None = 150.0,
    carbs_g: float | None = 300.0,
    fat_g: float | None = 80.0,
    adherence_score: float | None = 0.85,
) -> NutritionRecord:
    return NutritionRecord(
        athlete_id=athlete_id,
        date=date,
        calories=calories,
        protein_g=protein_g,
        carbs_g=carbs_g,
        fat_g=fat_g,
        adherence_score=adherence_score,
    )


_FIXED_UUID = "44444444-4444-4444-8444-444444444444"
_FIXED_NOW_MS = 1751000000000  # arbitrary ingest_time epoch-ms
# 2025-06-01 UTC midnight epoch-ms
_EXPECTED_EVENT_TIME_20250601 = 1748736000000


# --- Module-level constants ---


def test_default_source_is_nutrition_csv():
    """DEFAULT_SOURCE must be 'nutrition_csv'."""
    assert DEFAULT_SOURCE == "nutrition_csv"


def test_default_topic_is_raw_nutrition():
    """DEFAULT_TOPIC must be 'raw.nutrition'."""
    assert DEFAULT_TOPIC == "raw.nutrition"


# --- build_envelope: required fields present (sc-8) ---


def test_build_envelope_event_time_is_utc_midnight_epoch_ms():
    """sc-8: event_time for date='2025-06-01' MUST be the UTC midnight epoch-ms long."""
    record = _record(date="2025-06-01")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_time"] == _EXPECTED_EVENT_TIME_20250601
    assert isinstance(env["event_time"], int)


def test_build_envelope_has_correct_envelope_shape():
    """sc-8: Envelope has all required top-level fields."""
    record = _record(athlete_id="A1", date="2025-06-01")
    env = build_envelope(
        record,
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert env["event_id"] == _FIXED_UUID
    assert env["source"] == "nutrition_csv"
    assert env["athlete_id"] == "A1"
    assert env["event_time"] == _EXPECTED_EVENT_TIME_20250601
    assert env["ingest_time"] == _FIXED_NOW_MS
    assert "payload" in env


def test_build_envelope_exact_top_level_key_set():
    """sc-8: Raw envelope must have exactly these top-level keys — no event_type."""
    env = build_envelope(
        _record(),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert set(env.keys()) == {
        "event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"
    }


def test_build_envelope_payload_contains_adherence_score_not_nutrition_adherence():
    """sc-8 (ADR-N2 CRITICAL): payload key MUST be 'adherence_score', NOT 'nutrition_adherence'.

    The rename adherence_score -> nutrition_adherence happens ONLY in the
    canonicalize transform. The producer preserves the source-faithful column name.
    """
    env = build_envelope(_record(), uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    payload = env["payload"]

    assert "adherence_score" in payload
    assert "nutrition_adherence" not in payload


def test_build_envelope_payload_contains_all_source_fields():
    """sc-8: Payload carries all 5 nutrition fields (no event_type in payload)."""
    env = build_envelope(_record(), uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    payload = env["payload"]

    expected_keys = {
        "athlete_id", "date", "calories", "protein_g", "carbs_g", "fat_g", "adherence_score"
    }
    assert set(payload.keys()) == expected_keys


def test_build_envelope_payload_has_no_event_type():
    """sc-8: payload must NOT contain event_type — it is hardcoded by the canonicalize job."""
    env = build_envelope(_record(), uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)
    assert "event_type" not in env["payload"]
    assert "event_type" not in env


# --- build_envelope: injectable callables (sc-9) ---


def test_build_envelope_injectable_now_and_uuid_factory():
    """sc-9: ingest_time and event_id come from the injected callables."""
    fixed_ingest = 9999888877776666
    fixed_uuid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

    env = build_envelope(
        _record(),
        now=lambda: fixed_ingest,
        uuid_factory=lambda: fixed_uuid,
    )

    assert env["ingest_time"] == fixed_ingest
    assert env["event_id"] == fixed_uuid


def test_build_envelope_different_dates_produce_different_epoch_ms():
    """sc-9 triangulation: different dates produce different epoch-ms event_time values."""
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


# --- build_envelope: nullable payload fields — None preserved (sc-10) ---


def test_build_envelope_nullable_fields_preserved_as_none():
    """sc-10: None values in record appear as None in payload verbatim."""
    record = _record(
        calories=None,
        protein_g=None,
        carbs_g=None,
        fat_g=None,
        adherence_score=None,
    )
    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)

    assert env["payload"]["calories"] is None
    assert env["payload"]["protein_g"] is None
    assert env["payload"]["carbs_g"] is None
    assert env["payload"]["fat_g"] is None
    assert env["payload"]["adherence_score"] is None


def test_build_envelope_all_nullable_fields_none_roundtrips_json():
    """sc-10 triangulation: None fields round-trip through JSON as null."""
    record = _record(
        calories=None, protein_g=None, carbs_g=None, fat_g=None, adherence_score=None
    )
    env = build_envelope(record, uuid_factory=lambda: _FIXED_UUID, now=lambda: _FIXED_NOW_MS)

    serialized = json.dumps(env)
    restored = json.loads(serialized)

    for field in ("calories", "protein_g", "carbs_g", "fat_g", "adherence_score"):
        assert restored["payload"][field] is None, (
            f"payload.{field} should round-trip as null/None"
        )


# --- NutritionPublisher: Kafka side-effect wrapper ---


class _FakeKafkaProducer:
    """Records produce() calls for assertions without real Kafka."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []
        self.flushed = False

    def produce(self, topic: str, value: str, key: str) -> None:
        self.calls.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_nutrition_publisher_produces_to_raw_nutrition_keyed_by_athlete_id():
    """NutritionPublisher.publish produces to raw.nutrition keyed by athlete_id."""
    fake = _FakeKafkaProducer()
    publisher = NutritionPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    event_id = publisher.publish(
        _record(athlete_id="A1"),
        uuid_factory=lambda: _FIXED_UUID,
        now=lambda: _FIXED_NOW_MS,
    )

    assert event_id == _FIXED_UUID
    assert len(fake.calls) == 1
    topic, value, key = fake.calls[0]
    assert topic == "raw.nutrition"
    assert key == "A1"

    envelope = json.loads(value)
    assert envelope["event_id"] == _FIXED_UUID
    assert envelope["athlete_id"] == "A1"
    assert envelope["event_time"] == _EXPECTED_EVENT_TIME_20250601
    assert isinstance(envelope["event_time"], int)
    # sc-8 guard in publisher test: source-faithful key
    assert "adherence_score" in envelope["payload"]
    assert "nutrition_adherence" not in envelope["payload"]


def test_nutrition_publisher_flush_delegates_to_underlying_producer():
    """flush() delegates to the underlying producer."""
    fake = _FakeKafkaProducer()
    publisher = NutritionPublisher(bootstrap_servers="ignored", kafka_producer=fake)

    publisher.flush()

    assert fake.flushed is True
