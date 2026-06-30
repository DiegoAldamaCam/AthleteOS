"""Unit tests for the wellness canonicalize PURE transform logic (PR-W2).

Mirrors tests/unit/test_canonicalize_transform.py structure.

Covers:
  - validate_wellness_event: event_type symbol guard (W2-1, W2-2)
  - transform_wellness_to_canonical: full mapping, inapplicable nutrition
    fields → None (W2-3)
  - Fastavro roundtrip vs migrated WellnessEvent.avsc (W2-4): confirms
    event_type is a STRING in the registered schema and survives the Avro
    encode/decode cycle.

No pyflink dependency. Runnable on CPython 3.14 without Docker.
"""

from __future__ import annotations

import base64
from datetime import datetime
from pathlib import Path

import pytest

from jobs.wellness_canonicalize.transform import (
    ALLOWED_WELLNESS_TYPES,
    ValidationError,
    TransformError,
    transform_wellness_to_canonical,
    validate_wellness_event,
    load_wellness_event_avsc,
    serialize_wellness_event_avro,
    deserialize_wellness_event_avro,
    build_dlq_envelope,
    select_dlq_error_type,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_wellness_envelope(
    *,
    event_id: str = "wevt-001",
    event_time: int = 1740787200000,  # 2025-03-01 UTC midnight epoch-ms
    ingest_time: int = 1740790800000,
    source: str = "synthetic_wellness",
    athlete_id: str = "A1",
    event_type: str = "WELLNESS_DAILY",
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
    # nutrition fields (NUTRITION_DAILY only — None for WELLNESS_DAILY)
    calories: int | None = None,
    protein_g: float | None = None,
    carbs_g: float | None = None,
    fat_g: float | None = None,
    nutrition_adherence: float | None = None,
) -> dict:
    """Build a raw.wellness envelope as the ingestion/wellness producer emits it."""
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "payload": {
            "event_type": event_type,
            "hrv": hrv,
            "sleep_hours": sleep_hours,
            "resting_hr": resting_hr,
            "steps": steps,
            "body_weight_kg": body_weight_kg,
            "energy": energy,
            "soreness": soreness,
            "mood": mood,
            "stress": stress,
            "perceived_recovery": perceived_recovery,
            "calories": calories,
            "protein_g": protein_g,
            "carbs_g": carbs_g,
            "fat_g": fat_g,
            "nutrition_adherence": nutrition_adherence,
        },
    }


# ---------------------------------------------------------------------------
# W2-1: Invalid event_type symbol → ValidationError
# ---------------------------------------------------------------------------


def test_validate_wellness_event_rejects_unknown_event_type():
    """W2-1: An unknown event_type symbol must raise ValidationError and route
    to DLQ with error_type=VALIDATION_FAILURE (ADR-16 app-layer guard)."""
    raw = _raw_wellness_envelope(event_type="WELLNESS_DAILY")
    canonical = transform_wellness_to_canonical(raw, schema_version=1)
    # Inject an unknown event_type to simulate an off-symbol record
    canonical["event_type"] = "UNKNOWN_TYPE"
    with pytest.raises(ValidationError) as exc_info:
        validate_wellness_event(canonical)
    assert "UNKNOWN_TYPE" in str(exc_info.value) or "event_type" in str(exc_info.value).lower()


def test_unknown_event_type_routes_to_validation_failure():
    """select_dlq_error_type(ValidationError) == VALIDATION_FAILURE."""
    raw = _raw_wellness_envelope()
    canonical = transform_wellness_to_canonical(raw, schema_version=1)
    canonical["event_type"] = "BOGUS"
    try:
        validate_wellness_event(canonical)
    except ValidationError as exc:
        assert select_dlq_error_type(exc) == "VALIDATION_FAILURE"
    else:
        pytest.fail("BOGUS event_type should have raised ValidationError")


# ---------------------------------------------------------------------------
# W2-2: Valid symbol accepted
# ---------------------------------------------------------------------------


def test_validate_wellness_event_accepts_wellness_daily():
    """W2-2: WELLNESS_DAILY is in ALLOWED_WELLNESS_TYPES → no exception."""
    raw = _raw_wellness_envelope(event_type="WELLNESS_DAILY")
    canonical = transform_wellness_to_canonical(raw, schema_version=1)
    validate_wellness_event(canonical)  # must not raise


def test_validate_wellness_event_accepts_recovery_snapshot():
    """W2-2: RECOVERY_SNAPSHOT is valid."""
    raw = _raw_wellness_envelope(event_type="RECOVERY_SNAPSHOT")
    canonical = transform_wellness_to_canonical(raw, schema_version=1)
    validate_wellness_event(canonical)  # must not raise


def test_validate_wellness_event_accepts_nutrition_daily():
    """W2-2: NUTRITION_DAILY is valid."""
    raw = _raw_wellness_envelope(event_type="NUTRITION_DAILY")
    canonical = transform_wellness_to_canonical(raw, schema_version=1)
    validate_wellness_event(canonical)  # must not raise


def test_allowed_wellness_types_contains_expected_symbols():
    """ALLOWED_WELLNESS_TYPES must contain all three known symbols."""
    assert "RECOVERY_SNAPSHOT" in ALLOWED_WELLNESS_TYPES
    assert "NUTRITION_DAILY" in ALLOWED_WELLNESS_TYPES
    assert "WELLNESS_DAILY" in ALLOWED_WELLNESS_TYPES


# ---------------------------------------------------------------------------
# W2-3: Full canonical mapping — inapplicable nutrition fields → None
# ---------------------------------------------------------------------------


def test_transform_wellness_full_canonical_mapping():
    """W2-3: transform_wellness_to_canonical maps all fields; nutrition fields
    from a WELLNESS_DAILY event are None in the canonical output."""
    raw = _raw_wellness_envelope(
        event_id="wevt-w3",
        athlete_id="A2",
        event_type="WELLNESS_DAILY",
        hrv=65.0,
        sleep_hours=7.5,
        perceived_recovery=8,
    )
    out = transform_wellness_to_canonical(raw, schema_version=1)

    # Required envelope fields
    assert out["event_id"] == "wevt-w3"
    assert out["event_time"] == 1740787200000
    assert out["ingest_time"] == 1740790800000
    assert out["source"] == "synthetic_wellness"
    assert out["schema_version"] == 1
    assert out["athlete_id"] == "A2"
    assert out["event_type"] == "WELLNESS_DAILY"

    # Wellness signal fields
    assert out["hrv"] == pytest.approx(65.0)
    assert out["sleep_hours"] == pytest.approx(7.5)
    assert out["perceived_recovery"] == 8

    # Inapplicable nutrition fields → None for WELLNESS_DAILY
    assert out["calories"] is None
    assert out["protein_g"] is None
    assert out["carbs_g"] is None
    assert out["fat_g"] is None
    assert out["nutrition_adherence"] is None


def test_transform_wellness_adds_schema_version_not_present_in_raw():
    """schema_version is absent from the raw envelope; transform adds it."""
    raw = _raw_wellness_envelope()
    assert "schema_version" not in raw
    out = transform_wellness_to_canonical(raw, schema_version=2)
    assert out["schema_version"] == 2


def test_transform_wellness_missing_athlete_id_raises_validation_error():
    """Missing athlete_id in raw envelope → ValidationError."""
    raw = _raw_wellness_envelope()
    del raw["athlete_id"]
    with pytest.raises(ValidationError):
        transform_wellness_to_canonical(raw, schema_version=1)


def test_transform_wellness_missing_event_id_raises_validation_error():
    """Missing event_id in raw envelope → ValidationError."""
    raw = _raw_wellness_envelope()
    del raw["event_id"]
    with pytest.raises(ValidationError):
        transform_wellness_to_canonical(raw, schema_version=1)


# ---------------------------------------------------------------------------
# W2-4: Fastavro roundtrip vs migrated WellnessEvent.avsc
# ---------------------------------------------------------------------------


def test_wellness_event_avsc_declares_event_type_as_string():
    """ADR-16: migrated WellnessEvent.avsc must declare event_type as plain
    Avro 'string', NOT the former enum type."""
    schema = load_wellness_event_avsc()
    et_field = next(f for f in schema["fields"] if f["name"] == "event_type")
    assert et_field["type"] == "string", (
        f"event_type must be Avro 'string' per ADR-16, got {et_field['type']!r}"
    )


def test_canonical_wellness_event_roundtrips_through_avro():
    """W2-4: A canonical WellnessEvent dict round-trips through fastavro
    serialize/deserialize against the migrated .avsc with no error."""
    raw = _raw_wellness_envelope()
    event = transform_wellness_to_canonical(raw, schema_version=1)
    schema = load_wellness_event_avsc()

    encoded = serialize_wellness_event_avro(event, schema)
    assert isinstance(encoded, (bytes, bytearray))
    assert len(encoded) > 0

    decoded = deserialize_wellness_event_avro(encoded, schema)

    # event_type must survive as a STRING
    assert decoded["event_type"] == "WELLNESS_DAILY"
    assert decoded["event_id"] == "wevt-001"
    assert decoded["athlete_id"] == "A1"
    assert decoded["schema_version"] == 1

    # Timestamp fields: fastavro may decode timestamp-millis as datetime
    def _ms(v):
        if isinstance(v, datetime):
            return int(v.timestamp() * 1000)
        return int(v)

    assert _ms(decoded["event_time"]) == 1740787200000
    assert decoded["hrv"] == pytest.approx(65.0)
    assert decoded["sleep_hours"] == pytest.approx(7.5)
    assert decoded["perceived_recovery"] == 8
    # Nutrition fields default to None
    assert decoded["calories"] is None
    assert decoded["protein_g"] is None


def test_canonical_wellness_roundtrip_with_null_optional_fields():
    """W2-4 triangulation: all nullable fields None also round-trips."""
    raw = _raw_wellness_envelope(
        hrv=None,
        sleep_hours=None,
        resting_hr=None,
        steps=None,
        body_weight_kg=None,
        energy=None,
        soreness=None,
        mood=None,
        stress=None,
        perceived_recovery=None,
    )
    event = transform_wellness_to_canonical(raw, schema_version=1)
    schema = load_wellness_event_avsc()
    decoded = deserialize_wellness_event_avro(
        serialize_wellness_event_avro(event, schema), schema
    )
    assert decoded["hrv"] is None
    assert decoded["sleep_hours"] is None
    assert decoded["perceived_recovery"] is None
    assert decoded["event_type"] == "WELLNESS_DAILY"


# ---------------------------------------------------------------------------
# DLQ envelope helpers (reused from jobs.canonicalize.transform)
# ---------------------------------------------------------------------------


def test_build_dlq_envelope_has_expected_shape():
    """DLQ envelope builder produces spec-compliant dict with base64 value."""
    now_ms = 1740787200000
    original_value = b'{"bad":"record"}'
    env = build_dlq_envelope(
        original_topic="raw.wellness",
        original_key="A1",
        original_value=original_value,
        error_type="VALIDATION_FAILURE",
        error_message="missing athlete_id",
        timestamp=now_ms,
    )
    assert env["original_topic"] == "raw.wellness"
    assert env["original_key"] == "A1"
    assert env["original_value"] == base64.b64encode(original_value).decode("ascii")
    assert env["error_type"] == "VALIDATION_FAILURE"
    assert env["timestamp"] == now_ms


# ---------------------------------------------------------------------------
# FIX 5 (R3-C4): NUTRITION_DAILY with populated nutrition fields pass-through
# ---------------------------------------------------------------------------


def test_transform_nutrition_daily_populates_nutrition_fields():
    """FIX 5 (R3-C4): transform_wellness_to_canonical with event_type=NUTRITION_DAILY
    and populated nutrition fields must pass those values THROUGH to the canonical
    record (not zero/None). Inverse of W2-3 (which only tests nutrition→None for
    WELLNESS_DAILY). Confirms the pipeline does NOT null-out nutrition data for
    records that legitimately carry it."""
    raw = _raw_wellness_envelope(
        event_type="NUTRITION_DAILY",
        # Populate all nutrition fields with known values
        calories=2400,
        protein_g=160.5,
        carbs_g=280.0,
        fat_g=75.3,
        nutrition_adherence=0.87,
        # Wellness fields are typically None for NUTRITION_DAILY
        hrv=None,
        sleep_hours=None,
        perceived_recovery=None,
    )
    out = transform_wellness_to_canonical(raw, schema_version=1)

    assert out["event_type"] == "NUTRITION_DAILY"
    # Nutrition fields must be passed through as-is (not zeroed or nulled)
    assert out["calories"] == 2400
    assert out["protein_g"] == pytest.approx(160.5)
    assert out["carbs_g"] == pytest.approx(280.0)
    assert out["fat_g"] == pytest.approx(75.3)
    assert out["nutrition_adherence"] == pytest.approx(0.87)
    # validate must accept NUTRITION_DAILY
    validate_wellness_event(out)  # must not raise


# ---------------------------------------------------------------------------
# FIX 6 (R3-W2): end-to-end BOGUS_TYPE through full transform+validate flow
# ---------------------------------------------------------------------------


def test_bogus_event_type_through_full_pipeline_raises_validation_error():
    """FIX 6 (R3-W2): feed raw payload with event_type='BOGUS_TYPE' through the
    FULL flow — transform_wellness_to_canonical(raw) then validate_wellness_event(result)
    — and assert ValidationError is raised. Proves the pipeline as process_element
    runs it, not just the guard in isolation. The transform succeeds (BOGUS_TYPE
    is a non-empty string), but validate rejects the off-symbol value."""
    raw = _raw_wellness_envelope(event_type="BOGUS_TYPE")
    # Step 1: transform succeeds (BOGUS_TYPE is a valid non-empty string)
    canonical = transform_wellness_to_canonical(raw, schema_version=1)
    assert canonical["event_type"] == "BOGUS_TYPE"
    # Step 2: validate rejects the off-symbol value → DLQ path
    with pytest.raises(ValidationError) as exc_info:
        validate_wellness_event(canonical)
    assert "BOGUS_TYPE" in str(exc_info.value) or "event_type" in str(exc_info.value).lower()
    # Confirm the DLQ error_type is VALIDATION_FAILURE (the routing used in process_element)
    assert select_dlq_error_type(exc_info.value) == "VALIDATION_FAILURE"


# ---------------------------------------------------------------------------
# sc-1..sc-4c  key_by event_id guard (DLQ key_by error handling)
# ---------------------------------------------------------------------------


class TestKeyByEventIdGuard:
    """Unit tests for _key_by_event_id guard (sc-1..sc-4c).

    All scenarios require the function to never raise and to return ""
    (sentinel) for any non-dict / missing-key / malformed input, and to
    return the event_id string for well-formed payloads.
    """

    def test_sc1_malformed_json_returns_sentinel(self):
        """sc-1: malformed JSON string → \"\" (no exception)."""
        from jobs.wellness_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id("not-json")
        assert result == ""

    def test_sc2_missing_event_id_returns_sentinel(self):
        """sc-2: valid JSON dict without event_id → \"\"."""
        import json
        from jobs.wellness_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id(json.dumps({"other_field": "abc"}))
        assert result == ""

    def test_sc3_valid_dict_returns_event_id(self):
        """sc-3: valid JSON dict with event_id → the event_id value."""
        import json
        from jobs.wellness_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id(json.dumps({"event_id": "evt-123"}))
        assert result == "evt-123"

    def test_sc4_non_dict_json_returns_sentinel(self):
        """sc-4: valid JSON non-dict (integer) → \"\" (no AttributeError)."""
        from jobs.wellness_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id("42")
        assert result == ""

    def test_sc4b_empty_string_returns_sentinel(self):
        """sc-4b: empty string input → \"\"."""
        from jobs.wellness_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id("")
        assert result == ""

    def test_sc4c_none_input_returns_sentinel(self):
        """sc-4c: None input → \"\" (no TypeError)."""
        from jobs.wellness_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id(None)
        assert result == ""
