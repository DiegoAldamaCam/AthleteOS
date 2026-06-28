"""Unit tests for the cardio canonicalize PURE transform logic (PR-C2).

Mirrors tests/unit/test_wellness_transform.py structure.

Covers (sc-13..sc-22):
  sc-13: validate_cardio_event — valid envelope → no error
  sc-14: validate_cardio_event — missing athlete_id → ValidationError
  sc-15: validate_cardio_event — missing duration_sec → ValidationError
  sc-16: compute_cardio_session_load — tss present → session_load = tss (Tier 1)
  sc-17: compute_cardio_session_load — tss=None, avg_hr present → HR-TRIMP (Tier 2)
  sc-18: compute_cardio_session_load — both None → ValidationError (Tier 3 → DLQ)
  sc-19: transform_cardio_to_canonical — full mapping, TSS path
  sc-20: transform_cardio_to_canonical — unknown activity_type → no DLQ, CARDIO_ACTIVITY
  sc-21: transform_cardio_to_canonical — nullable fields absent → None in dict
  sc-22: fastavro roundtrip — canonical dict matches TrainingEvent.avsc field-for-field

No pyflink dependency. Runnable on CPython 3.14 without Docker.
"""

from __future__ import annotations

import io
import math
import pytest

from jobs.cardio_canonicalize.transform import (
    CARDIO_ACTIVITY,
    ValidationError,
    TransformError,
    validate_cardio_event,
    compute_cardio_session_load,
    transform_cardio_to_canonical,
    load_training_event_avsc,
    serialize_training_event_avro,
    deserialize_training_event_avro,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_EPOCH_MS_2025_06_01 = 1748736000000  # 2025-06-01T00:00:00 UTC


def _raw_cardio_envelope(
    *,
    event_id: str = "cevt-001",
    event_time: int = _EPOCH_MS_2025_06_01,
    ingest_time: int = _EPOCH_MS_2025_06_01 + 5_000,
    source: str = "synthetic_cardio",
    athlete_id: str = "A1",
    activity_type: str = "Run",
    duration_sec: int = 3600,
    distance_km: float | None = 10.0,
    avg_hr: int | None = 150,
    tss: float | None = 85.0,
) -> dict:
    """Build a raw.cardio envelope as the ingestion/cardio producer emits it."""
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "payload": {
            "activity_type": activity_type,
            "duration_sec": duration_sec,
            "distance_km": distance_km,
            "avg_hr": avg_hr,
            "tss": tss,
        },
    }


# ---------------------------------------------------------------------------
# sc-13: Valid envelope with all required fields — no error
# ---------------------------------------------------------------------------


class TestValidateCardioEvent:
    def test_sc13_valid_envelope_no_error(self):
        """sc-13: validate_cardio_event raises nothing for a valid envelope."""
        raw = _raw_cardio_envelope()
        # validate_cardio_event works on raw envelope fields
        validate_cardio_event(raw)  # must not raise

    def test_sc13_triangulation_minimal_nullable_fields_still_valid(self):
        """sc-13 triangulation: envelope with nullable fields absent is still valid."""
        raw = _raw_cardio_envelope(distance_km=None, avg_hr=None, tss=85.0)
        validate_cardio_event(raw)  # must not raise

    # sc-14 -----------------------------------------------------------------

    def test_sc14_missing_athlete_id_raises_validation_error(self):
        """sc-14: missing athlete_id → ValidationError."""
        raw = _raw_cardio_envelope()
        del raw["athlete_id"]
        with pytest.raises(ValidationError):
            validate_cardio_event(raw)

    def test_sc14_null_athlete_id_raises_validation_error(self):
        """sc-14 triangulation: null athlete_id → ValidationError."""
        raw = _raw_cardio_envelope()
        raw["athlete_id"] = None
        with pytest.raises(ValidationError):
            validate_cardio_event(raw)

    # sc-15 -----------------------------------------------------------------

    def test_sc15_missing_duration_sec_raises_validation_error(self):
        """sc-15: missing duration_sec in payload → ValidationError."""
        raw = _raw_cardio_envelope()
        del raw["payload"]["duration_sec"]
        with pytest.raises(ValidationError):
            validate_cardio_event(raw)

    def test_sc15_null_duration_sec_raises_validation_error(self):
        """sc-15 triangulation: null duration_sec in payload → ValidationError."""
        raw = _raw_cardio_envelope()
        raw["payload"]["duration_sec"] = None
        with pytest.raises(ValidationError):
            validate_cardio_event(raw)


# ---------------------------------------------------------------------------
# sc-16..sc-18: session_load computation (three-tier)
# ---------------------------------------------------------------------------


class TestComputeCardioSessionLoad:
    def test_sc16_tss_present_returns_tss(self):
        """sc-16: Tier 1 — tss present → session_load = tss, avg_hr ignored."""
        result = compute_cardio_session_load(tss=85.0, avg_hr=150, duration_sec=3600)
        assert result == 85.0

    def test_sc16_triangulation_different_tss_value(self):
        """sc-16 triangulation: different tss value, still direct return."""
        result = compute_cardio_session_load(tss=120.5, avg_hr=170, duration_sec=7200)
        assert result == 120.5

    def test_sc17_tss_none_avg_hr_present_returns_hr_trimp(self):
        """sc-17: Tier 2 — tss=None, avg_hr present → HR-TRIMP proxy."""
        result = compute_cardio_session_load(tss=None, avg_hr=150, duration_sec=3600)
        expected = (3600 / 3600.0) * (150 / 220.0) * 100.0
        assert math.isclose(result, expected, rel_tol=1e-6)

    def test_sc17_triangulation_different_duration_and_hr(self):
        """sc-17 triangulation: different inputs produce different HR-TRIMP value."""
        result = compute_cardio_session_load(tss=None, avg_hr=160, duration_sec=1800)
        expected = (1800 / 3600.0) * (160 / 220.0) * 100.0
        assert math.isclose(result, expected, rel_tol=1e-6)
        # Must differ from sc-17 base case
        assert not math.isclose(result, (3600 / 3600.0) * (150 / 220.0) * 100.0, rel_tol=1e-4)

    def test_sc17_result_is_finite_float(self):
        """sc-17: result must be a finite float (non-NaN, non-inf)."""
        result = compute_cardio_session_load(tss=None, avg_hr=150, duration_sec=3600)
        assert isinstance(result, float)
        assert math.isfinite(result)

    def test_sc18_both_none_raises_validation_error(self):
        """sc-18: Tier 3 — both tss and avg_hr None → ValidationError → DLQ."""
        with pytest.raises(ValidationError, match="session_load"):
            compute_cardio_session_load(tss=None, avg_hr=None, duration_sec=3600)

    def test_sc18_triangulation_different_duration_still_raises(self):
        """sc-18 triangulation: duration change doesn't rescue missing tss+avg_hr."""
        with pytest.raises(ValidationError):
            compute_cardio_session_load(tss=None, avg_hr=None, duration_sec=7200)


# ---------------------------------------------------------------------------
# sc-19..sc-21: Full canonical transform
# ---------------------------------------------------------------------------


class TestTransformCardioToCanonical:
    def test_sc19_full_canonical_mapping_tss_path(self):
        """sc-19: Full mapping — TSS path, event_type=CARDIO_ACTIVITY, session_load=tss."""
        raw = _raw_cardio_envelope(
            activity_type="Run",
            tss=70.0,
            avg_hr=145,
            duration_sec=3600,
            distance_km=10.0,
        )
        result = transform_cardio_to_canonical(raw, schema_version=1)

        assert result["event_type"] == CARDIO_ACTIVITY
        assert result["event_type"] == "CARDIO_ACTIVITY"
        assert result["session_load"] == 70.0
        assert result["distance_km"] == 10.0
        assert result["avg_hr"] == 145
        assert result["tss"] == 70.0
        assert result["event_id"] == raw["event_id"]
        assert result["event_time"] == raw["event_time"]
        assert result["athlete_id"] == raw["athlete_id"]

    def test_sc19_triangulation_strength_only_fields_are_none(self):
        """sc-19 triangulation: strength-only fields must be None in cardio canonical."""
        raw = _raw_cardio_envelope(tss=50.0)
        result = transform_cardio_to_canonical(raw, schema_version=1)
        assert result["workout_id"] is None
        assert result["exercise_id"] is None
        assert result["set_number"] is None
        assert result["reps"] is None
        assert result["weight_kg"] is None
        assert result["rpe"] is None
        assert result["rir"] is None

    def test_sc20_unknown_activity_type_no_dlq(self):
        """sc-20: Unknown activity_type → no ValidationError, event_type=CARDIO_ACTIVITY."""
        raw = _raw_cardio_envelope(activity_type="UltraMarathon", tss=50.0)
        result = transform_cardio_to_canonical(raw, schema_version=1)
        assert "UltraMarathon" in result["activity_type"]  # soft-normalized form present
        assert result["event_type"] == "CARDIO_ACTIVITY"

    def test_sc20_triangulation_another_unknown_type(self):
        """sc-20 triangulation: another unknown type also passes without error."""
        raw = _raw_cardio_envelope(activity_type="Kayaking", tss=30.0)
        result = transform_cardio_to_canonical(raw, schema_version=1)
        assert result["event_type"] == "CARDIO_ACTIVITY"
        assert result["activity_type"] is not None

    def test_sc21_nullable_fields_absent_become_none(self):
        """sc-21: distance_km=None, avg_hr=None → None in canonical dict; tss→session_load."""
        raw = _raw_cardio_envelope(distance_km=None, avg_hr=None, tss=85.0)
        result = transform_cardio_to_canonical(raw, schema_version=1)
        assert result["distance_km"] is None
        assert result["avg_hr"] is None
        assert result["session_load"] == 85.0

    def test_sc21_triangulation_tss_none_hr_present_session_load_from_hr(self):
        """sc-21 triangulation: tss=None, avg_hr present → session_load from HR-TRIMP."""
        raw = _raw_cardio_envelope(distance_km=None, avg_hr=150, tss=None, duration_sec=3600)
        result = transform_cardio_to_canonical(raw, schema_version=1)
        expected = (3600 / 3600.0) * (150 / 220.0) * 100.0
        assert math.isclose(result["session_load"], expected, rel_tol=1e-6)
        assert result["distance_km"] is None
        assert result["avg_hr"] == 150
        assert result["tss"] is None

    def test_sc14_transform_raises_on_missing_athlete_id(self):
        """sc-14 via transform: missing athlete_id in raw → ValidationError from transform."""
        raw = _raw_cardio_envelope()
        del raw["athlete_id"]
        with pytest.raises(ValidationError):
            transform_cardio_to_canonical(raw, schema_version=1)


# ---------------------------------------------------------------------------
# sc-22: Fastavro roundtrip — canonical dict ↔ TrainingEvent.avsc
# ---------------------------------------------------------------------------


class TestFastavroRoundtrip:
    def _make_canonical_tss_path(self) -> dict:
        raw = _raw_cardio_envelope(
            event_id="cevt-roundtrip-001",
            activity_type="Run",
            tss=70.0,
            avg_hr=145,
            duration_sec=3600,
            distance_km=10.0,
        )
        return transform_cardio_to_canonical(raw, schema_version=1)

    def test_sc22_roundtrip_tss_path(self):
        """sc-22: fastavro schemaless_writer → schemaless_reader round-trips correctly."""
        fastavro = pytest.importorskip("fastavro")
        schema = load_training_event_avsc()
        canonical = self._make_canonical_tss_path()

        encoded = serialize_training_event_avro(canonical, schema)
        decoded = deserialize_training_event_avro(encoded, schema)

        assert decoded["event_type"] == "CARDIO_ACTIVITY"
        assert decoded["session_load"] == canonical["session_load"]
        assert decoded["athlete_id"] == canonical["athlete_id"]
        assert decoded["event_id"] == canonical["event_id"]

    def test_sc22_triangulation_hr_trimp_path_roundtrip(self):
        """sc-22 triangulation: HR-TRIMP path also round-trips through Avro."""
        fastavro = pytest.importorskip("fastavro")
        schema = load_training_event_avsc()
        raw = _raw_cardio_envelope(
            event_id="cevt-roundtrip-002",
            tss=None,
            avg_hr=150,
            duration_sec=3600,
            distance_km=None,
        )
        canonical = transform_cardio_to_canonical(raw, schema_version=1)
        encoded = serialize_training_event_avro(canonical, schema)
        decoded = deserialize_training_event_avro(encoded, schema)

        expected_sl = (3600 / 3600.0) * (150 / 220.0) * 100.0
        assert math.isclose(decoded["session_load"], expected_sl, rel_tol=1e-5)
        assert decoded["event_type"] == "CARDIO_ACTIVITY"
        assert decoded["distance_km"] is None
