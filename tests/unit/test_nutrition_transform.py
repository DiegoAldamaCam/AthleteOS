"""Unit tests for the nutrition canonicalize PURE transform logic (PR-N2).

Mirrors tests/unit/test_recovery_transform.py structure.

Covers (sc-13..sc-26):
  sc-13: validate_nutrition_event — valid envelope with athlete_id + event_time → no error
  sc-14: validate_nutrition_event — missing athlete_id → ValidationError → DLQ
  sc-15: validate_nutrition_event — missing event_time → ValidationError → DLQ
  sc-16: transform_nutrition_to_canonical — adherence_score renamed to nutrition_adherence;
         output dict MUST NOT contain 'adherence_score' key
  sc-17: transform_nutrition_to_canonical — full canonical mapping (all nutrition fields,
         event_type='NUTRITION_DAILY', all recovery/subjective fields = None)
  sc-18: transform_nutrition_to_canonical — all-null data fields → valid dict, event_type
         still 'NUTRITION_DAILY'
  sc-19: fastavro roundtrip — canonical dict matches WellnessEvent.avsc field-for-field
  sc-25: TRANSACTIONAL_ID_PREFIX constant equals exact literal string (no Docker needed)

No pyflink dependency. Runnable on CPython 3.14 without Docker.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from jobs.nutrition_canonicalize.transform import (
    ValidationError,
    validate_nutrition_event,
    transform_nutrition_to_canonical,
    load_wellness_event_avsc,
    serialize_wellness_event_avro,
    deserialize_wellness_event_avro,
    NUTRITION_DAILY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# 2025-06-01 UTC midnight epoch-ms
_EPOCH_MS_2025_06_01 = 1748736000000


def _raw_nutrition_envelope(
    *,
    event_id: str = "nevt-001",
    event_time: int = _EPOCH_MS_2025_06_01,
    ingest_time: int = _EPOCH_MS_2025_06_01 + 5_000,
    source: str = "nutrition_csv",
    athlete_id: str = "A1",
    calories: int | None = 2400,
    protein_g: float | None = 150.0,
    carbs_g: float | None = 300.0,
    fat_g: float | None = 80.0,
    adherence_score: float | None = 0.9,
) -> dict:
    """Build a raw.nutrition envelope as the ingestion/nutrition producer emits it.

    NOTE: The payload key is 'adherence_score' (source-faithful, per sc-8).
    The rename to 'nutrition_adherence' is performed ONLY in the transform (sc-16).
    """
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "payload": {
            "calories": calories,
            "protein_g": protein_g,
            "carbs_g": carbs_g,
            "fat_g": fat_g,
            "adherence_score": adherence_score,  # source-faithful name (sc-8 guard)
        },
    }


# ---------------------------------------------------------------------------
# sc-13..sc-15: validate_nutrition_event
# ---------------------------------------------------------------------------


class TestValidateNutritionEvent:
    def test_sc13_valid_envelope_with_required_fields_no_error(self):
        """sc-13: Valid envelope with athlete_id and event_time → no exception raised."""
        raw = _raw_nutrition_envelope()
        validate_nutrition_event(raw)  # must not raise

    def test_sc13_triangulation_all_data_fields_null_still_valid(self):
        """sc-13 triangulation: all 5 data fields null — validation still passes."""
        raw = _raw_nutrition_envelope(
            calories=None,
            protein_g=None,
            carbs_g=None,
            fat_g=None,
            adherence_score=None,
        )
        validate_nutrition_event(raw)  # must not raise (data fields are optional)

    def test_sc14_missing_athlete_id_raises_validation_error(self):
        """sc-14: Missing athlete_id → ValidationError."""
        raw = _raw_nutrition_envelope()
        del raw["athlete_id"]
        with pytest.raises(ValidationError):
            validate_nutrition_event(raw)

    def test_sc14_null_athlete_id_raises_validation_error(self):
        """sc-14 triangulation: null athlete_id → ValidationError."""
        raw = _raw_nutrition_envelope()
        raw["athlete_id"] = None
        with pytest.raises(ValidationError):
            validate_nutrition_event(raw)

    def test_sc15_missing_event_time_raises_validation_error(self):
        """sc-15: Missing event_time → ValidationError, original_topic='raw.nutrition'."""
        raw = _raw_nutrition_envelope()
        del raw["event_time"]
        with pytest.raises(ValidationError):
            validate_nutrition_event(raw)

    def test_sc15_null_event_time_raises_validation_error(self):
        """sc-15 triangulation: null event_time → ValidationError."""
        raw = _raw_nutrition_envelope()
        raw["event_time"] = None
        with pytest.raises(ValidationError):
            validate_nutrition_event(raw)


# ---------------------------------------------------------------------------
# sc-16..sc-18: transform_nutrition_to_canonical
# ---------------------------------------------------------------------------


class TestTransformNutritionToCanonical:
    def test_sc16_adherence_score_renamed_to_nutrition_adherence(self):
        """sc-16: adherence_score in payload MUST be renamed to nutrition_adherence
        in canonical dict. Output MUST NOT contain 'adherence_score' key."""
        raw = _raw_nutrition_envelope(adherence_score=0.85)
        result = transform_nutrition_to_canonical(raw, schema_version=1)

        # The rename MUST happen
        assert result["nutrition_adherence"] == pytest.approx(0.85)
        # The original source-layer key MUST NOT appear in the canonical output
        assert "adherence_score" not in result

    def test_sc16_triangulation_null_adherence_score_renamed_to_none(self):
        """sc-16 triangulation: adherence_score=None → nutrition_adherence=None in output;
        key 'adherence_score' must still be absent from output."""
        raw = _raw_nutrition_envelope(adherence_score=None)
        result = transform_nutrition_to_canonical(raw, schema_version=1)

        assert result["nutrition_adherence"] is None
        assert "adherence_score" not in result

    def test_sc17_full_canonical_mapping(self):
        """sc-17: Full mapping — event_type='NUTRITION_DAILY', all 5 nutrition fields
        present with correct values, all recovery/subjective fields = None."""
        raw = _raw_nutrition_envelope(
            calories=2400,
            protein_g=150.0,
            carbs_g=300.0,
            fat_g=80.0,
            adherence_score=0.9,
        )
        result = transform_nutrition_to_canonical(raw, schema_version=1)

        # event_type MUST be the hardcoded constant (sc-17)
        assert result["event_type"] == "NUTRITION_DAILY"
        assert result["event_type"] == NUTRITION_DAILY

        # All 5 nutrition fields must match source values
        assert result["calories"] == 2400
        assert result["protein_g"] == pytest.approx(150.0)
        assert result["carbs_g"] == pytest.approx(300.0)
        assert result["fat_g"] == pytest.approx(80.0)
        assert result["nutrition_adherence"] == pytest.approx(0.9)

        # 'adherence_score' MUST NOT be in canonical output
        assert "adherence_score" not in result

        # Required envelope fields
        assert result["event_id"] == raw["event_id"]
        assert result["event_time"] == raw["event_time"]
        assert result["athlete_id"] == raw["athlete_id"]
        assert result["schema_version"] == 1

        # Recovery/subjective fields MUST be None (sc-17)
        assert result["sleep_hours"] is None
        assert result["resting_hr"] is None
        assert result["hrv"] is None
        assert result["steps"] is None
        assert result["body_weight_kg"] is None
        assert result["energy"] is None
        assert result["soreness"] is None
        assert result["mood"] is None
        assert result["stress"] is None
        assert result["perceived_recovery"] is None

    def test_sc17_triangulation_event_type_hardcoded_not_from_payload(self):
        """sc-17 triangulation: event_type is HARDCODED, never read from payload."""
        raw = _raw_nutrition_envelope()
        # Even if we inject a different event_type-like field in payload,
        # the transform must still produce NUTRITION_DAILY
        raw["payload"]["event_type"] = "WELLNESS_DAILY"  # injection attempt
        result = transform_nutrition_to_canonical(raw, schema_version=1)
        assert result["event_type"] == "NUTRITION_DAILY"

    def test_sc18_all_null_data_fields_valid_dict_event_type_nutrition(self):
        """sc-18: All 5 data fields null → valid dict, no exception,
        event_type still NUTRITION_DAILY, all data fields None."""
        raw = _raw_nutrition_envelope(
            calories=None,
            protein_g=None,
            carbs_g=None,
            fat_g=None,
            adherence_score=None,
        )
        result = transform_nutrition_to_canonical(raw, schema_version=1)

        # Must NOT raise; must return valid dict
        assert isinstance(result, dict)
        assert result["event_type"] == "NUTRITION_DAILY"

        # All 5 nutrition data fields must be None
        assert result["calories"] is None
        assert result["protein_g"] is None
        assert result["carbs_g"] is None
        assert result["fat_g"] is None
        assert result["nutrition_adherence"] is None
        assert "adherence_score" not in result

    def test_sc18_triangulation_mixed_null_fields(self):
        """sc-18 triangulation: only some fields null — still valid, event_type unchanged."""
        raw = _raw_nutrition_envelope(
            calories=2000,
            protein_g=None,
            carbs_g=None,
            fat_g=70.0,
            adherence_score=0.75,
        )
        result = transform_nutrition_to_canonical(raw, schema_version=1)
        assert result["event_type"] == "NUTRITION_DAILY"
        assert result["calories"] == 2000
        assert result["protein_g"] is None
        assert result["carbs_g"] is None
        assert result["fat_g"] == pytest.approx(70.0)
        assert result["nutrition_adherence"] == pytest.approx(0.75)
        assert "adherence_score" not in result

    def test_sc14_transform_raises_on_missing_athlete_id(self):
        """sc-14 via transform: missing athlete_id in raw → ValidationError."""
        raw = _raw_nutrition_envelope()
        del raw["athlete_id"]
        with pytest.raises(ValidationError):
            transform_nutrition_to_canonical(raw, schema_version=1)


# ---------------------------------------------------------------------------
# sc-19: Fastavro roundtrip — canonical dict ↔ WellnessEvent.avsc
# ---------------------------------------------------------------------------


class TestFastavroRoundtrip:
    def _make_canonical_full(self) -> dict:
        raw = _raw_nutrition_envelope(
            event_id="nevt-roundtrip-001",
            calories=2400,
            protein_g=150.0,
            carbs_g=300.0,
            fat_g=80.0,
            adherence_score=0.9,
        )
        return transform_nutrition_to_canonical(raw, schema_version=1)

    def test_sc19_roundtrip_full_fields(self):
        """sc-19: fastavro schemaless_writer → schemaless_reader round-trips correctly."""
        fastavro = pytest.importorskip("fastavro")
        schema = load_wellness_event_avsc()
        canonical = self._make_canonical_full()

        encoded = serialize_wellness_event_avro(canonical, schema)
        assert isinstance(encoded, (bytes, bytearray))
        assert len(encoded) > 0

        decoded = deserialize_wellness_event_avro(encoded, schema)

        # event_type must survive as NUTRITION_DAILY (string)
        assert decoded["event_type"] == "NUTRITION_DAILY"
        assert decoded["event_id"] == "nevt-roundtrip-001"
        assert decoded["athlete_id"] == "A1"
        assert decoded["schema_version"] == 1

        # Timestamp handling: fastavro may decode timestamp-millis as datetime
        def _ms(v):
            if isinstance(v, datetime):
                return int(v.timestamp() * 1000)
            return int(v)

        assert _ms(decoded["event_time"]) == _EPOCH_MS_2025_06_01

        # Nutrition fields must survive
        assert decoded["calories"] == 2400
        assert decoded["protein_g"] == pytest.approx(150.0)
        assert decoded["carbs_g"] == pytest.approx(300.0)
        assert decoded["fat_g"] == pytest.approx(80.0)
        assert decoded["nutrition_adherence"] == pytest.approx(0.9)
        assert "adherence_score" not in decoded

        # Recovery/subjective fields must be None
        assert decoded["sleep_hours"] is None
        assert decoded["hrv"] is None
        assert decoded["energy"] is None
        assert decoded["perceived_recovery"] is None

    def test_sc19_triangulation_all_null_data_roundtrip(self):
        """sc-19 triangulation: all-null data fields also round-trip without error."""
        fastavro = pytest.importorskip("fastavro")
        schema = load_wellness_event_avsc()
        raw = _raw_nutrition_envelope(
            event_id="nevt-roundtrip-002",
            calories=None,
            protein_g=None,
            carbs_g=None,
            fat_g=None,
            adherence_score=None,
        )
        canonical = transform_nutrition_to_canonical(raw, schema_version=1)
        encoded = serialize_wellness_event_avro(canonical, schema)
        decoded = deserialize_wellness_event_avro(encoded, schema)

        assert decoded["event_type"] == "NUTRITION_DAILY"
        assert decoded["calories"] is None
        assert decoded["nutrition_adherence"] is None
        assert decoded["sleep_hours"] is None


# ---------------------------------------------------------------------------
# sc-25: TRANSACTIONAL_ID_PREFIX constant (no Docker required)
# ---------------------------------------------------------------------------


class TestTransactionalIdPrefix:
    def test_sc25_transactional_id_prefix_exact_literal(self):
        """sc-25: TRANSACTIONAL_ID_PREFIX must be the exact literal string
        'athleteos-canonicalize-nutrition-wellness-event' (distinct from wellness,
        recovery, and cardio prefixes — no ProducerFencedException on concurrent
        writes to canonical.wellness_event)."""
        from jobs.nutrition_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        assert TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-nutrition-wellness-event"

    def test_sc25_triangulation_prefix_disjoint_from_wellness(self):
        """sc-25 triangulation: nutrition prefix differs from the wellness prefix."""
        from jobs.nutrition_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        wellness_prefix = "athleteos-canonicalize-wellness-event"
        assert TRANSACTIONAL_ID_PREFIX != wellness_prefix

    def test_sc25_triangulation_prefix_disjoint_from_recovery(self):
        """sc-25 triangulation: nutrition prefix differs from the recovery prefix."""
        from jobs.nutrition_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        recovery_prefix = "athleteos-canonicalize-recovery-wellness-event"
        assert TRANSACTIONAL_ID_PREFIX != recovery_prefix


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
        from jobs.nutrition_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id("not-json")
        assert result == ""

    def test_sc2_missing_event_id_returns_sentinel(self):
        """sc-2: valid JSON dict without event_id → \"\"."""
        import json
        from jobs.nutrition_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id(json.dumps({"other_field": "abc"}))
        assert result == ""

    def test_sc3_valid_dict_returns_event_id(self):
        """sc-3: valid JSON dict with event_id → the event_id value."""
        import json
        from jobs.nutrition_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id(json.dumps({"event_id": "evt-123"}))
        assert result == "evt-123"

    def test_sc4_non_dict_json_returns_sentinel(self):
        """sc-4: valid JSON non-dict (integer) → \"\" (no AttributeError)."""
        from jobs.nutrition_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id("42")
        assert result == ""

    def test_sc4b_empty_string_returns_sentinel(self):
        """sc-4b: empty string input → \"\"."""
        from jobs.nutrition_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id("")
        assert result == ""

    def test_sc4c_none_input_returns_sentinel(self):
        """sc-4c: None input → \"\" (no TypeError)."""
        from jobs.nutrition_canonicalize.transform import _key_by_event_id
        result = _key_by_event_id(None)
        assert result == ""
