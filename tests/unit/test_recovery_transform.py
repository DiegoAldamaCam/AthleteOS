"""Unit tests for the recovery canonicalize PURE transform logic (PR-R2).

Mirrors tests/unit/test_cardio_transform.py and test_wellness_transform.py structure.

Covers (sc-12..sc-24):
  sc-12: validate_recovery_event — valid envelope with athlete_id + event_time → no error
  sc-13: validate_recovery_event — missing athlete_id → ValidationError → DLQ
  sc-14: validate_recovery_event — missing event_time → ValidationError → DLQ
  sc-15: transform_recovery_to_canonical — full mapping asserts event_type="RECOVERY_SNAPSHOT"
         and all 5 Apple Health fields with correct types
  sc-16: transform_recovery_to_canonical — all-null data fields → valid dict, event_type still
         "RECOVERY_SNAPSHOT"
  sc-17: fastavro roundtrip — canonical dict matches WellnessEvent.avsc field-for-field
  sc-23: TRANSACTIONAL_ID_PREFIX constant equals exact literal string (no Docker needed)

No pyflink dependency. Runnable on CPython 3.14 without Docker.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from jobs.recovery_canonicalize.transform import (
    ValidationError,
    validate_recovery_event,
    transform_recovery_to_canonical,
    load_wellness_event_avsc,
    serialize_wellness_event_avro,
    deserialize_wellness_event_avro,
    RECOVERY_SNAPSHOT,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# 2025-06-01 UTC midnight epoch-ms
_EPOCH_MS_2025_06_01 = 1748736000000


def _raw_recovery_envelope(
    *,
    event_id: str = "revt-001",
    event_time: int = _EPOCH_MS_2025_06_01,
    ingest_time: int = _EPOCH_MS_2025_06_01 + 5_000,
    source: str = "apple_health",
    athlete_id: str = "A1",
    sleep_hours: float | None = 7.5,
    resting_hr: int | None = 58,
    hrv: float | None = 42.0,
    steps: int | None = 8500,
    body_weight_kg: float | None = 72.3,
) -> dict:
    """Build a raw.recovery envelope as the ingestion/recovery producer emits it."""
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "payload": {
            "sleep_hours": sleep_hours,
            "resting_hr": resting_hr,
            "hrv": hrv,
            "steps": steps,
            "body_weight_kg": body_weight_kg,
        },
    }


# ---------------------------------------------------------------------------
# sc-12..sc-14: validate_recovery_event
# ---------------------------------------------------------------------------


class TestValidateRecoveryEvent:
    def test_sc12_valid_envelope_with_required_fields_no_error(self):
        """sc-12: Valid envelope with athlete_id and event_time → no exception raised."""
        raw = _raw_recovery_envelope()
        validate_recovery_event(raw)  # must not raise

    def test_sc12_triangulation_all_data_fields_null_still_valid(self):
        """sc-12 triangulation: all 5 data fields null — validation still passes."""
        raw = _raw_recovery_envelope(
            sleep_hours=None,
            resting_hr=None,
            hrv=None,
            steps=None,
            body_weight_kg=None,
        )
        validate_recovery_event(raw)  # must not raise (data fields are optional)

    def test_sc13_missing_athlete_id_raises_validation_error(self):
        """sc-13: Missing athlete_id → ValidationError."""
        raw = _raw_recovery_envelope()
        del raw["athlete_id"]
        with pytest.raises(ValidationError):
            validate_recovery_event(raw)

    def test_sc13_null_athlete_id_raises_validation_error(self):
        """sc-13 triangulation: null athlete_id → ValidationError."""
        raw = _raw_recovery_envelope()
        raw["athlete_id"] = None
        with pytest.raises(ValidationError):
            validate_recovery_event(raw)

    def test_sc14_missing_event_time_raises_validation_error(self):
        """sc-14: Missing event_time → ValidationError, original_topic='raw.recovery'."""
        raw = _raw_recovery_envelope()
        del raw["event_time"]
        with pytest.raises(ValidationError):
            validate_recovery_event(raw)

    def test_sc14_null_event_time_raises_validation_error(self):
        """sc-14 triangulation: null event_time → ValidationError."""
        raw = _raw_recovery_envelope()
        raw["event_time"] = None
        with pytest.raises(ValidationError):
            validate_recovery_event(raw)


# ---------------------------------------------------------------------------
# sc-15..sc-16: transform_recovery_to_canonical
# ---------------------------------------------------------------------------


class TestTransformRecoveryToCanonical:
    def test_sc15_full_canonical_mapping(self):
        """sc-15: Full mapping — event_type='RECOVERY_SNAPSHOT', all 5 Apple Health
        fields present, wellness/nutrition/subjective fields = None."""
        raw = _raw_recovery_envelope(
            sleep_hours=7.5,
            resting_hr=58,
            hrv=42.0,
            steps=8500,
            body_weight_kg=72.3,
        )
        result = transform_recovery_to_canonical(raw, schema_version=1)

        # event_type MUST be the hardcoded constant
        assert result["event_type"] == "RECOVERY_SNAPSHOT"
        assert result["event_type"] == RECOVERY_SNAPSHOT

        # All 5 Apple Health fields must match source values with correct types
        assert result["sleep_hours"] == pytest.approx(7.5)
        assert result["resting_hr"] == 58
        assert result["hrv"] == pytest.approx(42.0)
        assert result["steps"] == 8500
        assert result["body_weight_kg"] == pytest.approx(72.3)

        # Required envelope fields must be present
        assert result["event_id"] == raw["event_id"]
        assert result["event_time"] == raw["event_time"]
        assert result["athlete_id"] == raw["athlete_id"]
        assert result["schema_version"] == 1

        # Wellness/nutrition/subjective fields MUST be None
        assert result["calories"] is None
        assert result["protein_g"] is None
        assert result["carbs_g"] is None
        assert result["fat_g"] is None
        assert result["nutrition_adherence"] is None
        assert result["energy"] is None
        assert result["soreness"] is None
        assert result["mood"] is None
        assert result["stress"] is None
        assert result["perceived_recovery"] is None

    def test_sc15_triangulation_event_type_hardcoded_not_from_payload(self):
        """sc-15 triangulation: event_type is HARDCODED, never read from payload."""
        raw = _raw_recovery_envelope()
        # Even if we inject a different event_type-like field in payload,
        # the transform must still produce RECOVERY_SNAPSHOT
        raw["payload"]["event_type"] = "WELLNESS_DAILY"  # injection attempt
        result = transform_recovery_to_canonical(raw, schema_version=1)
        assert result["event_type"] == "RECOVERY_SNAPSHOT"

    def test_sc16_all_null_data_fields_valid_dict_event_type_recovery(self):
        """sc-16: All 5 data fields null → valid dict, no exception, event_type still
        RECOVERY_SNAPSHOT."""
        raw = _raw_recovery_envelope(
            sleep_hours=None,
            resting_hr=None,
            hrv=None,
            steps=None,
            body_weight_kg=None,
        )
        result = transform_recovery_to_canonical(raw, schema_version=1)

        # Must NOT raise; must return valid dict
        assert isinstance(result, dict)
        assert result["event_type"] == "RECOVERY_SNAPSHOT"

        # All 5 Apple Health data fields must be None
        assert result["sleep_hours"] is None
        assert result["resting_hr"] is None
        assert result["hrv"] is None
        assert result["steps"] is None
        assert result["body_weight_kg"] is None

    def test_sc16_triangulation_mixed_null_fields(self):
        """sc-16 triangulation: only some fields null — still valid, event_type unchanged."""
        raw = _raw_recovery_envelope(hrv=None, steps=None, sleep_hours=6.0)
        result = transform_recovery_to_canonical(raw, schema_version=1)
        assert result["event_type"] == "RECOVERY_SNAPSHOT"
        assert result["hrv"] is None
        assert result["steps"] is None
        assert result["sleep_hours"] == pytest.approx(6.0)

    def test_sc13_transform_raises_on_missing_athlete_id(self):
        """sc-13 via transform: missing athlete_id in raw → ValidationError."""
        raw = _raw_recovery_envelope()
        del raw["athlete_id"]
        with pytest.raises(ValidationError):
            transform_recovery_to_canonical(raw, schema_version=1)


# ---------------------------------------------------------------------------
# sc-17: Fastavro roundtrip — canonical dict ↔ WellnessEvent.avsc
# ---------------------------------------------------------------------------


class TestFastavroRoundtrip:
    def _make_canonical_full(self) -> dict:
        raw = _raw_recovery_envelope(
            event_id="revt-roundtrip-001",
            sleep_hours=7.5,
            resting_hr=58,
            hrv=42.0,
            steps=8500,
            body_weight_kg=72.3,
        )
        return transform_recovery_to_canonical(raw, schema_version=1)

    def test_sc17_roundtrip_full_fields(self):
        """sc-17: fastavro schemaless_writer → schemaless_reader round-trips correctly."""
        fastavro = pytest.importorskip("fastavro")
        schema = load_wellness_event_avsc()
        canonical = self._make_canonical_full()

        encoded = serialize_wellness_event_avro(canonical, schema)
        assert isinstance(encoded, (bytes, bytearray))
        assert len(encoded) > 0

        decoded = deserialize_wellness_event_avro(encoded, schema)

        # event_type must survive as RECOVERY_SNAPSHOT (string)
        assert decoded["event_type"] == "RECOVERY_SNAPSHOT"
        assert decoded["event_id"] == "revt-roundtrip-001"
        assert decoded["athlete_id"] == "A1"
        assert decoded["schema_version"] == 1

        # Timestamp handling: fastavro may decode timestamp-millis as datetime
        def _ms(v):
            if isinstance(v, datetime):
                return int(v.timestamp() * 1000)
            return int(v)

        assert _ms(decoded["event_time"]) == _EPOCH_MS_2025_06_01
        assert decoded["sleep_hours"] == pytest.approx(7.5)
        assert decoded["hrv"] == pytest.approx(42.0)
        assert decoded["resting_hr"] == 58
        assert decoded["steps"] == 8500

        # Wellness/nutrition/subjective fields must be None
        assert decoded["calories"] is None
        assert decoded["protein_g"] is None
        assert decoded["energy"] is None
        assert decoded["perceived_recovery"] is None

    def test_sc17_triangulation_all_null_data_roundtrip(self):
        """sc-17 triangulation: all-null data fields also round-trip without error."""
        fastavro = pytest.importorskip("fastavro")
        schema = load_wellness_event_avsc()
        raw = _raw_recovery_envelope(
            event_id="revt-roundtrip-002",
            sleep_hours=None,
            resting_hr=None,
            hrv=None,
            steps=None,
            body_weight_kg=None,
        )
        canonical = transform_recovery_to_canonical(raw, schema_version=1)
        encoded = serialize_wellness_event_avro(canonical, schema)
        decoded = deserialize_wellness_event_avro(encoded, schema)

        assert decoded["event_type"] == "RECOVERY_SNAPSHOT"
        assert decoded["sleep_hours"] is None
        assert decoded["hrv"] is None
        assert decoded["body_weight_kg"] is None


# ---------------------------------------------------------------------------
# sc-23: TRANSACTIONAL_ID_PREFIX constant (no Docker required)
# ---------------------------------------------------------------------------


class TestTransactionalIdPrefix:
    def test_sc23_transactional_id_prefix_exact_literal(self):
        """sc-23: TRANSACTIONAL_ID_PREFIX must be the exact literal string
        'athleteos-canonicalize-recovery-wellness-event' (distinct from wellness/cardio
        prefixes — no ProducerFencedException on concurrent writes)."""
        from jobs.recovery_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        assert TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-recovery-wellness-event"

    def test_sc23_triangulation_prefix_disjoint_from_wellness(self):
        """sc-23 triangulation: recovery prefix differs from the wellness prefix."""
        from jobs.recovery_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        wellness_prefix = "athleteos-canonicalize-wellness-event"
        assert TRANSACTIONAL_ID_PREFIX != wellness_prefix

    def test_sc23_triangulation_prefix_disjoint_from_cardio(self):
        """sc-23 triangulation: recovery prefix differs from the cardio prefix."""
        from jobs.recovery_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        cardio_prefix = "athleteos-canonicalize-cardio-training-event"
        assert TRANSACTIONAL_ID_PREFIX != cardio_prefix
