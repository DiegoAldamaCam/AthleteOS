"""Unit tests for jobs.planning_canonicalize.transform — pure canonicalize logic.

Covers:
  - transform_planning_to_canonical: all 12 Avro fields (PL2-4)
  - fastavro round-trip vs PlanningBlock.avsc (PL2-5)
  - validate_planning_block: end_date < start_date → DLQ (PL2-6)
  - validate_planning_block: planned_sessions_per_week <= 0 → DLQ (PL2-7)
  - validate_planning_block: malformed weekly_volume_targets → DLQ (PL2-8)

No pyflink dependency. Runnable on CPython 3.14 without Docker.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from jobs.planning_canonicalize.transform import (
    ValidationError,
    TransformError,
    transform_planning_to_canonical,
    validate_planning_block,
    load_planning_block_avsc,
    serialize_planning_block_avro,
    deserialize_planning_block_avro,
    select_dlq_error_type,
    build_dlq_envelope,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_START_DATE_ISO = "2025-06-01"
_END_DATE_ISO = "2025-08-31"

# 2025-06-01 00:00:00 UTC midnight → epoch-ms
_START_DATE_EPOCH_MS = 1_748_736_000_000
# 2025-08-31 00:00:00 UTC midnight → epoch-ms
_END_DATE_EPOCH_MS = 1_756_598_400_000

_INGEST_TIME_MS = 1_748_740_000_000


def _raw_planning_envelope(
    *,
    event_id: str = "evt-plan-001",
    event_time: int = _START_DATE_EPOCH_MS,
    ingest_time: int = _INGEST_TIME_MS,
    source: str = "planning_connector",
    athlete_id: str = "A1",
    block_id: str = "BLK-001",
    goal: str = "Build aerobic base",
    start_date: str = _START_DATE_ISO,
    end_date: str = _END_DATE_ISO,
    planned_sessions_per_week: int = 5,
    weekly_volume_targets: str = '{"strength": 3, "cardio": 2}',
) -> dict:
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "block_id": block_id,
        "goal": goal,
        "start_date": start_date,
        "end_date": end_date,
        "planned_sessions_per_week": planned_sessions_per_week,
        "weekly_volume_targets": weekly_volume_targets,
    }


# ---------------------------------------------------------------------------
# PL2-4: All 12 canonical fields mapped
# ---------------------------------------------------------------------------


class TestTransformPlanningToCanonical:
    """transform_planning_to_canonical maps all 12 Avro fields correctly (PL2-4)."""

    def test_all_12_avro_fields_present(self):
        """PL2-4: canonical dict must contain all 12 PlanningBlock.avsc fields."""
        raw = _raw_planning_envelope()
        result = transform_planning_to_canonical(raw, schema_version=1)

        expected_fields = {
            "event_id", "event_time", "ingest_time", "source", "schema_version",
            "athlete_id", "block_id", "goal",
            "start_date", "end_date",
            "planned_sessions_per_week", "weekly_volume_targets",
        }
        assert set(result.keys()) == expected_fields, (
            f"Missing or extra fields. Got: {set(result.keys())}"
        )

    def test_event_id_carried_through(self):
        raw = _raw_planning_envelope(event_id="evt-xyz-999")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["event_id"] == "evt-xyz-999"

    def test_schema_version_injected(self):
        raw = _raw_planning_envelope()
        result = transform_planning_to_canonical(raw, schema_version=3)
        assert result["schema_version"] == 3

    def test_source_carried_through(self):
        raw = _raw_planning_envelope(source="file_watcher")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["source"] == "file_watcher"

    def test_athlete_id_carried_through(self):
        raw = _raw_planning_envelope(athlete_id="A42")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["athlete_id"] == "A42"

    def test_block_id_carried_through(self):
        raw = _raw_planning_envelope(block_id="BLK-999")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["block_id"] == "BLK-999"

    def test_goal_carried_through(self):
        raw = _raw_planning_envelope(goal="Peak for competition")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["goal"] == "Peak for competition"

    def test_start_date_iso_to_epoch_ms(self):
        """start_date must be epoch-ms long, not ISO string (PL2-4)."""
        raw = _raw_planning_envelope(start_date="2025-06-01")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["start_date"] == _START_DATE_EPOCH_MS, (
            f"start_date must be {_START_DATE_EPOCH_MS} (2025-06-01 UTC midnight), "
            f"got {result['start_date']!r}"
        )
        assert isinstance(result["start_date"], int)

    def test_end_date_iso_to_epoch_ms(self):
        """end_date must be epoch-ms long, not ISO string (PL2-4)."""
        raw = _raw_planning_envelope(end_date="2025-08-31")
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["end_date"] == _END_DATE_EPOCH_MS, (
            f"end_date must be {_END_DATE_EPOCH_MS} (2025-08-31 UTC midnight), "
            f"got {result['end_date']!r}"
        )
        assert isinstance(result["end_date"], int)

    def test_planned_sessions_per_week_is_int(self):
        """planned_sessions_per_week must be int (PL2-4)."""
        raw = _raw_planning_envelope(planned_sessions_per_week=4)
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["planned_sessions_per_week"] == 4
        assert isinstance(result["planned_sessions_per_week"], int)

    def test_weekly_volume_targets_is_json_string(self):
        """weekly_volume_targets must be a JSON string, not a dict (PL2-4)."""
        wvt_dict = {"strength": 3, "cardio": 2}
        raw = _raw_planning_envelope(weekly_volume_targets=json.dumps(wvt_dict))
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert isinstance(result["weekly_volume_targets"], str)
        # round-trip: the string must deserialize back to the original dict
        assert json.loads(result["weekly_volume_targets"]) == wvt_dict

    def test_ingest_time_carried_through_as_int(self):
        raw = _raw_planning_envelope(ingest_time=_INGEST_TIME_MS)
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["ingest_time"] == _INGEST_TIME_MS
        assert isinstance(result["ingest_time"], int)

    def test_event_time_carried_through_as_int(self):
        raw = _raw_planning_envelope(event_time=_START_DATE_EPOCH_MS)
        result = transform_planning_to_canonical(raw, schema_version=1)
        assert result["event_time"] == _START_DATE_EPOCH_MS

    # --- triangulation: different inputs produce different outputs ---

    def test_different_block_different_dates(self):
        """Triangulation: a second block with different dates maps independently."""
        raw = _raw_planning_envelope(
            block_id="BLK-002",
            start_date="2025-09-01",
            end_date="2025-11-30",
            planned_sessions_per_week=3,
        )
        result = transform_planning_to_canonical(raw, schema_version=2)
        # 2025-09-01 UTC midnight
        assert result["start_date"] == 1_756_684_800_000
        assert result["block_id"] == "BLK-002"
        assert result["planned_sessions_per_week"] == 3
        assert result["schema_version"] == 2


# ---------------------------------------------------------------------------
# PL2-5: fastavro round-trip
# ---------------------------------------------------------------------------


def _ms(v: Any) -> int:
    """Normalize a fastavro-decoded timestamp field to epoch-ms int.

    fastavro deserializes ``timestamp-millis`` logical-type fields as
    tz-aware ``datetime.datetime`` objects. This helper converts them
    back to epoch-ms so tests can compare against the original int values.
    """
    from datetime import datetime as _dt
    if isinstance(v, _dt):
        return int(v.timestamp() * 1000)
    return int(v)


class TestFastavroRoundtrip:
    """PL2-5: Canonical PlanningBlock dict survives fastavro schemaless roundtrip."""

    def test_roundtrip_preserves_all_fields(self):
        """PL2-5: serialize + deserialize must preserve every field value.

        fastavro decodes ``timestamp-millis`` fields as ``datetime`` objects;
        we normalize via _ms() for comparison (mirrors test_canonicalize_transform
        pattern for TrainingEvent.avsc roundtrip).
        """
        raw = _raw_planning_envelope()
        canonical = transform_planning_to_canonical(raw, schema_version=1)
        schema = load_planning_block_avsc()

        encoded = serialize_planning_block_avro(canonical, schema)
        assert isinstance(encoded, bytes) and len(encoded) > 0

        decoded = deserialize_planning_block_avro(encoded, schema)

        # String / int fields survive intact
        assert decoded["event_id"] == canonical["event_id"]
        assert decoded["source"] == canonical["source"]
        assert decoded["schema_version"] == canonical["schema_version"]
        assert decoded["athlete_id"] == canonical["athlete_id"]
        assert decoded["block_id"] == canonical["block_id"]
        assert decoded["goal"] == canonical["goal"]
        assert decoded["planned_sessions_per_week"] == canonical["planned_sessions_per_week"]
        assert decoded["weekly_volume_targets"] == canonical["weekly_volume_targets"]

        # timestamp-millis fields: normalize to epoch-ms for comparison
        assert _ms(decoded["event_time"]) == canonical["event_time"]
        assert _ms(decoded["ingest_time"]) == canonical["ingest_time"]
        assert _ms(decoded["start_date"]) == canonical["start_date"]
        assert _ms(decoded["end_date"]) == canonical["end_date"]

    def test_roundtrip_with_different_wvt(self):
        """Triangulation: different wvt string also survives roundtrip (PL2-5)."""
        wvt = json.dumps({"strength": 5, "cardio": 1, "endurance": 2})
        raw = _raw_planning_envelope(weekly_volume_targets=wvt, planned_sessions_per_week=8)
        canonical = transform_planning_to_canonical(raw, schema_version=1)
        schema = load_planning_block_avsc()

        encoded = serialize_planning_block_avro(canonical, schema)
        decoded = deserialize_planning_block_avro(encoded, schema)

        assert decoded["weekly_volume_targets"] == wvt
        assert decoded["planned_sessions_per_week"] == 8


# ---------------------------------------------------------------------------
# PL2-6: end_date < start_date → ValidationError → DLQ
# ---------------------------------------------------------------------------


class TestValidatePlanningBlockEndBeforeStart:
    """PL2-6: end_date < start_date triggers ValidationError routed to DLQ."""

    def test_end_before_start_raises_validation_error(self):
        raw = _raw_planning_envelope(
            start_date="2025-06-15",
            end_date="2025-06-01",  # earlier than start
        )
        with pytest.raises(ValidationError, match="end_date"):
            validate_planning_block(raw)

    def test_transform_end_before_start_raises(self):
        """Transform path: end_date < start_date must also raise (PL2-6)."""
        raw = _raw_planning_envelope(
            start_date="2025-09-01",
            end_date="2025-08-01",
        )
        with pytest.raises(ValidationError):
            validate_planning_block(raw)

    def test_select_dlq_error_type_is_validation_failure(self):
        """ValidationError maps to VALIDATION_FAILURE via select_dlq_error_type (PL2-6)."""
        exc = ValidationError("end_date < start_date")
        error_type = select_dlq_error_type(exc)
        assert error_type == "VALIDATION_FAILURE"

    def test_build_dlq_envelope_contains_validation_failure(self):
        """DLQ envelope shape matches spec (PL2-6): error_type, base64, timestamp."""
        raw_value = '{"end_date": "2025-06-01"}'
        envelope = build_dlq_envelope(
            original_topic="raw.planning",
            original_key="A1",
            original_value=raw_value,
            error_type="VALIDATION_FAILURE",
            error_message="end_date < start_date",
            timestamp=_INGEST_TIME_MS,
        )
        assert envelope["error_type"] == "VALIDATION_FAILURE"
        assert envelope["original_topic"] == "raw.planning"
        assert envelope["timestamp"] == _INGEST_TIME_MS
        # original_value must be base64-encoded
        import base64
        decoded = base64.b64decode(envelope["original_value"]).decode("utf-8")
        assert decoded == raw_value


# ---------------------------------------------------------------------------
# PL2-7: planned_sessions_per_week <= 0 → ValidationError → DLQ
# ---------------------------------------------------------------------------


class TestValidatePlanningBlockSessionsLteZero:
    """PL2-7: planned_sessions_per_week <= 0 triggers ValidationError."""

    def test_zero_sessions_raises_validation_error(self):
        raw = _raw_planning_envelope(planned_sessions_per_week=0)
        with pytest.raises(ValidationError, match="planned_sessions_per_week"):
            validate_planning_block(raw)

    def test_negative_sessions_raises_validation_error(self):
        raw = _raw_planning_envelope(planned_sessions_per_week=-1)
        with pytest.raises(ValidationError, match="planned_sessions_per_week"):
            validate_planning_block(raw)

    def test_positive_sessions_does_not_raise(self):
        """Triangulation: positive sessions pass validation (PL2-7)."""
        raw = _raw_planning_envelope(planned_sessions_per_week=1)
        validate_planning_block(raw)  # must not raise

    def test_five_sessions_does_not_raise(self):
        raw = _raw_planning_envelope(planned_sessions_per_week=5)
        validate_planning_block(raw)  # must not raise


# ---------------------------------------------------------------------------
# PL2-8: malformed weekly_volume_targets → ValidationError → DLQ
# ---------------------------------------------------------------------------


class TestValidatePlanningBlockMalformedWvt:
    """PL2-8: malformed wvt JSON string triggers ValidationError."""

    def test_bare_word_wvt_raises_validation_error(self):
        """wvt that is not valid JSON (bare word) must raise ValidationError."""
        raw = _raw_planning_envelope(weekly_volume_targets="not-json")
        with pytest.raises(ValidationError, match="weekly_volume_targets"):
            validate_planning_block(raw)

    def test_unclosed_bracket_raises_validation_error(self):
        """Triangulation: unclosed JSON bracket also raises ValidationError."""
        raw = _raw_planning_envelope(weekly_volume_targets='{"strength": 3')
        with pytest.raises(ValidationError, match="weekly_volume_targets"):
            validate_planning_block(raw)

    def test_valid_json_wvt_does_not_raise(self):
        """Valid JSON string must NOT raise ValidationError (PL2-8)."""
        raw = _raw_planning_envelope(weekly_volume_targets='{"cardio": 3}')
        validate_planning_block(raw)  # must not raise

    def test_dlq_error_type_for_wvt_error(self):
        """ValidationError from malformed wvt → VALIDATION_FAILURE (PL2-8)."""
        exc = ValidationError("weekly_volume_targets is not valid JSON")
        assert select_dlq_error_type(exc) == "VALIDATION_FAILURE"
