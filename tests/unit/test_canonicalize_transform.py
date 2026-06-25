"""Unit tests for the canonicalize-job PURE transform logic (PR3, task 4.1/4.2).

These tests exercise ``jobs.canonicalize.transform`` only. That module imports
WITHOUT pyflink (the Flink wiring lives in ``jobs.canonicalize.main`` behind a
lazy import), so the whole suite is runnable without apache-flink installed
(which has no wheel for Python 3.14) and without a Docker daemon.

What is covered here (spec: event-contracts):
  - Common Event Envelope canonical shape: event_time/ingest_time as epoch-ms
    long, schema_version REQUIRED int, event_type, all TrainingEvent fields.
  - Source field mapping (Strong): payload fields -> canonical fields direct;
    timestamp -> event_time via "parse ISO -> epoch-ms".
  - session_load derivation for STRENGTH_SET
    (reps * weight_kg * (rpe / 10.0) when rpe present, else reps * weight_kg).
  - Validation: ValueError/missing-field -> ValidationError; session_load NaN ->
    ValidationError; unexpected mapping failure -> TransformError.
  - DLQ error envelope per spec (original_value base64, error_type, timestamp).
  - Avro roundtrip: canonical dict -> fastavro serialize -> deserialize matches
    the TrainingEvent.avsc schema (no pyflink needed).

This is the unit slice of PR3; the testcontainers-backed integration slice lives
in tests/integration/test_canonicalize_job.py and is Docker/pyflink-gated.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from jobs.canonicalize import transform as T


# --- helpers ---------------------------------------------------------------


def _raw_strength_envelope(
    *,
    event_id: str = "evt-1",
    event_time: str = "2024-01-15T10:30:00",  # naive ISO -> assumed UTC
    ingest_time: str = "2024-01-15T10:31:00",
    source: str = "strong_csv",
    athlete_id: str = "athlete-123",
    payload: dict | None = None,
) -> dict:
    """Build a raw.strength envelope exactly as PR2 produces it (ISO-8601
    strings, no schema_version, payload verbatim, no session_load)."""
    if payload is None:
        payload = {
            "workout_id": "w-001",
            "exercise_id": "bench-press",
            "set_number": 1,
            "reps": 8,
            "weight_kg": 100.0,
            "rpe": 8.5,
            "rir": 2.0,
            "timestamp": "2024-01-15T10:30:00",
        }
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "payload": payload,
    }


# --- parse_iso_to_epoch_ms -------------------------------------------------


def test_parse_iso_naive_assumed_utc_to_epoch_ms():
    """Naive ISO-8601 (no tz) is interpreted as UTC for deterministic epoch-ms."""
    # 2024-01-15T10:30:00 UTC -> 1705314600 s -> 1705314600000 ms
    assert T.parse_iso_to_epoch_ms("2024-01-15T10:30:00") == 1705314600000


def test_parse_iso_with_explicit_utc_offset():
    """ISO strings carrying an explicit offset must be honored as-is."""
    assert T.parse_iso_to_epoch_ms("2024-01-15T10:30:00+00:00") == 1705314600000


def test_parse_iso_with_non_utc_offset_normalizes_to_epoch_ms():
    """A +02:00 offset maps to the same instant as 10:30Z-2h = 08:30Z."""
    assert T.parse_iso_to_epoch_ms("2024-01-15T10:30:00+02:00") == 1705307400000


def test_parse_iso_garbage_raises_transform_error():
    with pytest.raises(T.TransformError):
        T.parse_iso_to_epoch_ms("not-a-timestamp")


# --- compute_strength_session_load ------------------------------------------


def test_session_load_with_rpe_matches_spec_scenario():
    """Spec scenario: reps=8, weight_kg=100, rpe=8.5 -> 8*100*(8.5/10)=680.0."""
    assert T.compute_strength_session_load(reps=8, weight_kg=100.0, rpe=8.5) == 680.0


def test_session_load_without_rpe_falls_back_to_volume():
    """Spec: when rpe absent -> reps * weight_kg."""
    assert T.compute_strength_session_load(reps=8, weight_kg=100.0, rpe=None) == 800.0


def test_session_load_rpe_zero_treated_as_absent():
    """rpe=0.0 is falsy; spec formula (rpe/10)*... yields 0 which is not a
    meaningful load. Treat rpe<=0 as 'absent' and fall back to reps*weight_kg
    (matches the absent-rpe branch; documented in transform)."""
    assert T.compute_strength_session_load(reps=10, weight_kg=50.0, rpe=0.0) == 500.0


# --- transform_strength_to_canonical ----------------------------------------


def test_transform_strength_maps_all_fields_and_adds_schema_version():
    raw = _raw_strength_envelope()
    out = T.transform_strength_to_canonical(raw, schema_version=1)

    # Common Event Envelope: epoch-ms longs + schema_version required
    assert out["event_id"] == "evt-1"
    assert out["event_time"] == 1705314600000  # ISO -> epoch-ms (naive=UTC)
    assert out["ingest_time"] == 1705314660000  # 10:31:00 UTC
    assert out["source"] == "strong_csv"
    assert out["schema_version"] == 1
    assert out["athlete_id"] == "athlete-123"
    assert out["event_type"] == "STRENGTH_SET"

    # direct payload mapping
    assert out["workout_id"] == "w-001"
    assert out["exercise_id"] == "bench-press"
    assert out["set_number"] == 1
    assert out["reps"] == 8
    assert out["weight_kg"] == 100.0
    assert out["rpe"] == 8.5
    assert out["rir"] == 2.0

    # cardio-only fields are null for a strength-sourced event
    assert out["activity_type"] is None
    assert out["distance_km"] is None
    assert out["duration_sec"] is None
    assert out["avg_hr"] is None
    assert out["tss"] is None

    # session_load derived (reps*weight_kg*(rpe/10) = 680.0 per spec)
    assert out["session_load"] == pytest.approx(680.0)


def test_transform_strength_derives_session_load_when_rpe_absent():
    raw = _raw_strength_envelope(
        payload={
            "workout_id": "w-002",
            "exercise_id": "squat",
            "set_number": 3,
            "reps": 5,
            "weight_kg": 120.0,
            "rpe": None,
            "rir": None,
            "timestamp": "2024-02-01T08:00:00",
        }
    )
    out = T.transform_strength_to_canonical(raw, schema_version=1)
    assert out["rpe"] is None
    assert out["rir"] is None
    # reps * weight_kg (no rpe)
    assert out["session_load"] == pytest.approx(5 * 120.0)
    assert out["event_type"] == "STRENGTH_SET"


def test_transform_strength_must_add_schema_version_not_present_in_raw():
    """Continuity note: raw envelope OMITS schema_version; canonical REQUIRES
    it. transform MUST add it (passed in by the Flink job from Registry)."""
    raw = _raw_strength_envelope()
    assert "schema_version" not in raw
    out = T.transform_strength_to_canonical(raw, schema_version=2)
    assert out["schema_version"] == 2


def test_transform_strength_missing_event_id_raises_validation_error():
    raw = _raw_strength_envelope()
    del raw["event_id"]
    with pytest.raises(T.ValidationError):
        T.transform_strength_to_canonical(raw, schema_version=1)


def test_transform_strength_missing_payload_raises_validation_error():
    raw = _raw_strength_envelope()
    del raw["payload"]
    with pytest.raises(T.ValidationError):
        T.transform_strength_to_canonical(raw, schema_version=1)


def test_transform_strength_payload_missing_reps_raises_validation_error():
    raw = _raw_strength_envelope()
    del raw["payload"]["reps"]
    with pytest.raises(T.ValidationError):
        T.transform_strength_to_canonical(raw, schema_version=1)


def test_transform_strength_unparseable_event_time_raises_transform_error():
    raw = _raw_strength_envelope(event_time="not-a-time")
    with pytest.raises(T.TransformError):
        T.transform_strength_to_canonical(raw, schema_version=1)


def test_transform_strength_non_numeric_weight_raises_transform_error():
    raw = _raw_strength_envelope()
    raw["payload"]["weight_kg"] = "heavy"  # type: ignore[assignment]
    with pytest.raises(T.TransformError):
        T.transform_strength_to_canonical(raw, schema_version=1)


# --- validate_training_event ------------------------------------------------


def test_validate_training_event_accepts_well_formed_event():
    event = T.transform_strength_to_canonical(_raw_strength_envelope(), schema_version=1)
    # must not raise
    T.validate_training_event(event)


def test_validate_training_event_rejects_nan_session_load():
    """Spec DLQ scenario: session_load = NaN -> VALIDATION_FAILURE."""
    event = T.transform_strength_to_canonical(_raw_strength_envelope(), schema_version=1)
    event["session_load"] = float("nan")
    with pytest.raises(T.ValidationError):
        T.validate_training_event(event)


def test_validate_training_event_rejects_missing_required_envelope_field():
    event = T.transform_strength_to_canonical(_raw_strength_envelope(), schema_version=1)
    del event["event_id"]
    with pytest.raises(T.ValidationError):
        T.validate_training_event(event)


# --- DLQ error envelope -----------------------------------------------------


def test_build_dlq_envelope_shape_and_base64_original_value():
    now_ms = 1719331200000
    original_value = b'{"bad":"event"}'
    env = T.build_dlq_envelope(
        original_topic="canonical.training_event",
        original_key="athlete-123",
        original_value=original_value,
        error_type=T.VALIDATION_FAILURE,
        error_message="Missing required field: session_load",
        timestamp=now_ms,
    )
    assert env["original_topic"] == "canonical.training_event"
    assert env["original_key"] == "athlete-123"
    # original_value MUST be base64-encoded bytes (spec DLQ envelope)
    assert env["original_value"] == base64.b64encode(original_value).decode("ascii")
    # roundtrip
    assert base64.b64decode(env["original_value"]) == original_value
    assert env["error_type"] == "VALIDATION_FAILURE"
    assert env["error_message"] == "Missing required field: session_load"
    assert env["timestamp"] == now_ms
    # error_stack optional
    assert "error_stack" in env
    assert env["error_stack"] is None


def test_build_dlq_envelope_accepts_str_original_value():
    """Original value may arrive as a JSON str (e.g. already-decoded raw).
    Either bytes or str must be encodable; str is utf-8 encoded first."""
    env = T.build_dlq_envelope(
        original_topic="dlq.canonical.training_event",
        original_key="athlete-123",
        original_value='{"x":1}',
        error_type=T.TRANSFORM_ERROR,
        error_message="boom",
        timestamp=1,
        error_stack="trace...",
    )
    assert base64.b64decode(env["original_value"]) == b'{"x":1}'
    assert env["error_stack"] == "trace..."


def test_build_dlq_envelope_serializes_to_json():
    """DLQ messages are JSON; the envelope must be json-dumpable."""
    env = T.build_dlq_envelope(
        original_topic="canonical.training_event",
        original_key="k",
        original_value=b"bytes",
        error_type=T.VALIDATION_FAILURE,
        error_message="m",
        timestamp=1,
    )
    s = json.dumps(env)  # must not raise
    assert "original_value" in json.loads(s)


# --- Avro roundtrip (fastavro, NO pyflink) ----------------------------------


def test_training_event_avsc_file_has_expected_fields():
    schema = T.load_training_event_avsc()
    names = [f["name"] for f in schema["fields"]]
    assert "event_id" in names
    assert "event_time" in names
    assert "schema_version" in names
    assert "event_type" in names
    assert "session_load" in names


def test_canonical_event_roundtrips_through_avro_against_schema():
    """A transformed canonical dict must serialize+deserialize as a valid
    TrainingEvent Avro record (fastavro stand-in for the Confluent Registry
    Avro serde the Flink job uses). Verifies field types/defaults coherence."""
    event = T.transform_strength_to_canonical(_raw_strength_envelope(), schema_version=1)
    schema = T.load_training_event_avsc()

    encoded = T.serialize_training_event_avro(event, schema)
    assert isinstance(encoded, (bytes, bytearray))
    assert len(encoded) > 0

    decoded = T.deserialize_training_event_avro(encoded, schema)
    # epoch-ms longs survive intact. Avro `timestamp-millis` logicalType
    # decodes to a tz-aware datetime; compare via epoch-ms normalization.
    def _ms(v):
        if isinstance(v, datetime):
            return int(v.timestamp() * 1000)
        return int(v)

    assert decoded["event_id"] == "evt-1"
    assert _ms(decoded["event_time"]) == 1705314600000
    assert _ms(decoded["ingest_time"]) == 1705314660000
    assert decoded["schema_version"] == 1
    assert decoded["event_type"] == "STRENGTH_SET"
    assert decoded["session_load"] == pytest.approx(680.0)
    # cardio fields defaulted to null survive
    assert decoded["tss"] is None
    assert decoded["avg_hr"] is None


def test_canonical_event_roundtrips_when_rpe_absent():
    raw = _raw_strength_envelope(
        payload={
            "workout_id": "w-003",
            "exercise_id": "deadlift",
            "set_number": 1,
            "reps": 3,
            "weight_kg": 180.0,
            "rpe": None,
            "rir": None,
            "timestamp": "2024-03-01T09:00:00",
        }
    )
    event = T.transform_strength_to_canonical(raw, schema_version=1)
    schema = T.load_training_event_avsc()
    decoded = T.deserialize_training_event_avro(
        T.serialize_training_event_avro(event, schema), schema
    )
    assert decoded["rpe"] is None
    assert decoded["rir"] is None
    assert decoded["session_load"] == pytest.approx(3 * 180.0)