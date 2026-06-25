"""PURE canonicalization logic for the strength -> canonical.training_event
transform (PR3, task 4.1/4.2).

This module is deliberately pyflink-free so unit tests run on interpreters where
apache-flink has no wheel (CPython 3.14) and without a Docker daemon. The Flink
job wiring (jobs/canonicalize/main.py) calls into these pure functions from
inside its ``KeyedProcessFunction`` and import-isolates pyflink.

Contracts (event-contracts spec):
  - Common Event Envelope: event_time/ingest_time are epoch-ms longs,
    schema_version is a REQUIRED int (added here; the raw envelope omits it).
  - TrainingEvent Avro schema; event_type = STRENGTH_SET for strength-sourced
    events.
  - session_load derivation for STRENGTH_SET:
        reps * weight_kg * (rpe / 10.0)   when rpe present
        reps * weight_kg                  when rpe absent
  - DLQ error envelope (JSON, AT_LEAST_ONCE sink):
        original_topic, original_key, original_value (base64), error_type,
        error_message, error_stack, timestamp

Field mapping for strength (spec "Source Field Mappings"):
  raw envelope -> canonical TrainingEvent
    event_id        -> event_id            (direct)
    event_time      -> event_time          (ISO -> epoch-ms long; naive=UTC)
    ingest_time     -> ingest_time         (ISO -> epoch-ms long; naive=UTC)
    source          -> source
    (job-supplied)  -> schema_version
    athlete_id      -> athlete_id
    payload.workout_id    -> workout_id
    payload.exercise_id   -> exercise_id
    payload.set_number    -> set_number
    payload.reps          -> reps
    payload.weight_kg     -> weight_kg
    payload.rpe           -> rpe   (nullable)
    payload.rir           -> rir   (nullable)
    computed              -> session_load
    (constant)           -> event_type = STRENGTH_SET
    (n/a for strength)   -> activity_type/distance_km/duration_sec/avg_hr/tss = null
"""

from __future__ import annotations

import base64
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# --- DLQ error types (spec DLQ Routing table) -----------------------------

VALIDATION_FAILURE = "VALIDATION_FAILURE"
SCHEMA_INCOMPATIBILITY = "SCHEMA_INCOMPATIBILITY"
DESERIALIZATION_ERROR = "DESERIALIZATION_ERROR"
TRANSFORM_ERROR = "TRANSFORM_ERROR"

# --- Canonical event types -------------------------------------------------

STRENGTH_SET = "STRENGTH_SET"
CARDIO_ACTIVITY = "CARDIO_ACTIVITY"

# Required Common Event Envelope + required TrainingEvent fields.
# (session_load is required on the Avro record; null cardio fields are allowed.)
_REQUIRED_ENVELOPE_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "ingest_time",
    "source",
    "schema_version",
    "athlete_id",
    "event_type",
    "session_load",
)


# --- Exceptions ------------------------------------------------------------


class ValidationError(Exception):
    """Missing/invalid required field or out-of-range value (-> DLQ)."""


class TransformError(Exception):
    """Raw->canonical mapping failure (-> DLQ)."""


# --- Timestamp mapping: parse ISO -> epoch-ms ------------------------------


def parse_iso_to_epoch_ms(iso_str: str) -> int:
    """Parse an ISO-8601 string to epoch milliseconds (long).

    Naive timestamps (no tz offset) are interpreted as UTC so the result is
    deterministic regardless of the host's local timezone - raw-strength event
    times from Strong CSV carry no offset, and canonical event_time/ingest_time
    semantics are real-world occurrence time in UTC.

    Raises TransformError on unparseable input.
    """
    if not isinstance(iso_str, str) or iso_str == "":
        raise TransformError(f"unparseable timestamp: {iso_str!r}")
    try:
        dt = datetime.fromisoformat(iso_str)
    except ValueError as exc:
        raise TransformError(f"unparseable timestamp: {iso_str!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# --- session_load derivation (spec STRENGTH_SET formula) -------------------


def compute_strength_session_load(reps: int, weight_kg: float, rpe: float | None) -> float:
    """Compute session_load for a STRENGTH_SET per the spec formula.

        session_load = reps * weight_kg * (rpe / 10.0)   when rpe present
        session_load = reps * weight_kg                  when rpe absent

    Out-of-range handling (spec DLQ rule line ~357: out-of-range value ->
    VALIDATION_FAILURE):
      - ``rpe < 0`` is physically impossible. A negative RPE is an out-of-range
        data-quality problem, NOT an absent measurement, so the source record
        MUST be routed to the DLQ (error_type=VALIDATION_FAILURE) instead of
        being silently normalized via the volume-only proxy. We raise
        ``ValidationError`` here; the canonicalize ProcessFunction catches it
        and routes via :func:`select_dlq_error_type`.
      - ``rpe == 0`` (falsy zero) is treated as 'absent' and mapped to the
        volume-only branch: RPE 0 is not a meaningful measurement (no
        RPE-assisted set is actually rated 0), so the volume-only proxy is the
        documented fallback (decision preserved from PR3 v1).
    """
    if reps is None or weight_kg is None:
        raise ValidationError("session_load requires non-null reps and weight_kg")
    if rpe is not None and rpe < 0:
        raise ValidationError(
            f"rpe out of range (negative): {rpe!r} -- route to DLQ as "
            f"VALIDATION_FAILURE (spec DLQ Routing table line ~357)"
        )
    if rpe is None or rpe == 0:
        return float(reps) * float(weight_kg)
    return float(reps) * float(weight_kg) * (float(rpe) / 10.0)


# --- DLQ error_type routing (pure helper, spec DLQ Routing table) ------------


def select_dlq_error_type(exc: BaseException) -> str:
    """Pick the spec DLQ ``error_type`` string for a raised exception.

    Spec DLQ Routing rules (event-contracts spec ~line 353):
      - VALIDATION_FAILURE      <- "Missing required field, out-of-range value"
      - SCHEMA_INCOMPATIBILITY  <- Schema Registry rejects producer write
                                   (not raised here; surfaced by the sink).
      - DESERIALIZATION_ERROR   <- malformed Avro bytes on the CONSUMER side
                                   (raw side is JSON here, NOT Avro, so this
                                   code never applies to raw-strength failures).
      - TRANSFORM_ERROR         <- raw->canonical mapping failure.

    This helper is intentionally pure (no pyflink) so DLQ routing has unit
    coverage independent of the gated Flink integration test. The canonicalize
    ProcessFunction (jobs/canonicalize/main.py) catches ``ValidationError`` /
    ``TransformError`` (and any re-raised ``TransformError`` over a malformed
    JSON envelope) and dispatches via this helper.
    """
    if isinstance(exc, ValidationError):
        return VALIDATION_FAILURE
    # Any other exception during canonicalization is a raw->canonical mapping
    # failure (covers malformed raw JSON, unexpected coercion failures, etc.).
    return TRANSFORM_ERROR


# --- core transform: raw strength envelope -> canonical TrainingEvent -------


def _coerce_int(value: Any, field: str) -> int:
    if isinstance(value, bool):  # guard: bool is an int subclass
        raise TransformError(f"{field!r} must be int, got bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TransformError(f"{field!r} not coercible to int: {value!r}") from exc


def _coerce_float(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise TransformError(f"{field!r} must be float, got bool")
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TransformError(f"{field!r} not coercible to float: {value!r}") from exc


def _coerce_optional_float(value: Any, field: str) -> float | None:
    if value is None:
        return None
    return _coerce_float(value, field)


def transform_strength_to_canonical(raw: dict, schema_version: int) -> dict:
    """Map a raw.strength envelope (JSON) to a canonical TrainingEvent dict
    (Avro-ready):

      - event_time/ingest_time: ISO-8601 -> epoch-ms long (naive = UTC)
      - schema_version: added (REQUIRED; absent from raw)
      - event_type: STRENGTH_SET
      - session_load: derived per spec formula
      - cardio-only fields: null

    Raises ValidationError for missing required fields, TransformError for
    mapping/parse failures. Either is routed to the DLQ by the Flink job.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw envelope must be a dict")

    # envelope-level required fields (payload handled below)
    for field in ("event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"):
        if field not in raw or raw[field] is None:
            raise ValidationError(f"missing required raw envelope field: {field!r}")

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValidationError("raw envelope 'payload' must be a dict")

    for field in ("reps", "weight_kg"):
        if field not in payload or payload[field] is None:
            raise ValidationError(f"missing required payload field: {field!r}")

    # event_time / ingest_time: ISO -> epoch-ms long
    try:
        event_time_ms = parse_iso_to_epoch_ms(raw["event_time"])
        ingest_time_ms = parse_iso_to_epoch_ms(raw["ingest_time"])
    except TransformError:
        raise

    reps = _coerce_int(payload.get("reps"), "reps")
    weight_kg = _coerce_float(payload.get("weight_kg"), "weight_kg")
    rpe = _coerce_optional_float(payload.get("rpe"), "rpe")
    rir = _coerce_optional_float(payload.get("rir"), "rir")

    session_load = compute_strength_session_load(reps, weight_kg, rpe)

    return {
        "event_id": raw["event_id"],
        "event_time": event_time_ms,
        "ingest_time": ingest_time_ms,
        "source": raw["source"],
        "schema_version": int(schema_version),
        "athlete_id": raw["athlete_id"],
        "event_type": STRENGTH_SET,
        "workout_id": payload.get("workout_id"),
        "exercise_id": payload.get("exercise_id"),
        "set_number": _coerce_int(payload.get("set_number"), "set_number")
        if payload.get("set_number") is not None
        else None,
        "reps": reps,
        "weight_kg": weight_kg,
        "rpe": rpe,
        "rir": rir,
        # strength-sourced events leave cardio fields null
        "activity_type": None,
        "distance_km": None,
        "duration_sec": None,
        "avg_hr": None,
        "tss": None,
        "session_load": session_load,
    }


# --- validation ------------------------------------------------------------


def validate_training_event(event: dict) -> None:
    """Validate a canonical TrainingEvent dict against the spec envelope.

    Catches:
      - missing required envelope/session_load fields -> ValidationError
      - session_load is NaN / inf -> ValidationError (spec DLQ scenario)

    The Avro schema itself enforces types at serialization; this is the
    in-ProcessFunction guard that lets the job route bad records to the DLQ
    side output BEFORE serializing (cheap, deterministic).
    """
    if not isinstance(event, dict):
        raise ValidationError("canonical event must be a dict")
    for field in _REQUIRED_ENVELOPE_FIELDS:
        if field not in event or event[field] is None:
            raise ValidationError(f"missing required canonical field: {field!r}")
    sl = event.get("session_load")
    if not isinstance(sl, (int, float)) or isinstance(sl, bool):
        raise ValidationError("session_load must be a number")
    if math.isnan(sl) or math.isinf(sl):
        raise ValidationError("session_load must not be NaN/Inf")


# --- DLQ error envelope ----------------------------------------------------


def _encode_original_value(value: Any) -> str:
    """Base64-encode the original_value bytes for the DLQ envelope.

    Accepts bytes/bytearray (as-is) or str (utf-8 encoded first), matching the
    spec DLQ envelope: ``original_value`` is base64-encoded-bytes.
    """
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        raw_bytes = bytes(value)
    elif isinstance(value, str):
        raw_bytes = value.encode("utf-8")
    else:
        # for anything else, JSON-encode then utf-8 (keeps it printable)
        import json

        raw_bytes = json.dumps(value).encode("utf-8")
    return base64.b64encode(raw_bytes).decode("ascii")


def build_dlq_envelope(
    *,
    original_topic: str,
    original_key: str | None,
    original_value: Any,
    error_type: str,
    error_message: str,
    timestamp: int,
    error_stack: str | None = None,
) -> dict:
    """Build the DLQ error envelope dict (spec "DLQ Error Envelope").

    DLQ messages are JSON (not Avro) because the original event may have an
    unparseable schema. The DLQ KafkaSink uses DeliveryGuarantee.AT_LEAST_ONCE
    (duplicates are tolerable diagnostics).
    """
    return {
        "original_topic": original_topic,
        "original_key": original_key,
        "original_value": _encode_original_value(original_value),
        "error_type": error_type,
        "error_message": error_message,
        "error_stack": error_stack,
        "timestamp": int(timestamp),
    }


# --- Avro helpers (fastavro, stand-in for Confluent Registry Avro serde) ----
# Pure roundtrip used by the unit tests to prove the canonical dict is
# type/default coherent with TrainingEvent.avsc. The Flink job uses
# ConfluentRegistryAvroSerializationSchema against the SAME .avsc via the
# Registry; fastavro is a faithful local stand-in for unit-level verification
# (no pyflink/Docker required).


_SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schemas" / "canonical"


def load_training_event_avsc() -> dict:
    """Load and parse schemas/canonical/TrainingEvent.avsc."""
    import json

    path = _SCHEMA_DIR / "TrainingEvent.avsc"
    return json.loads(path.read_text(encoding="utf-8"))


def serialize_training_event_avro(event: dict, schema: dict) -> bytes:
    """Serialize a canonical TrainingEvent dict to Avro bytes (fastavro).

    The Flink job uses ConfluentRegistryAvroSerializationSchema (wire format
    embeds a 5-byte schema-id header per the Confluent wire format); this helper
    emits bare Avro (no Confluent header) which is sufficient for unit-level
    field-type/default verification. The integration test exercises the real
    Registry serde.
    """
    from fastavro import schemaless_writer
    import io

    bio = io.BytesIO()
    schemaless_writer(bio, schema, event)
    return bio.getvalue()


def deserialize_training_event_avro(encoded: bytes, schema: dict) -> dict:
    """Deserialize Avro bytes to a canonical TrainingEvent dict (fastavro)."""
    from fastavro import schemaless_reader
    import io

    bio = io.BytesIO(encoded)
    return schemaless_reader(bio, schema)