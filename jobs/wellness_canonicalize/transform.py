"""PURE canonicalization logic for the raw.wellness → canonical.wellness_event
transform (PR-W2, ADR-16).

This module is deliberately pyflink-free so unit tests run on interpreters where
apache-flink has no wheel (CPython 3.14) and without a Docker daemon. The Flink
job wiring (jobs/wellness_canonicalize/main.py) calls into these pure functions
from inside its ``KeyedProcessFunction`` and import-isolates pyflink.

ADR-16: WellnessEvent.avsc event_type ENUM → STRING
  The Avro wire type is ``string`` (NOT the former enum). The semantic guarantee
  of the symbol set is enforced at the application layer by
  ``validate_wellness_event()`` (ALLOWED_WELLNESS_TYPES). Off-symbol values are
  routed to the DLQ as VALIDATION_FAILURE via select_dlq_error_type, mirroring
  the ADR-15 pattern in jobs/canonicalize/transform.py.

Reuses from jobs.canonicalize.transform (imported directly):
  - ValidationError, TransformError
  - parse_iso_to_epoch_ms, select_dlq_error_type
  - build_dlq_envelope, _encode_original_value
  - serialize/deserialize helpers (fastavro)
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

# Reuse shared exceptions and pure helpers from the strength canonicalize transform.
# This avoids duplicating tested logic and keeps the DLQ routing consistent.
from jobs.canonicalize.transform import (  # noqa: F401  (re-exported for test imports)
    ValidationError,
    TransformError,
    parse_iso_to_epoch_ms,
    select_dlq_error_type,
    build_dlq_envelope,
    _encode_original_value,
    VALIDATION_FAILURE,
    TRANSFORM_ERROR,
)


# ---------------------------------------------------------------------------
# Wellness event_type symbol set (ADR-16)
# ---------------------------------------------------------------------------

RECOVERY_SNAPSHOT = "RECOVERY_SNAPSHOT"
NUTRITION_DAILY = "NUTRITION_DAILY"
WELLNESS_DAILY = "WELLNESS_DAILY"

# Canonical event_type is an Avro STRING (NOT an enum) on the wire — ADR-16.
# The semantic guarantee of the former Avro enum is preserved at the application
# layer: validate_wellness_event() rejects any event_type NOT in this set,
# routing the offending record to the DLQ as VALIDATION_FAILURE.
ALLOWED_WELLNESS_TYPES: frozenset[str] = frozenset({
    RECOVERY_SNAPSHOT,
    NUTRITION_DAILY,
    WELLNESS_DAILY,
})

# Required canonical WellnessEvent envelope fields.
_REQUIRED_ENVELOPE_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "ingest_time",
    "source",
    "schema_version",
    "athlete_id",
    "event_type",
)


# ---------------------------------------------------------------------------
# Core transform: raw.wellness envelope → canonical WellnessEvent dict
# ---------------------------------------------------------------------------


def transform_wellness_to_canonical(raw: dict, schema_version: int) -> dict:
    """Map a raw.wellness envelope (JSON) to a canonical WellnessEvent dict
    (Avro-ready, event_type as STRING per ADR-16).

    Field mapping:
      raw.event_id         → event_id         (direct)
      raw.event_time       → event_time        (epoch-ms long — wellness producer
                                                already emits epoch-ms, not ISO)
      raw.ingest_time      → ingest_time       (epoch-ms long)
      raw.source           → source
      (job-supplied)       → schema_version
      raw.athlete_id       → athlete_id
      raw.payload.event_type → event_type      (validated at app layer)
      raw.payload.*        → per-field nullable mapping

    Inapplicable nutrition fields (calories, protein_g, carbs_g, fat_g,
    nutrition_adherence) are mapped as None for WELLNESS_DAILY /
    RECOVERY_SNAPSHOT event types. For NUTRITION_DAILY they are passed through.

    Raises:
      ValidationError: missing required envelope/payload field.
      TransformError:  unexpected mapping/coercion failure.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw envelope must be a dict")

    # Required envelope fields (payload handled below)
    for field in ("event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"):
        if field not in raw or raw[field] is None:
            raise ValidationError(f"missing required raw envelope field: {field!r}")

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValidationError("raw envelope 'payload' must be a dict")

    # event_time / ingest_time: wellness producer emits epoch-ms longs directly
    # (not ISO strings — intentional divergence from strength, per spec W1-5 /
    # design #133). Accept both int and str (ISO) defensively.
    event_time_ms = _to_epoch_ms(raw["event_time"], "event_time")
    ingest_time_ms = _to_epoch_ms(raw["ingest_time"], "ingest_time")

    # event_type comes from the payload (per the raw envelope shape from the
    # wellness producer). The symbol set is validated later in
    # validate_wellness_event(); here we just extract the string value.
    event_type = payload.get("event_type")
    if not isinstance(event_type, str) or event_type == "":
        raise ValidationError(
            f"missing or empty event_type in payload; got {event_type!r}"
        )

    return {
        "event_id": raw["event_id"],
        "event_time": event_time_ms,
        "ingest_time": ingest_time_ms,
        "source": raw["source"],
        "schema_version": int(schema_version),
        "athlete_id": raw["athlete_id"],
        "event_type": event_type,  # Avro STRING per ADR-16
        # Wellness signal fields (nullable)
        "sleep_hours": _opt_float(payload.get("sleep_hours")),
        "resting_hr": _opt_int(payload.get("resting_hr")),
        "hrv": _opt_float(payload.get("hrv")),
        "steps": _opt_int(payload.get("steps")),
        "body_weight_kg": _opt_float(payload.get("body_weight_kg")),
        # Nutrition fields: present only for NUTRITION_DAILY; None otherwise.
        # The caller's raw envelope already carries None for inapplicable fields.
        "calories": _opt_int(payload.get("calories")),
        "protein_g": _opt_float(payload.get("protein_g")),
        "carbs_g": _opt_float(payload.get("carbs_g")),
        "fat_g": _opt_float(payload.get("fat_g")),
        "nutrition_adherence": _opt_float(payload.get("nutrition_adherence")),
        # Subjective wellness fields (nullable)
        "energy": _opt_int(payload.get("energy")),
        "soreness": _opt_int(payload.get("soreness")),
        "mood": _opt_int(payload.get("mood")),
        "stress": _opt_int(payload.get("stress")),
        "perceived_recovery": _opt_int(payload.get("perceived_recovery")),
    }


# ---------------------------------------------------------------------------
# Validation (app-layer symbol guard — ADR-16)
# ---------------------------------------------------------------------------


def validate_wellness_event(event: dict) -> None:
    """Validate a canonical WellnessEvent dict against the spec envelope.

    Catches:
      - missing required envelope fields → ValidationError
      - event_type not in ALLOWED_WELLNESS_TYPES → ValidationError
        (the wire type is Avro STRING per ADR-16; the former enum's semantic
        guarantee is enforced here at the application layer, routing off-symbol
        values to the DLQ as VALIDATION_FAILURE via select_dlq_error_type)

    The Avro schema itself enforces field types at serialization; this is the
    in-ProcessFunction guard that lets the job route bad records to the DLQ
    side output BEFORE serializing.
    """
    if not isinstance(event, dict):
        raise ValidationError("canonical event must be a dict")
    for field in _REQUIRED_ENVELOPE_FIELDS:
        if field not in event or event[field] is None:
            raise ValidationError(f"missing required canonical field: {field!r}")
    event_type = event.get("event_type")
    if not isinstance(event_type, str) or event_type == "":
        raise ValidationError(
            f"event_type must be a non-empty string in "
            f"{sorted(ALLOWED_WELLNESS_TYPES)!r}, got {event_type!r}"
        )
    if event_type not in ALLOWED_WELLNESS_TYPES:
        raise ValidationError(
            f"event_type {event_type!r} not in allowed set "
            f"{sorted(ALLOWED_WELLNESS_TYPES)!r} -- route to DLQ as "
            f"VALIDATION_FAILURE (ADR-16: symbol set enforced at application "
            f"layer since the Avro wire type is STRING, not enum)"
        )


# ---------------------------------------------------------------------------
# Private coercion helpers
# ---------------------------------------------------------------------------


def _to_epoch_ms(value: Any, field: str) -> int:
    """Convert an event_time / ingest_time value to epoch-ms int.

    The wellness producer emits epoch-ms longs directly (design ADR / W1-5).
    We accept both int (primary path) and str (ISO-8601 fallback, defensive).
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        # Defensive fallback: parse ISO-8601 string to epoch-ms
        return parse_iso_to_epoch_ms(value)
    raise TransformError(f"{field!r} must be epoch-ms int or ISO string, got {value!r}")


def _opt_float(value: Any) -> float | None:
    """Coerce value to float or return None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TransformError(f"cannot coerce {value!r} to float") from exc


def _opt_int(value: Any) -> int | None:
    """Coerce value to int or return None."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise TransformError(f"expected int, got bool: {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TransformError(f"cannot coerce {value!r} to int") from exc


# ---------------------------------------------------------------------------
# Avro helpers (fastavro, stand-in for Confluent Registry Avro serde)
# Pure roundtrip used by unit tests to verify the canonical dict is
# type/default coherent with the migrated WellnessEvent.avsc.
# ---------------------------------------------------------------------------

_SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schemas" / "canonical"


def load_wellness_event_avsc() -> dict:
    """Load and parse schemas/canonical/WellnessEvent.avsc."""
    import json

    path = _SCHEMA_DIR / "WellnessEvent.avsc"
    return json.loads(path.read_text(encoding="utf-8"))


def serialize_wellness_event_avro(event: dict, schema: dict) -> bytes:
    """Serialize a canonical WellnessEvent dict to Avro bytes (fastavro)."""
    from fastavro import schemaless_writer

    bio = io.BytesIO()
    schemaless_writer(bio, schema, event)
    return bio.getvalue()


def deserialize_wellness_event_avro(encoded: bytes, schema: dict) -> dict:
    """Deserialize Avro bytes to a canonical WellnessEvent dict (fastavro)."""
    from fastavro import schemaless_reader

    bio = io.BytesIO(encoded)
    return schemaless_reader(bio, schema)
