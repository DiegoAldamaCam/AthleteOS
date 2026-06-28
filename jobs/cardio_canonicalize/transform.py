"""PURE canonicalization logic for the raw.cardio → canonical.training_event
transform (PR-C2, ADR-C1..ADR-C4).

This module is deliberately pyflink-free so unit tests run on interpreters where
apache-flink has no wheel (CPython 3.14) and without a Docker daemon. The Flink
job wiring (jobs/cardio_canonicalize/main.py) calls into these pure functions
from inside its ``KeyedProcessFunction`` and import-isolates pyflink.

Design decisions (design #205):
  ADR-C1 session_load formula — TWO-TIER (strictly):
    Tier 1: if tss is not None → session_load = float(tss)
    Tier 2: elif avg_hr is not None → (duration_sec/3600) * (avg_hr/220) * 100
    Tier 3: both None → raise ValidationError → DLQ
    There is NO duration_sec/60 third tier. session_load is REQUIRED non-null
    in TrainingEvent.avsc line 21; uncomputable rows must DLQ.

  ADR-C2 txn-id-prefix isolation:
    transactional_id_prefix = "athleteos-canonicalize-cardio-training-event"
    (distinct from strength "athleteos-canonicalize-training-event",
    wellness "athleteos-canonicalize-wellness-event",
    planning "athleteos-canonicalize-planning-block")

  ADR-C3 activity_type non-rejection:
    Free-form string, soft-normalize (strip). Never DLQ on unknown value.
    TrainingEvent.avsc activity_type is ["null","string"], not an enum.

  ADR-C4 transform reuse:
    Import helpers from jobs.canonicalize.transform (same as wellness does).
    Keeps DLQ routing consistent across all canonicalize jobs.

Reuses from jobs.canonicalize.transform (ADR-C4):
  - ValidationError, TransformError
  - parse_iso_to_epoch_ms, select_dlq_error_type
  - build_dlq_envelope, _encode_original_value
  - CARDIO_ACTIVITY (the constant is already defined there)
  - Avro helpers: load_training_event_avsc, serialize_training_event_avro,
    deserialize_training_event_avro
"""

from __future__ import annotations

from typing import Any

# Reuse shared exceptions, pure helpers, and constants from the strength
# canonicalize transform. Mirrors the wellness canonicalize pattern exactly
# (jobs/wellness_canonicalize/transform.py lines 36-45).
from jobs.canonicalize.transform import (  # noqa: F401  (re-exported for test imports)
    ValidationError,
    TransformError,
    parse_iso_to_epoch_ms,
    select_dlq_error_type,
    build_dlq_envelope,
    _encode_original_value,
    VALIDATION_FAILURE,
    TRANSFORM_ERROR,
    CARDIO_ACTIVITY,
    load_training_event_avsc,
    serialize_training_event_avro,
    deserialize_training_event_avro,
)


# ---------------------------------------------------------------------------
# Required raw envelope fields for cardio events
# ---------------------------------------------------------------------------

_REQUIRED_ENVELOPE_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "athlete_id",
)

_REQUIRED_PAYLOAD_FIELDS: tuple[str, ...] = (
    "activity_type",
    "duration_sec",
)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_cardio_event(raw: dict) -> None:
    """Validate a raw.cardio envelope against the spec required fields.

    Required envelope fields: event_id, event_time, athlete_id.
    Required payload fields: activity_type, duration_sec.
    Nullable payload fields: distance_km, avg_hr, tss.

    Raises:
      ValidationError: missing or null required field.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw cardio envelope must be a dict")

    for field in _REQUIRED_ENVELOPE_FIELDS:
        if field not in raw or raw[field] is None:
            raise ValidationError(
                f"missing required raw cardio envelope field: {field!r}"
            )

    payload = raw.get("payload")
    if not isinstance(payload, dict):
        raise ValidationError("raw cardio envelope 'payload' must be a dict")

    for field in _REQUIRED_PAYLOAD_FIELDS:
        if field not in payload or payload[field] is None:
            raise ValidationError(
                f"missing required cardio payload field: {field!r}"
            )


# ---------------------------------------------------------------------------
# session_load computation — TWO-TIER (ADR-C1)
# ---------------------------------------------------------------------------


def compute_cardio_session_load(
    tss: float | None,
    avg_hr: int | None,
    duration_sec: int,
) -> float:
    """Compute session_load for a CARDIO_ACTIVITY event (ADR-C1 two-tier).

    Tier 1 (TSS direct):
        if tss is not None → return float(tss)

    Tier 2 (HR-TRIMP proxy):
        elif avg_hr is not None → return (duration_sec / 3600) * (avg_hr / 220) * 100

    Tier 3 (DLQ):
        both tss and avg_hr are None → raise ValidationError

    ``duration_sec`` is always present when this function is reached
    (validate_cardio_event() checked it upstream). The result is a
    finite float ≥ 0 when a tier matches.

    Raises:
      ValidationError: both tss and avg_hr are None (session_load uncomputable).
    """
    if tss is not None:
        return float(tss)
    if avg_hr is not None:
        return (float(duration_sec) / 3600.0) * (float(avg_hr) / 220.0) * 100.0
    raise ValidationError(
        "session_load uncomputable: tss and avg_hr are both None — route to DLQ"
    )


# ---------------------------------------------------------------------------
# Core transform: raw.cardio envelope → canonical TrainingEvent dict
# ---------------------------------------------------------------------------


def transform_cardio_to_canonical(raw: dict, schema_version: int) -> dict:
    """Map a raw.cardio envelope (JSON) to a canonical TrainingEvent dict
    (Avro-ready, event_type=CARDIO_ACTIVITY per ADR-C3).

    Field mapping:
      raw.event_id         → event_id         (direct)
      raw.event_time       → event_time        (epoch-ms long; cardio producer
                                               emits epoch-ms, not ISO)
      raw.ingest_time      → ingest_time       (epoch-ms long)
      raw.source           → source
      (job-supplied)       → schema_version
      raw.athlete_id       → athlete_id
      (constant)           → event_type = CARDIO_ACTIVITY
      payload.activity_type → activity_type   (soft-normalized: stripped)
      payload.duration_sec → duration_sec
      payload.distance_km  → distance_km      (nullable float)
      payload.avg_hr       → avg_hr           (nullable int)
      payload.tss          → tss              (nullable float)
      (computed)           → session_load     (two-tier, REQUIRED non-null)
      (n/a for cardio)     → workout_id/exercise_id/set_number/reps/
                             weight_kg/rpe/rir = None

    Raises:
      ValidationError: missing required field or session_load uncomputable.
      TransformError:  unexpected mapping/coercion failure.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw cardio envelope must be a dict")

    # Required envelope fields
    for field in ("event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"):
        if field not in raw or raw[field] is None:
            raise ValidationError(
                f"missing required raw cardio envelope field: {field!r}"
            )

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValidationError("raw cardio envelope 'payload' must be a dict")

    # Required payload fields
    for field in ("activity_type", "duration_sec"):
        if field not in payload or payload[field] is None:
            raise ValidationError(
                f"missing required cardio payload field: {field!r}"
            )

    # event_time / ingest_time: cardio producer emits epoch-ms longs directly
    # (same as wellness, W1-5 compliant). Accept both int and str (ISO) defensively.
    event_time_ms = _to_epoch_ms(raw["event_time"], "event_time")
    ingest_time_ms = _to_epoch_ms(raw["ingest_time"], "ingest_time")

    # activity_type: soft-normalize (strip whitespace). Never reject. (ADR-C3)
    activity_type_raw = payload["activity_type"]
    if isinstance(activity_type_raw, str):
        activity_type = activity_type_raw.strip()
    else:
        raise ValidationError(
            f"activity_type must be a string, got {activity_type_raw!r}"
        )

    duration_sec = int(payload["duration_sec"])

    # Nullable cardio fields
    distance_km = _opt_float(payload.get("distance_km"))
    avg_hr = _opt_int(payload.get("avg_hr"))
    tss = _opt_float(payload.get("tss"))

    # session_load: two-tier (ADR-C1) — raises ValidationError → DLQ on Tier 3
    session_load = compute_cardio_session_load(tss, avg_hr, duration_sec)

    return {
        # Common event envelope
        "event_id": raw["event_id"],
        "event_time": event_time_ms,
        "ingest_time": ingest_time_ms,
        "source": raw["source"],
        "schema_version": int(schema_version),
        "athlete_id": raw["athlete_id"],
        "event_type": CARDIO_ACTIVITY,
        # Strength-only fields — null for cardio events
        "workout_id": None,
        "exercise_id": None,
        "set_number": None,
        "reps": None,
        "weight_kg": None,
        "rpe": None,
        "rir": None,
        # Cardio-specific fields
        "activity_type": activity_type,
        "distance_km": distance_km,
        "duration_sec": duration_sec,
        "avg_hr": avg_hr,
        "tss": tss,
        # Computed REQUIRED field (non-nullable in TrainingEvent.avsc line 21)
        "session_load": session_load,
    }


# ---------------------------------------------------------------------------
# Private coercion helpers (mirrors wellness_canonicalize/transform.py)
# ---------------------------------------------------------------------------


def _to_epoch_ms(value: Any, field: str) -> int:
    """Convert an event_time / ingest_time value to epoch-ms int.

    The cardio producer emits epoch-ms longs directly (W1-5 compliant).
    We accept both int (primary path) and str (ISO-8601 fallback, defensive).
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        return parse_iso_to_epoch_ms(value)
    raise TransformError(
        f"{field!r} must be epoch-ms int or ISO string, got {value!r}"
    )


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
