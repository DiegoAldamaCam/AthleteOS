"""PURE canonicalization logic for the raw.recovery → canonical.wellness_event
transform (PR-R2, ADR-R1..ADR-R4).

This module is deliberately pyflink-free so unit tests run on interpreters where
apache-flink has no wheel (CPython 3.14) and without a Docker daemon. The Flink
job wiring (jobs/recovery_canonicalize/main.py) calls into these pure functions
from inside its ``KeyedProcessFunction`` and import-isolates pyflink.

Design decisions (design #225):
  ADR-R1 event_type source:
    HARDCODE ``"RECOVERY_SNAPSHOT"`` constant inside ``transform_recovery_to_canonical``.
    Recovery is ALWAYS a snapshot; hardcoding decouples the source format from the
    canonical model and removes a payload field + a validation branch. The producer
    omits event_type from the payload entirely.

  ADR-R2 txn-id-prefix isolation:
    transactional_id_prefix = "athleteos-canonicalize-recovery-wellness-event"
    (distinct from wellness "athleteos-canonicalize-wellness-event",
    strength "athleteos-canonicalize-training-event",
    planning "athleteos-canonicalize-planning-block",
    cardio "athleteos-canonicalize-cardio-training-event")
    Constant is unit-testable (sc-23).

  ADR-R3 same-date collision: last-writer-wins (decision #222).
    Idempotent PG UPSERT on (athlete_id, metric_date). No precedence filter.

  ADR-R4 integration test convention:
    ``importorskip("testcontainers")`` + shared ``redpanda_endpoints`` fixture
    (conftest.py:69) which uses ``from testcontainers.kafka import RedpandaContainer``.
    NO local fixture. NO ``testcontainers.redpanda`` import (obs #214 cardio CI fail).

Reuses from jobs.wellness_canonicalize.transform (ADR-R1):
  - ValidationError, TransformError
  - build_dlq_envelope, select_dlq_error_type, parse_iso_to_epoch_ms
  - Avro helpers: load_wellness_event_avsc, serialize_wellness_event_avro,
    deserialize_wellness_event_avro
  - RECOVERY_SNAPSHOT constant (already defined at wellness_canonicalize/transform.py:52)
"""

from __future__ import annotations

from typing import Any

# Reuse shared exceptions, pure helpers, and the RECOVERY_SNAPSHOT constant
# from jobs.wellness_canonicalize.transform. RECOVERY_SNAPSHOT is already
# defined at line 52 of that module and is in ALLOWED_WELLNESS_TYPES.
from jobs.wellness_canonicalize.transform import (  # noqa: F401  (re-exported for test imports)
    ValidationError,
    TransformError,
    parse_iso_to_epoch_ms,
    select_dlq_error_type,
    build_dlq_envelope,
    _encode_original_value,
    VALIDATION_FAILURE,
    TRANSFORM_ERROR,
    RECOVERY_SNAPSHOT,
    load_wellness_event_avsc,
    serialize_wellness_event_avro,
    deserialize_wellness_event_avro,
)


# ---------------------------------------------------------------------------
# Required raw envelope fields for recovery events
# ---------------------------------------------------------------------------

# Recovery has two required envelope fields: athlete_id and event_time.
# All payload fields (sleep_hours, resting_hr, hrv, steps, body_weight_kg)
# are nullable — null-row policy: downstream W3-5 guard handles no-op metric update.
_REQUIRED_ENVELOPE_FIELDS: tuple[str, ...] = (
    "athlete_id",
    "event_time",
)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_recovery_event(raw: dict) -> None:
    """Validate a raw.recovery envelope against the spec required fields.

    Required fields: athlete_id, event_time.
    Nullable payload fields: sleep_hours, resting_hr, hrv, steps, body_weight_kg.

    Raises:
      ValidationError: athlete_id or event_time is missing or null.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw recovery envelope must be a dict")

    for field in _REQUIRED_ENVELOPE_FIELDS:
        if field not in raw or raw[field] is None:
            raise ValidationError(
                f"missing required raw recovery envelope field: {field!r}"
            )


# ---------------------------------------------------------------------------
# Core transform: raw.recovery envelope → canonical WellnessEvent dict
# ---------------------------------------------------------------------------


def transform_recovery_to_canonical(raw: dict, schema_version: int) -> dict:
    """Map a raw.recovery envelope (JSON) to a canonical WellnessEvent dict
    (Avro-ready, event_type=RECOVERY_SNAPSHOT hardcoded per ADR-R1).

    Field mapping:
      raw.event_id         → event_id         (direct)
      raw.event_time       → event_time        (epoch-ms long; recovery producer
                                               emits epoch-ms, same as wellness W1-5)
      raw.ingest_time      → ingest_time       (epoch-ms long)
      raw.source           → source
      (job-supplied)       → schema_version
      raw.athlete_id       → athlete_id
      (constant)           → event_type = RECOVERY_SNAPSHOT  (ADR-R1 — hardcoded)
      payload.sleep_hours  → sleep_hours       (nullable float)
      payload.resting_hr   → resting_hr        (nullable int)
      payload.hrv          → hrv               (nullable float)
      payload.steps        → steps             (nullable int)
      payload.body_weight_kg → body_weight_kg  (nullable float)
      (n/a for recovery)   → calories/protein_g/carbs_g/fat_g/nutrition_adherence/
                             energy/soreness/mood/stress/perceived_recovery = None

    Raises:
      ValidationError: missing required field (athlete_id or event_time).
      TransformError:  unexpected mapping/coercion failure.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw recovery envelope must be a dict")

    # Validate required envelope fields
    for field in ("event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"):
        if field not in raw or raw[field] is None:
            raise ValidationError(
                f"missing required raw recovery envelope field: {field!r}"
            )

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValidationError("raw recovery envelope 'payload' must be a dict")

    # event_time / ingest_time: recovery producer emits epoch-ms longs (W1-5 compliant).
    # Accept both int and str (ISO) defensively.
    event_time_ms = _to_epoch_ms(raw["event_time"], "event_time")
    ingest_time_ms = _to_epoch_ms(raw["ingest_time"], "ingest_time")

    # Apple Health nullable payload fields
    sleep_hours = _opt_float(payload.get("sleep_hours"))
    resting_hr = _opt_int(payload.get("resting_hr"))
    hrv = _opt_float(payload.get("hrv"))
    steps = _opt_int(payload.get("steps"))
    body_weight_kg = _opt_float(payload.get("body_weight_kg"))

    return {
        # Common event envelope
        "event_id": raw["event_id"],
        "event_time": event_time_ms,
        "ingest_time": ingest_time_ms,
        "source": raw["source"],
        "schema_version": int(schema_version),
        "athlete_id": raw["athlete_id"],
        # event_type HARDCODED (ADR-R1) — recovery is ALWAYS a snapshot;
        # the producer omits event_type from the payload.
        "event_type": RECOVERY_SNAPSHOT,
        # Apple Health recovery fields (nullable)
        "sleep_hours": sleep_hours,
        "resting_hr": resting_hr,
        "hrv": hrv,
        "steps": steps,
        "body_weight_kg": body_weight_kg,
        # Nutrition fields — not applicable for recovery events
        "calories": None,
        "protein_g": None,
        "carbs_g": None,
        "fat_g": None,
        "nutrition_adherence": None,
        # Subjective wellness fields — not applicable for recovery events
        "energy": None,
        "soreness": None,
        "mood": None,
        "stress": None,
        "perceived_recovery": None,
    }


# ---------------------------------------------------------------------------
# Private coercion helpers (mirrors wellness_canonicalize/transform.py)
# ---------------------------------------------------------------------------


def _to_epoch_ms(value: Any, field: str) -> int:
    """Convert an event_time / ingest_time value to epoch-ms int.

    The recovery producer emits epoch-ms longs directly (W1-5 compliant).
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
