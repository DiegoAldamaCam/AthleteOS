"""PURE canonicalization logic for the raw.nutrition → canonical.wellness_event
transform (PR-N2, ADR-N1..ADR-N4).

This module is deliberately pyflink-free so unit tests run on interpreters where
apache-flink has no wheel (CPython 3.14) and without a Docker daemon. The Flink
job wiring (jobs/nutrition_canonicalize/main.py) calls into these pure functions
from inside its ``KeyedProcessFunction`` and import-isolates pyflink.

Design decisions (design #241):
  ADR-N1 event_type source:
    HARDCODE ``"NUTRITION_DAILY"`` constant inside ``transform_nutrition_to_canonical``.
    Nutrition is ALWAYS daily; hardcoding decouples the source format from the
    canonical model and removes a payload field + a validation branch. The producer
    omits event_type from the payload entirely.

  ADR-N2 rename:
    ``payload["adherence_score"]`` → canonical ``nutrition_adherence`` in transform ONLY.
    Raw layer (parser/producer) keeps the source-faithful name ``adherence_score``
    (sc-8 guard). The rename happens here and only here (sc-16).

  ADR-N3 txn-id-prefix isolation:
    transactional_id_prefix = "athleteos-canonicalize-nutrition-wellness-event"
    (distinct from wellness "athleteos-canonicalize-wellness-event",
    strength "athleteos-canonicalize-training-event",
    planning "athleteos-canonicalize-planning-block",
    cardio "athleteos-canonicalize-cardio-training-event",
    recovery "athleteos-canonicalize-recovery-wellness-event")
    Constant is unit-testable (sc-25).

  ADR-N4 integration test convention:
    ``importorskip("testcontainers")`` + shared ``redpanda_endpoints`` fixture
    (conftest.py:69) which uses ``from testcontainers.kafka import RedpandaContainer``.
    NO local fixture. NO ``testcontainers.redpanda`` import (obs #214 cardio CI fail).

Reuses from jobs.wellness_canonicalize.transform (ADR-N1):
  - ValidationError, TransformError
  - build_dlq_envelope, select_dlq_error_type, parse_iso_to_epoch_ms
  - Avro helpers: load_wellness_event_avsc, serialize_wellness_event_avro,
    deserialize_wellness_event_avro
  - NUTRITION_DAILY constant (already defined at wellness_canonicalize/transform.py:53)
"""

from __future__ import annotations

from typing import Any

# Reuse shared exceptions, pure helpers, and the NUTRITION_DAILY constant
# from jobs.wellness_canonicalize.transform. NUTRITION_DAILY is already
# defined at line 53 of that module and is in ALLOWED_WELLNESS_TYPES.
from jobs.wellness_canonicalize.transform import (  # noqa: F401  (re-exported for test imports)
    ValidationError,
    TransformError,
    parse_iso_to_epoch_ms,
    select_dlq_error_type,
    build_dlq_envelope,
    _encode_original_value,
    VALIDATION_FAILURE,
    TRANSFORM_ERROR,
    NUTRITION_DAILY,
    load_wellness_event_avsc,
    serialize_wellness_event_avro,
    deserialize_wellness_event_avro,
)


# ---------------------------------------------------------------------------
# Required raw envelope fields for nutrition events
# ---------------------------------------------------------------------------

# Nutrition has two required envelope fields: athlete_id and event_time.
# All payload fields (calories, protein_g, carbs_g, fat_g, adherence_score)
# are nullable — null-row policy: downstream W3-5 guard handles no-op metric update.
_REQUIRED_ENVELOPE_FIELDS: tuple[str, ...] = (
    "athlete_id",
    "event_time",
)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_nutrition_event(raw: dict) -> None:
    """Validate a raw.nutrition envelope against the spec required fields.

    Required fields: athlete_id, event_time.
    Nullable payload fields: calories, protein_g, carbs_g, fat_g, adherence_score.

    Raises:
      ValidationError: athlete_id or event_time is missing or null.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw nutrition envelope must be a dict")

    for field in _REQUIRED_ENVELOPE_FIELDS:
        if field not in raw or raw[field] is None:
            raise ValidationError(
                f"missing required raw nutrition envelope field: {field!r}"
            )


# ---------------------------------------------------------------------------
# Core transform: raw.nutrition envelope → canonical WellnessEvent dict
# ---------------------------------------------------------------------------


def transform_nutrition_to_canonical(raw: dict, schema_version: int) -> dict:
    """Map a raw.nutrition envelope (JSON) to a canonical WellnessEvent dict
    (Avro-ready, event_type=NUTRITION_DAILY hardcoded per ADR-N1).

    Field mapping:
      raw.event_id               → event_id         (direct)
      raw.event_time             → event_time        (epoch-ms long; nutrition producer
                                                     emits epoch-ms, same as wellness W1-5)
      raw.ingest_time            → ingest_time       (epoch-ms long)
      raw.source                 → source
      (job-supplied)             → schema_version
      raw.athlete_id             → athlete_id
      (constant)                 → event_type = NUTRITION_DAILY  (ADR-N1 — hardcoded)
      payload.calories           → calories          (nullable int)
      payload.protein_g          → protein_g         (nullable float)
      payload.carbs_g            → carbs_g           (nullable float)
      payload.fat_g              → fat_g             (nullable float)
      payload.adherence_score    → nutrition_adherence  (ADR-N2 — RENAMED HERE ONLY)
      (n/a for nutrition)        → sleep_hours/resting_hr/hrv/steps/body_weight_kg/
                                   energy/soreness/mood/stress/perceived_recovery = None

    Raises:
      ValidationError: missing required field (athlete_id or event_time).
      TransformError:  unexpected mapping/coercion failure.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw nutrition envelope must be a dict")

    # Validate required envelope fields
    for field in ("event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"):
        if field not in raw or raw[field] is None:
            raise ValidationError(
                f"missing required raw nutrition envelope field: {field!r}"
            )

    payload = raw["payload"]
    if not isinstance(payload, dict):
        raise ValidationError("raw nutrition envelope 'payload' must be a dict")

    # event_time / ingest_time: nutrition producer emits epoch-ms longs (W1-5 compliant).
    # Accept both int and str (ISO) defensively.
    event_time_ms = _to_epoch_ms(raw["event_time"], "event_time")
    ingest_time_ms = _to_epoch_ms(raw["ingest_time"], "ingest_time")

    # Nutrition nullable payload fields
    calories = _opt_int(payload.get("calories"))
    protein_g = _opt_float(payload.get("protein_g"))
    carbs_g = _opt_float(payload.get("carbs_g"))
    fat_g = _opt_float(payload.get("fat_g"))
    # ADR-N2: source key is 'adherence_score' (source-faithful in parser/producer);
    # renamed to 'nutrition_adherence' HERE in the transform (and ONLY here).
    nutrition_adherence = _opt_float(payload.get("adherence_score"))

    return {
        # Common event envelope
        "event_id": raw["event_id"],
        "event_time": event_time_ms,
        "ingest_time": ingest_time_ms,
        "source": raw["source"],
        "schema_version": int(schema_version),
        "athlete_id": raw["athlete_id"],
        # event_type HARDCODED (ADR-N1) — nutrition is ALWAYS daily;
        # the producer omits event_type from the payload.
        "event_type": NUTRITION_DAILY,
        # Apple Health recovery fields — not applicable for nutrition events
        "sleep_hours": None,
        "resting_hr": None,
        "hrv": None,
        "steps": None,
        "body_weight_kg": None,
        # Nutrition fields (nullable). Note: adherence_score (raw) → nutrition_adherence (canonical).
        "calories": calories,
        "protein_g": protein_g,
        "carbs_g": carbs_g,
        "fat_g": fat_g,
        "nutrition_adherence": nutrition_adherence,  # ADR-N2 rename
        # Subjective wellness fields — not applicable for nutrition events
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

    The nutrition producer emits epoch-ms longs directly (W1-5 compliant).
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
