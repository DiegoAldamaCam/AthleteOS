"""PURE canonicalization logic for the raw.planning → canonical.planning_block
transform (PR-PL2a).

This module is deliberately pyflink-free so unit tests run on interpreters where
apache-flink has no wheel (CPython 3.14) and without a Docker daemon. The Flink
job wiring (jobs/planning_canonicalize/main.py — PR-PL2b) calls into these pure
functions from inside its KeyedProcessFunction and import-isolates pyflink.

ADR-20: Block identity = VERSIONING, not dedup-by-key.
  PG PK = (athlete_id, block_id, ingest_time). A repeat block_id for the same
  athlete with a new ingest_time is a NEW revision, kept — never dropped.
  event_id dedup remains (7d ValueState TTL) for idempotent reprocessing.
  This module does NOT implement dedup state (pyflink concern, PR-PL2b).

ADR-21: planning UPSERT is effectively an INSERT (no overwrite).
  INSERT INTO planning_blocks ... ON CONFLICT (athlete_id, block_id, ingest_time)
  DO NOTHING. Because the conflict target is the full versioning PK, a genuine
  new revision never conflicts; an exact replay (same ingest_time) → DO NOTHING.

Reuses from jobs.canonicalize.transform (imported directly — no copy):
  - ValidationError, TransformError
  - parse_iso_to_epoch_ms
  - select_dlq_error_type
  - build_dlq_envelope, _encode_original_value
  - VALIDATION_FAILURE, TRANSFORM_ERROR

Field mapping (spec PL2-4):
  raw envelope                     → canonical PlanningBlock.avsc field
  event_id                         → event_id            (direct)
  event_time  (epoch-ms int)       → event_time          (epoch-ms long, direct)
  ingest_time (epoch-ms int)       → ingest_time         (epoch-ms long, direct)
  source                           → source
  (job-supplied)                   → schema_version
  athlete_id                       → athlete_id
  block_id                         → block_id
  goal                             → goal
  start_date  (ISO date str)       → start_date          (epoch-ms long via parse_iso)
  end_date    (ISO date str)       → end_date            (epoch-ms long via parse_iso)
  planned_sessions_per_week (int)  → planned_sessions_per_week (int)
  weekly_volume_targets  (JSON str)→ weekly_volume_targets (JSON string)
"""

from __future__ import annotations

import io
import json
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
# Required canonical PlanningBlock fields (Avro schema — all 12)
# ---------------------------------------------------------------------------

_REQUIRED_CANONICAL_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_time",
    "ingest_time",
    "source",
    "schema_version",
    "athlete_id",
    "block_id",
    "goal",
    "start_date",
    "end_date",
    "planned_sessions_per_week",
    "weekly_volume_targets",
)


# ---------------------------------------------------------------------------
# Core transform: raw.planning envelope → canonical PlanningBlock dict
# ---------------------------------------------------------------------------


def transform_planning_to_canonical(raw: dict, schema_version: int) -> dict:
    """Map a raw.planning envelope (JSON) to a canonical PlanningBlock dict
    (Avro-ready, all 12 fields per PlanningBlock.avsc).

    Field rules (spec PL2-4):
      - start_date / end_date: ISO date string → epoch-ms long (UTC midnight)
        via parse_iso_to_epoch_ms.
      - weekly_volume_targets: must be a valid JSON string; round-trips via
        json.loads → json.dumps so the canonical form is always compact JSON.
      - planned_sessions_per_week: coerced to int.
      - event_time / ingest_time: already epoch-ms ints from the producer;
        passed through directly.
      - schema_version: injected by the caller (job-supplied).

    Raises:
      ValidationError: missing required field.
      TransformError:  unexpected mapping/coercion failure.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw envelope must be a dict")

    for field in (
        "event_id", "event_time", "ingest_time", "source",
        "athlete_id", "block_id", "goal",
        "start_date", "end_date",
        "planned_sessions_per_week", "weekly_volume_targets",
    ):
        if field not in raw or raw[field] is None:
            raise ValidationError(f"missing required raw envelope field: {field!r}")

    # ISO date → epoch-ms long (UTC midnight)
    try:
        start_date_ms = parse_iso_to_epoch_ms(raw["start_date"])
        end_date_ms = parse_iso_to_epoch_ms(raw["end_date"])
    except TransformError:
        raise

    # weekly_volume_targets: must be a valid JSON string
    try:
        wvt_parsed = json.loads(raw["weekly_volume_targets"])
        wvt_str = json.dumps(wvt_parsed)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise TransformError(
            f"weekly_volume_targets is not valid JSON: {raw['weekly_volume_targets']!r}"
        ) from exc

    # planned_sessions_per_week: coerce to int
    try:
        sessions = int(raw["planned_sessions_per_week"])
    except (TypeError, ValueError) as exc:
        raise TransformError(
            f"planned_sessions_per_week not coercible to int: "
            f"{raw['planned_sessions_per_week']!r}"
        ) from exc

    return {
        "event_id": str(raw["event_id"]),
        "event_time": int(raw["event_time"]),
        "ingest_time": int(raw["ingest_time"]),
        "source": str(raw["source"]),
        "schema_version": int(schema_version),
        "athlete_id": str(raw["athlete_id"]),
        "block_id": str(raw["block_id"]),
        "goal": str(raw["goal"]),
        "start_date": start_date_ms,
        "end_date": end_date_ms,
        "planned_sessions_per_week": sessions,
        "weekly_volume_targets": wvt_str,
    }


# ---------------------------------------------------------------------------
# Validation: planning-specific business rules (spec PL2-6, PL2-7, PL2-8)
# ---------------------------------------------------------------------------


def validate_planning_block(raw: dict) -> None:
    """Validate a raw.planning envelope against planning-specific business rules.

    Called BEFORE transform in the Flink ProcessFunction (PR-PL2b main.py) so
    invalid records are routed to the DLQ without attempting the transform.

    Validates (spec PL2-6, PL2-7, PL2-8):
      - end_date < start_date → ValidationError (PL2-6)
      - planned_sessions_per_week <= 0 → ValidationError (PL2-7)
      - weekly_volume_targets not valid JSON → ValidationError (PL2-8)

    Raises:
      ValidationError: any of the above conditions is true.
    """
    if not isinstance(raw, dict):
        raise ValidationError("raw envelope must be a dict")

    # PL2-8: weekly_volume_targets must be a valid JSON string
    wvt = raw.get("weekly_volume_targets")
    if wvt is not None:
        try:
            json.loads(wvt)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValidationError(
                f"weekly_volume_targets is not valid JSON: {wvt!r}"
            ) from exc

    # PL2-6: end_date < start_date
    start_str = raw.get("start_date")
    end_str = raw.get("end_date")
    if start_str is not None and end_str is not None:
        if end_str < start_str:  # ISO date strings compare lexicographically
            raise ValidationError(
                f"end_date ({end_str!r}) must not be before start_date ({start_str!r})"
            )

    # PL2-7: planned_sessions_per_week <= 0
    sessions = raw.get("planned_sessions_per_week")
    if sessions is not None:
        try:
            sessions_int = int(sessions)
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                f"planned_sessions_per_week not coercible to int: {sessions!r}"
            ) from exc
        if sessions_int <= 0:
            raise ValidationError(
                f"planned_sessions_per_week must be > 0, got {sessions_int!r}"
            )


# ---------------------------------------------------------------------------
# Avro helpers (fastavro, stand-in for Confluent Registry Avro serde)
# Pure roundtrip used by unit tests to verify the canonical dict is
# type/default coherent with PlanningBlock.avsc.
# ---------------------------------------------------------------------------

_SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schemas" / "canonical"


def load_planning_block_avsc() -> dict:
    """Load and parse schemas/canonical/PlanningBlock.avsc."""
    path = _SCHEMA_DIR / "PlanningBlock.avsc"
    return json.loads(path.read_text(encoding="utf-8"))


def serialize_planning_block_avro(event: dict, schema: dict) -> bytes:
    """Serialize a canonical PlanningBlock dict to Avro bytes (fastavro).

    Uses schemaless_writer (no Confluent 5-byte header) — sufficient for
    unit-level field-type and default verification. The Flink job writes
    canonical PlanningBlocks via a Table sink with value.format=avro-confluent.
    """
    from fastavro import schemaless_writer

    bio = io.BytesIO()
    schemaless_writer(bio, schema, event)
    return bio.getvalue()


def deserialize_planning_block_avro(encoded: bytes, schema: dict) -> dict:
    """Deserialize Avro bytes to a canonical PlanningBlock dict (fastavro)."""
    from fastavro import schemaless_reader

    bio = io.BytesIO(encoded)
    return schemaless_reader(bio, schema)
