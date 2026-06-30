"""PURE metrics-computation logic for the metrics job (PR4, task 5.1-5.4).

This module is deliberately pyflink-free so the spec metric formulas have full
unit-test coverage on interpreters where apache-flink has no wheel (CPython
3.14) and without a Docker daemon. The Flink job wiring
(:mod:`jobs.metrics.main`) calls into these pure functions from inside its
window ``ProcessWindowFunction`` / ``AggregateFunction`` and the deload
``KeyedProcessFunction``, and import-isolates pyflink.

Source of truth: serving-store spec "Metric Formulas".

  daily_load(d)       = sum(session_load on day d)
  acute_load          = sum(daily_load for d in [t-6, t])           -- 7d rolling SUM
  chronic_load_28d    = sum(daily_load for d in [t-27, t]) / n      -- 28d rolling AVG, n=days present (ADR-16)
  chronic_load_42d    = sum(daily_load for d in [t-41, t]) / n      -- 42d rolling AVG, n=days present (ADR-16)
  acute_chronic_ratio = acute_load / chronic_load_28d               -- NULL if chronic=0
  deload_flag         = +1 if ACR>1.3 for >=3 consecutive days
                      | -1 if ACR<0.8 for >=3 consecutive days
                      | 0  otherwise

The windowing/state backend (event-time TumblingEventTimeWindows daily pre-agg
+ SlidingEventTimeWindows rolling + KeyedProcessFunction deload counter) lives
in :mod:`jobs.metrics.main` and wires these pure functions. Per ADR-11 the
rolling load is computed with EVENT-TIME WINDOWS (not manual MapState); this
module holds the math those windows invoke.
"""

from __future__ import annotations

import base64
import json
import math
import time
from collections.abc import Iterable, Sequence
from typing import Any

# --- Constants (spec-locked) -----------------------------------------------

MILLIS_PER_DAY: int = 24 * 60 * 60 * 1000

# Rolling-load window sizes (spec "MUST Metrics" table).
ACUTE_WINDOW_DAYS: int = 7
CHRONIC_28D_WINDOW_DAYS: int = 28
CHRONIC_42D_WINDOW_DAYS: int = 42

# deload_flag rule thresholds (spec "deload_flag rules").
DELOAD_HIGH_THRESHOLD: float = 1.3
DELOAD_LOW_THRESHOLD: float = 0.8
DELOAD_CONSECUTIVE_DAYS: int = 3

# deload_flag values (spec: +1 overreaching, -1 undertraining, 0 normal).
DELOAD_HIGH: int = 1
DELOAD_LOW: int = -1
DELOAD_NORMAL: int = 0

# DLQ error types reused by the metrics job (spec "DLQ Routing" table). The
# NaN guard routes to VALIDATION_FAILURE (spec DLQ scenario: session_load=NaN).
# LATE_DATA is a metrics-job extension for the event-time window late side
# output (the spec's 4 error types cover validation/schema/deser/transform, not
# lateness; the design routes late data to the same DLQ topic as a "late log").
VALIDATION_FAILURE: str = "VALIDATION_FAILURE"
DESERIALIZATION_ERROR: str = "DESERIALIZATION_ERROR"
LATE_DATA: str = "LATE_DATA"

# Original-topic for the metrics DLQ envelope (the job consumes
# canonical.training_event; late/invalid records route back to its DLQ topic).
METRICS_SOURCE_TOPIC: str = "canonical.training_event"

# --- metrics-v2 score constants (spec-locked) -------------------------------
# Fatigue score thresholds (Gabbett 2016 ACR framework).
FATIGUE_HIGH: float = 80.0       # >= this -> "high_fatigue" flag
FATIGUE_MONITOR: float = 70.0    # >= this and < FATIGUE_HIGH -> "monitor" flag
READINESS_CAP: float = 80.0      # Honesty cap: readiness_score MUST NOT exceed 80 (Scenario 9)
FATIGUE_CLAMP_CEILING: float = 5.0   # ACR ratio clamped to [0, 5] before scaling
FATIGUE_SCALE: float = 20.0          # Scale factor: clamped_ratio * 20 -> 0..100


# --- metrics-v2 pure functions (load-based scores + coaching flags) ---------


def compute_fatigue_score(acute_load: float, chronic_load_42d: float) -> "float | None":
    """Compute fatigue_score from acute load and 42-day chronic baseline.

    Formula (Banister 1980 / Gabbett 2016):
        clamp(acute_load / max(chronic_load_42d, 1.0), 0.0, FATIGUE_CLAMP_CEILING)
        * FATIGUE_SCALE

    Returns:
        float in [0, 100], or None when chronic_load_42d == 0 (no baseline yet),
        or None when either input is NaN (FIX 2 — NaN must not propagate as nan).

    Spec: Scenarios 1-4.
    """
    # FIX 2: Guard NaN inputs — NaN propagates silently through arithmetic.
    if isinstance(chronic_load_42d, float) and math.isnan(chronic_load_42d):
        return None
    if isinstance(acute_load, float) and math.isnan(acute_load):
        return None
    if chronic_load_42d == 0:
        return None
    ratio = acute_load / max(chronic_load_42d, 1.0)
    clamped = min(max(ratio, 0.0), FATIGUE_CLAMP_CEILING)
    return clamped * FATIGUE_SCALE


def compute_readiness_score(acr: "float | None", chronic_load_28d: float) -> "float | None":
    """Compute readiness_score via ACR-zone piecewise interpolation.

    Zone boundaries (spec):
        ACR <= 0.8: undertrained zone, score 40-60
        ACR <= 1.0: optimal zone,      score 60-80
        ACR <= 1.3: moderate zone,     score 60-80 (descending)
        ACR >  1.3: high-load zone,    score 0-60  (descending)

    The result is capped at READINESS_CAP (80.0) — the honesty cap ensures the
    API never claims an athlete is fully "ready" (Scenario 9 invariant).

    Returns:
        float in [0, 80], or None when acr is None or chronic_load_28d == 0,
        or None when either input is NaN (FIX 2 — NaN must not propagate as nan).

    Spec: Scenarios 5-11.
    """
    # FIX 2: Guard NaN inputs before the None/zero checks.
    if isinstance(acr, float) and math.isnan(acr):
        return None
    if isinstance(chronic_load_28d, float) and math.isnan(chronic_load_28d):
        return None
    if chronic_load_28d == 0 or acr is None:
        return None
    if acr <= 0.8:
        score = 40.0 + (acr / 0.8) * 20.0
    elif acr <= 1.0:
        score = 60.0 + ((acr - 0.8) / 0.2) * 20.0
    elif acr <= 1.3:
        score = 80.0 - ((acr - 1.0) / 0.3) * 20.0
    else:
        score = max(0.0, 60.0 - ((acr - 1.3) / 0.7) * 60.0)
    return min(score, READINESS_CAP)


def compute_coaching_flags(
    deload_flag: int,
    fatigue_score: "float | None",
    readiness_score: "float | None",
) -> "list[str]":
    """Derive the list of active coaching flag strings from the computed scores.

    Active flags (multiple may be active simultaneously):
        "deload"       -- deload_flag == 1
        "undertrained" -- deload_flag == -1
        "high_fatigue" -- fatigue_score >= FATIGUE_HIGH (80.0)
        "monitor"      -- FATIGUE_MONITOR (70.0) <= fatigue_score < FATIGUE_HIGH

    "high_fatigue" and "monitor" are mutually exclusive (elif guard — Scenario 16).
    readiness_score is accepted as a parameter for forward-compat (future recovery
    flags); it is not consumed in this slice.

    Returns:
        list[str] — empty list when no flags are active (Scenario 14).

    Spec: Scenarios 12-16.
    """
    flags: list[str] = []
    if deload_flag == 1:
        flags.append("deload")
    if deload_flag == -1:
        flags.append("undertrained")
    if fatigue_score is not None:
        if fatigue_score >= FATIGUE_HIGH:
            flags.append("high_fatigue")
        elif FATIGUE_MONITOR <= fatigue_score < FATIGUE_HIGH:
            flags.append("monitor")
    return flags


# --- NaN / Inf guard (task 5.4) --------------------------------------------


def is_finite_load(value: object) -> bool:
    """Return True iff ``value`` is a real, finite number usable as session_load.

    Rejects None, bool (a session_load is a measurement, not a flag), NaN and
    +/-Inf. The Flink layer uses this to route bad session_load values to the
    DLQ as VALIDATION_FAILURE (spec DLQ scenario: session_load = NaN).
    """
    if value is None or isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value))


# --- daily_load = sum of session_load on a day -----------------------------


def sum_loads(loads: Iterable[float]) -> float:
    """Sum a sequence of session_load values for one day (spec daily_load).

    Raises ValueError on any non-finite value (NaN/Inf) so a corrupted daily
    sum cannot silently propagate; the Flink layer guards upstream via
    :func:`is_finite_load` and routes bad values to the DLQ, so a well-formed
    pipeline never feeds this a NaN.
    """
    total = 0.0
    for load in loads:
        if not is_finite_load(load):
            raise ValueError(f"non-finite session_load in daily sum: {load!r}")
        total += float(load)
    return total


# --- rolling windows (acute SUM, chronic AVERAGE) --------------------------


def acute_load(daily_loads: Sequence[float]) -> float:
    """7-day rolling SUM of daily_load (spec acute_load).

    The caller passes exactly the daily_load values inside the 7-day window
    (the event-time sliding window has already selected them). Returns 0.0 for
    an empty window (no recent load yet).
    """
    return float(sum(daily_loads))


def chronic_load(daily_loads: Sequence[float]) -> float:
    """Rolling AVERAGE of daily_load (spec chronic_load_28d / chronic_load_42d).

    Uses a DYNAMIC denominator /n (days present in the window), not a fixed
    /28 or /42 (ADR-16). Sports-science rationale: for a new athlete with only
    a few training days the fixed-window denominator would produce an
    artificially low chronic baseline (e.g., 3 days @100 -> chronic=10.7 via
    /28 -> ACR≈65 -> false DELOAD_HIGH on day 1). The /n denominator reflects
    the actual average load over the days present and makes ACR meaningful from
    the first recorded day.

    Used for both the 28d and 42d chronic baselines (the caller passes the
    daily_load values inside the relevant window). Returns 0.0 for an empty
    window to avoid a div-by-zero (no baseline yet -> ACR becomes NULL via
    :func:`acute_chronic_ratio`).
    """
    n = len(daily_loads)
    if n == 0:
        return 0.0
    return float(sum(daily_loads)) / float(n)


# --- acute_chronic_ratio = acute / chronic_28d (NULL if chronic=0) ---------


def acute_chronic_ratio(acute: float, chronic_28d: float) -> float | None:
    """Compute ACR = acute_load / chronic_load_28d (spec).

    Returns None when chronic_load_28d == 0 (spec: "NULL if chronic=0"). The
    caller MUST propagate None downstream (the deload state machine treats None
    as a streak-resetting non-breach day).
    """
    if chronic_28d == 0:
        return None
    return float(acute) / float(chronic_28d)


# --- deload consecutive-day state machine ----------------------------------


def update_deload_state(
    prev_count: int,
    prev_sign: int,
    acr: float | None,
) -> tuple[int, int, int]:
    """Advance the deload consecutive-day state machine by one day.

    Inputs:
      prev_count: consecutive-day count carried from the previous day.
      prev_sign:  the breach direction carried from the previous day
                  (DELOAD_HIGH / DELOAD_LOW / DELOAD_NORMAL).
      acr:        the current day's acute_chronic_ratio (None when
                  chronic_load_28d == 0).

    Returns ``(new_count, new_sign, flag)`` where ``flag`` is the deload_flag
    for the current day per the spec rule:
      +1 if ACR > 1.3 for >= 3 consecutive days
      -1 if ACR < 0.8 for >= 3 consecutive days
      0  otherwise

    A None ACR (chronic=0) resets the streak: no breach can be asserted without
    a ratio. Thresholds are strict (>= 1.3 is NOT a high breach; <= 0.8 is NOT
    a low breach), matching the spec's ``>`` and ``<``.
    """
    if acr is None:
        return 0, DELOAD_NORMAL, DELOAD_NORMAL

    if acr > DELOAD_HIGH_THRESHOLD:
        if prev_sign == DELOAD_HIGH:
            new_count = prev_count + 1
        else:
            new_count = 1
        new_sign = DELOAD_HIGH
        flag = DELOAD_HIGH if new_count >= DELOAD_CONSECUTIVE_DAYS else DELOAD_NORMAL
        return new_count, new_sign, flag

    if acr < DELOAD_LOW_THRESHOLD:
        if prev_sign == DELOAD_LOW:
            new_count = prev_count + 1
        else:
            new_count = 1
        new_sign = DELOAD_LOW
        flag = DELOAD_LOW if new_count >= DELOAD_CONSECUTIVE_DAYS else DELOAD_NORMAL
        return new_count, new_sign, flag

    # 0.8 <= ACR <= 1.3 -> normal day, streak resets.
    return 0, DELOAD_NORMAL, DELOAD_NORMAL


def compute_deload_flags(acrs: Sequence[float | None]) -> list[int]:
    """Run the deload state machine over a sequence of daily ACRs.

    Convenience batch wrapper around :func:`update_deload_state` for unit
    testing the full consecutive-day rule end-to-end. The Flink
    ``KeyedProcessFunction`` uses :func:`update_deload_state` step-by-step with
    ValueState (it does not buffer the whole history); this helper mirrors that
    exactly so the unit tests prove the rule the live job will run.

    Returns one deload_flag per input day, in order.
    """
    count = 0
    sign = DELOAD_NORMAL
    flags: list[int] = []
    for acr in acrs:
        count, sign, flag = update_deload_state(count, sign, acr)
        flags.append(flag)
    return flags


# --- Rolling-window metrics computation (single source of truth, C8) ------
# Extracted from RollingMetricsWindowFn.process() so the window math is:
#   1. Unit-tested directly (no pyflink needed)
#   2. Shared with any future operator that needs the same rolling formulas
#   3. A single implementation — the function running in prod IS the tested one.
#
# The window function in main.py calls compute_rolling_metrics() instead of
# reimplementing the 7/28/42 filter + SUM/AVG inline.


def compute_rolling_metrics(
    by_day: dict[int, float],
    window_end: int,
) -> tuple[float, float, float, "float | None"]:
    """Compute acute_load, chronic_load_28d, chronic_load_42d, and ACR.

    Args:
        by_day: mapping of {day_start_ms: daily_load} for all days in the
                42d sliding window (already deduped to keep the max daily_load
                per day, absorbing ContinuousEventTimeTrigger multi-emit).
        window_end: epoch-ms of the window's exclusive end (day-aligned).
                    metric_date = window_end - MILLIS_PER_DAY.

    Returns:
        (acute_load, chronic_load_28d, chronic_load_42d, acr)
        acr is None when chronic_load_28d == 0 (spec: NULL if chronic=0).

    This is the CANONICAL formula implementation shared between the unit tests
    and the Flink window operator. The unit tests (TestComputeRollingMetrics)
    prove the formulas; the Flink operator calls this function so what is
    tested IS what runs in production. (C8 single source of truth)
    """
    metric_date = window_end - MILLIS_PER_DAY  # last full day in the window

    # Slice the day buckets for each window size.
    acute_start = window_end - ACUTE_WINDOW_DAYS * MILLIS_PER_DAY
    c28_start = window_end - CHRONIC_28D_WINDOW_DAYS * MILLIS_PER_DAY
    c42_start = window_end - CHRONIC_42D_WINDOW_DAYS * MILLIS_PER_DAY

    acute_days = [v for day, v in by_day.items() if acute_start <= day <= metric_date]
    c28_days = [v for day, v in by_day.items() if c28_start <= day <= metric_date]
    c42_days = [v for day, v in by_day.items() if c42_start <= day <= metric_date]

    al = acute_load(acute_days)
    cl28 = chronic_load(c28_days)
    cl42 = chronic_load(c42_days)
    acr = acute_chronic_ratio(al, cl28)
    return al, cl28, cl42, acr


# --- Metrics-row JSON serialization (RFC 8259 safe) ----------------------
#
# Extracted from main.py's inner _metrics_row_to_json closure so the
# serialization logic is unit-testable and pyflink-free.
#
# NF-2 fix: use allow_nan=False so any non-finite numeric value raises
# ValueError immediately (fail-fast to DLQ) instead of emitting the
# non-standard `NaN` / `Infinity` tokens that violate RFC 8259 and crash
# PR5's PostgreSQL consumer.  ACR is guarded to None before this call
# (chronic==0 sentinel), so None/nan ACR becomes JSON null cleanly.


def metrics_row_to_json(
    *,
    athlete_id: str,
    metric_date: int,
    acute_load_val: float,
    chronic_load_28d_val: float,
    chronic_load_42d_val: float,
    acr_val: "float | None",
    deload_flag: int,
    fatigue_score_val: "float | None" = None,
    readiness_score_val: "float | None" = None,
    coaching_flags_val: "list[str] | None" = None,
) -> str:
    """Serialize one metrics row to a RFC 8259-compliant JSON string.

    Raises ValueError for any non-finite load field (NaN, +/-Inf) so the
    caller (Flink map operator) routes the record to the DLQ rather than
    emitting an invalid JSON token to the metrics stream.

    ACR is allowed to be None (chronic==0) or float('nan') (IEEE-754 sentinel
    from the upstream guard); both are serialized as JSON null.

    FIX 3: fatigue_score_val, readiness_score_val, coaching_flags_val are now
    included in the JSON payload so the Kafka staging topic matches the PG schema.
    NaN scores are serialized as null (not raised) — they are derived values,
    not raw load fields, so graceful degradation applies.
    """
    # Guard ACR: None and NaN both become null in JSON.
    if acr_val is None or (isinstance(acr_val, float) and math.isnan(acr_val)):
        acr_json: "float | None" = None
    else:
        acr_json = float(acr_val)

    # Guard score fields: None and NaN both become null in JSON (graceful degradation).
    def _score_to_json(val: "float | None") -> "float | None":
        if val is None:
            return None
        if isinstance(val, float) and math.isnan(val):
            return None
        return float(val)

    payload = {
        "athlete_id": athlete_id,
        "metric_date": metric_date,
        "acute_load": acute_load_val,
        "chronic_load_28d": chronic_load_28d_val,
        "chronic_load_42d": chronic_load_42d_val,
        "acute_chronic_ratio": acr_json,
        "deload_flag": deload_flag,
        # FIX 3: v2 fields included in Kafka staging JSON.
        "fatigue_score": _score_to_json(fatigue_score_val),
        "readiness_score": _score_to_json(readiness_score_val),
        "coaching_flags": coaching_flags_val if coaching_flags_val is not None else [],
    }
    # allow_nan=False enforces RFC 8259: any non-finite float raises ValueError
    # so the Flink operator can route the record to the DLQ (fail-fast).
    return json.dumps(payload, allow_nan=False)


# --- DLQ error envelope (spec "DLQ Error Envelope") -----------------------

# Maximum raw bytes for original_value before truncation. Own copy per ADR-5:
# metrics and canonicalize jobs deploy independently; no cross-job import.
# 512 KiB raw keeps base64-encoded envelope under Kafka/Redpanda 1 MB default.
MAX_ORIGINAL_VALUE_BYTES = 524_288


def epoch_ms_now() -> int:
    """Wall-clock now as epoch-ms long (DLQ envelope timestamp)."""
    return int(time.time() * 1000)


def _encode_original_value(value: Any) -> tuple[str, bool, int]:
    """Base64-encode original_value bytes, enforcing the 512 KiB size guard.

    Returns:
        A tuple (encoded_value, truncated, size_bytes) where:
        - encoded_value: base64 ASCII string, or "" when None or oversized.
        - truncated: True only when raw byte length exceeds MAX_ORIGINAL_VALUE_BYTES.
        - size_bytes: raw byte count (0 for None inputs).
    """
    if value is None:
        return "", False, 0
    if isinstance(value, (bytes, bytearray)):
        raw_bytes = bytes(value)
    elif isinstance(value, str):
        raw_bytes = value.encode("utf-8")
    else:
        raw_bytes = json.dumps(value).encode("utf-8")
    size = len(raw_bytes)
    if size > MAX_ORIGINAL_VALUE_BYTES:
        return "", True, size
    return base64.b64encode(raw_bytes).decode("ascii"), False, size


def build_metrics_dlq_envelope(
    *,
    original_key: str | None,
    original_value: Any,
    error_type: str,
    error_message: str,
    timestamp: int,
    original_topic: str = METRICS_SOURCE_TOPIC,
    error_stack: str | None = None,
) -> dict:
    """Build the DLQ error envelope dict (spec "DLQ Error Envelope").

    Mirrors the canonicalize job's envelope so DLQ consumers see one shape
    across jobs. DLQ messages are JSON (not Avro) because the original event
    may be unparseable. error_type is one of VALIDATION_FAILURE (NaN guard),
    LATE_DATA (event-time window late side output), or DESERIALIZATION_ERROR.
    """
    encoded, truncated, size_bytes = _encode_original_value(original_value)
    return {
        "original_topic": original_topic,
        "original_key": original_key,
        "original_value": encoded,
        "original_value_truncated": truncated,
        "original_value_size_bytes": size_bytes,
        "error_type": error_type,
        "error_message": error_message,
        "error_stack": error_stack,
        "timestamp": int(timestamp),
    }
