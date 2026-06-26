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


# --- DLQ error envelope (spec "DLQ Error Envelope") -----------------------


def epoch_ms_now() -> int:
    """Wall-clock now as epoch-ms long (DLQ envelope timestamp)."""
    return int(time.time() * 1000)


def _encode_original_value(value: Any) -> str:
    """Base64-encode the original_value bytes for the DLQ envelope (spec)."""
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        raw_bytes = bytes(value)
    elif isinstance(value, str):
        raw_bytes = value.encode("utf-8")
    else:
        raw_bytes = json.dumps(value).encode("utf-8")
    return base64.b64encode(raw_bytes).decode("ascii")


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
    return {
        "original_topic": original_topic,
        "original_key": original_key,
        "original_value": _encode_original_value(original_value),
        "error_type": error_type,
        "error_message": error_message,
        "error_stack": error_stack,
        "timestamp": int(timestamp),
    }
