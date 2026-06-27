"""Pure recovery-score computation for the wellness-metrics job (W3-1..W3-6).

This module is deliberately pyflink-free and has no I/O side-effects —
the same design philosophy as jobs.metrics.compute — so the formula is
fully unit-testable without a Flink runtime or database.

Public API
----------
compute_recovery_score(hrv, hrv_baseline, sleep_hours, perceived_recovery) -> float | None
    Compute the daily recovery score on a 0-100 scale using:
      * Formula A (hrv present):  normalized_hrv*50 + sleep_score*30 + perceived_score*20
      * Formula B (hrv absent):   sleep_score*60 + perceived_score*40
      * None when ALL of {hrv, sleep_hours, perceived_recovery} are None

Design notes (obs #133, ADR-18)
--------------------------------
- hrv_baseline is passed per-call (ADR-18 MVP: fixed config constant, not rolling 7d state).
- max(hrv_baseline, 1.0) guard prevents ZeroDivisionError when baseline is 0 (W3-2).
- All intermediate scores are clamped to [0, 1]; final result is clamped to [0, 100].
- The clamp helper is a pure closure — no mutable state.
"""

from __future__ import annotations

from typing import Optional

# ---------------------------------------------------------------------------
# Weights (named constants eliminate magic numbers in the formula)
# ---------------------------------------------------------------------------

_WEIGHT_HRV: float = 50.0
_WEIGHT_SLEEP: float = 30.0
_WEIGHT_PERCEIVED: float = 20.0

_WEIGHT_B_SLEEP: float = 60.0
_WEIGHT_B_PERCEIVED: float = 40.0

_SLEEP_TARGET: float = 8.0       # hours — full sleep score at this value
_PERCEIVED_MAX: float = 10.0     # max perceived recovery scale


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _clamp(value: float, lo: float, hi: float) -> float:
    """Clamp value to [lo, hi]."""
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_recovery_score(
    hrv: Optional[float],
    hrv_baseline: float,
    sleep_hours: Optional[float],
    perceived_recovery: Optional[float],
) -> Optional[float]:
    """Compute the daily recovery score in [0.0, 100.0], or None when all inputs absent.

    Args:
        hrv:                 Current HRV measurement (ms). None if not recorded today.
        hrv_baseline:        Reference HRV baseline (ms). Treated as max(baseline, 1.0)
                             to prevent ZeroDivisionError (W3-2, ADR-18).
        sleep_hours:         Hours slept. None if not recorded.
        perceived_recovery:  Subjective recovery rating on a 0–10 scale. None if absent.

    Returns:
        A float in [0.0, 100.0] when at least one input is present.
        None when all of {hrv, sleep_hours, perceived_recovery} are None.

    Formula A (hrv not None):
        normalized_hrv  = clamp((hrv / max(hrv_baseline, 1.0) - 0.5) / 1.0, 0, 1)
        sleep_score     = clamp(sleep_hours / SLEEP_TARGET, 0, 1)     — 0 when None
        perceived_score = clamp(perceived_recovery / PERCEIVED_MAX, 0, 1) — 0 when None
        score = normalized_hrv * 50 + sleep_score * 30 + perceived_score * 20

    Formula B (hrv is None, at least one of sleep/perceived present):
        sleep_score     = clamp(sleep_hours / SLEEP_TARGET, 0, 1)
        perceived_score = clamp(perceived_recovery / PERCEIVED_MAX, 0, 1)
        score = sleep_score * 60 + perceived_score * 40
    """
    # NULL guard: all variable inputs absent → no score can be computed
    if hrv is None and sleep_hours is None and perceived_recovery is None:
        return None

    # Sub-scores shared by both formulas
    sleep_score = _clamp(
        (sleep_hours or 0.0) / _SLEEP_TARGET,
        0.0,
        1.0,
    )
    perceived_score = _clamp(
        (perceived_recovery or 0.0) / _PERCEIVED_MAX,
        0.0,
        1.0,
    )

    if hrv is None:
        # Formula B — graceful degradation without HRV
        raw = sleep_score * _WEIGHT_B_SLEEP + perceived_score * _WEIGHT_B_PERCEIVED
        return _clamp(raw, 0.0, 100.0)

    # Formula A — full HRV-guided score
    safe_baseline = max(hrv_baseline, 1.0)
    normalized_hrv = _clamp((hrv / safe_baseline - 0.5) / 1.0, 0.0, 1.0)
    raw = (
        normalized_hrv * _WEIGHT_HRV
        + sleep_score * _WEIGHT_SLEEP
        + perceived_score * _WEIGHT_PERCEIVED
    )
    return _clamp(raw, 0.0, 100.0)
