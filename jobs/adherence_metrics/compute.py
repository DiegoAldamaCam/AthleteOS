"""Pure adherence-score computation for the adherence_metrics job (ADH-C1..C7).

This module is deliberately pyflink-free and has no I/O side-effects —
the same design philosophy as jobs.wellness_metrics.compute — so the formula
is fully unit-testable without a Flink runtime or database.

Public API
----------
compute_adherence_score(actual_sessions, planned_sessions, actual_volume, target_volume)
    -> float | None
    Compute the block-level adherence score on a [0.0, 1.0] scale using:
      * Full formula (target_volume present):
            0.5 × min(actual_sessions/planned_sessions, 1.0)
          + 0.5 × min(actual_volume/target_volume, 1.0)
      * Sessions-only degraded formula (target_volume is None or ≤ 0):
            min(actual_sessions/planned_sessions, 1.0)   (weight 1.0)
      * None when planned_sessions ≤ 0 (E5 guard — div-by-zero prevention)

parse_weekly_volume_targets(text) -> float | None
    Parse the weekly_volume_targets JSON string from planning_blocks.
    Returns the sum of all numeric dict values, or None on malformed input.
    Mirrors ADR-22 volume-parse fallback (E3).

Design notes (ADR-22)
----------------------
- E1 (zero actual): falls out naturally from the formula → 0.0 (not None).
- E3 (bad JSON): parse_weekly_volume_targets returns None → sessions-only path.
- E5 (no plan): planned_sessions ≤ 0 → None (no score possible).
- All sub-scores capped at 1.0 via min() — overcompliance doesn't exceed 1.0.
- Pyflink-free, no I/O — full unit coverage without runtime or DB.
"""

from __future__ import annotations

import json
from typing import Optional

# ---------------------------------------------------------------------------
# Weights (named constants eliminate magic numbers — ADR-22)
# ---------------------------------------------------------------------------

_SESSION_WEIGHT: float = 0.5
_VOLUME_WEIGHT: float = 0.5


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def compute_adherence_score(
    actual_sessions: int,
    planned_sessions: int,
    actual_volume: float,
    target_volume: Optional[float],
) -> Optional[float]:
    """Compute the block-level adherence score in [0.0, 1.0], or None when unplannable.

    Args:
        actual_sessions:  Number of distinct training days completed in the block window.
        planned_sessions: Total planned sessions for the block (planned_per_week × weeks).
                          Must be > 0; returns None otherwise (E5 guard).
        actual_volume:    Sum of session_load values for completed sessions.
        target_volume:    Total planned volume for the block (weekly_target × weeks).
                          When None (malformed JSON or absent), uses sessions-only formula (E3).

    Returns:
        A float in [0.0, 1.0] representing adherence, or None when planned_sessions ≤ 0.

    Formula (target_volume present and > 0):
        0.5 × min(actual_sessions / planned_sessions, 1.0)
      + 0.5 × min(actual_volume / target_volume, 1.0)

    Degraded formula (target_volume is None or ≤ 0):
        min(actual_sessions / planned_sessions, 1.0)
    """
    # E5: div-by-zero guard — no meaningful score when there is no plan
    if planned_sessions <= 0:
        return None

    sessions_ratio = min(actual_sessions / planned_sessions, 1.0)

    # E3: sessions-only formula when volume data is absent or unusable
    if target_volume is None or target_volume <= 0:
        return sessions_ratio

    volume_ratio = min(actual_volume / target_volume, 1.0)
    return _SESSION_WEIGHT * sessions_ratio + _VOLUME_WEIGHT * volume_ratio


def parse_weekly_volume_targets(text: str) -> Optional[float]:
    """Parse weekly_volume_targets JSON string → sum of numeric dict values.

    Args:
        text: JSON string from planning_blocks.weekly_volume_targets.
              Expected to be a JSON object (dict) with numeric values.

    Returns:
        Sum of all numeric values in the dict as float, or None on any error:
        - malformed JSON (ADH-C4)
        - valid JSON that is not a dict (array, null, string, number)
        - empty string

    Examples:
        >>> parse_weekly_volume_targets('{"strength": 3, "cardio": 2}')
        5.0
        >>> parse_weekly_volume_targets("not-json")
        None
        >>> parse_weekly_volume_targets("null")
        None
    """
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None

    if not isinstance(parsed, dict):
        return None

    total = sum(v for v in parsed.values() if isinstance(v, (int, float)))
    return float(total)
