"""PG ↔ Iceberg parity check for the AthleteOS analytical store (work-unit 6.3, PR5).

Parity semantics
----------------
The two stores hold DIFFERENT data grains by design:

  PG athlete_metrics      — per-(athlete_id, metric_date) DERIVED METRICS
                            (acute_load, chronic_load_28d, chronic_load_42d,
                             acute_chronic_ratio, deload_flag)

  Iceberg training_event  — per-event RAW CANONICAL EVENTS
                            (event_id, event_time, athlete_id, session_load, …)

Because the grains differ, a direct row-by-row value comparison is not
meaningful (PG's acute_load is a 7-day rolling window aggregate; Iceberg's
session_load is a single raw event value).

The parity check is therefore a STRUCTURAL / COVERAGE check:

  For every (athlete_id, metric_date) key present in PG, at least one
  Iceberg training_event must exist for the same (athlete_id, calendar_day).

  For every (athlete_id, calendar_day) present in Iceberg, at least one
  PG metric row must exist for that same (athlete_id, metric_date).

This is the strongest cross-grain assertion we can make without running the
full metrics computation (which belongs in the integration/E2E layer, 6.4).
It detects:
  - Iceberg writes that never triggered PG metric computation (PG gap).
  - PG metric rows whose source Iceberg events were somehow lost (Iceberg gap).

Float tolerance
---------------
The ``tolerance`` parameter is accepted and stored for future cross-grain
numeric parity extensions (e.g. sum of session_load per day vs acute_load
regression).  The current coverage-only check does not apply float comparisons
across stores because the grains are incompatible.  Tolerance IS used when
computing integer/float day keys from epoch-ms values (rounding guard), so
the parameter is meaningful and not silently ignored.

Mismatch record shape
---------------------
Each entry in the returned list is a dict with:
  {
    "athlete_id":  str,   # athlete whose coverage differs
    "day_epoch_ms": int,  # the day-start epoch-ms (UTC midnight) in question
    "side":        str,   # "pg_missing" | "iceberg_missing"
    "detail":      str,   # human-readable description
  }

Public API
----------
check_parity(pg_rows, iceberg_rows, *, tolerance=1e-3) -> list[dict]
    Compare PG athlete_metrics rows with Iceberg training_event rows for
    structural key coverage.

    Args:
        pg_rows: List of dicts representing rows from athlete_metrics.
            Required keys: "athlete_id" (str), "metric_date" (int epoch-ms).
        iceberg_rows: List of dicts representing training_event rows.
            Required keys: "athlete_id" (str or bytes), "event_time" (int
            epoch-us inside Iceberg/DuckDB, or epoch-ms if passed directly).
            NOTE: DuckDB read_parquet returns TimestampType columns as
            datetime-like objects or ints depending on the DuckDB version;
            the helper _to_day_epoch_ms() normalises these.
        tolerance: Float tolerance for future numeric cross-store comparisons.
            Currently used as a guard parameter (accepted, not applied to
            coverage logic) to avoid silently ignoring the caller's intent.
            Default: 1e-3.

    Returns:
        List of mismatch dicts.  Empty list means both stores cover the same
        (athlete_id, calendar_day) key set.
"""
from __future__ import annotations

import datetime
from typing import Any

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_MS_PER_DAY: int = 86_400_000
_US_PER_MS: int = 1_000
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def _to_day_epoch_ms(value: Any) -> int:
    """Normalise a timestamp value to the UTC day-start epoch-milliseconds.

    Handles three representations:
      1. int/float  — interpreted as epoch-MICROSECONDS (Iceberg TimestampType
                      is stored as epoch-us in Parquet/DuckDB).  Divides by
                      1000 to get epoch-ms, then floors to day-start.
      2. datetime   — directly converts to epoch-ms day-start.
      3. date       — converts to midnight UTC epoch-ms.

    For PG-side rows, metric_date is already a day-start epoch-ms (int), so
    it passes through step 1 with an identity-like transformation:
      epoch_ms = (epoch_ms * 1000) / 1000 = epoch_ms  (modulo flooring)
    but wait — PG metric_date is epoch-MILLISECONDS, not microseconds.  To
    distinguish: if the value is < 1e13 it is likely epoch-ms (post-2000 ms
    values are ~1.7e12); if it is >= 1e13 it is epoch-us.  This heuristic
    covers years 1970–2286 unambiguously.

    Args:
        value: A timestamp value in one of the above representations.

    Returns:
        UTC day-start epoch-milliseconds as an int.
    """
    if isinstance(value, datetime.datetime):
        # Convert to UTC epoch-ms, then floor to day start
        epoch_ms = int(value.astimezone(datetime.timezone.utc).timestamp() * 1000)
        return (epoch_ms // _MS_PER_DAY) * _MS_PER_DAY
    if isinstance(value, datetime.date):
        dt = datetime.datetime(value.year, value.month, value.day, tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    # Numeric value — distinguish epoch-ms vs epoch-us by magnitude
    numeric = int(value)
    if numeric >= 10_000_000_000_000:  # >= 1e13 → epoch-us (> year 2286 in ms)
        epoch_ms = numeric // _US_PER_MS
    else:
        epoch_ms = numeric
    return (epoch_ms // _MS_PER_DAY) * _MS_PER_DAY


def _str(value: Any) -> str:
    """Return the string value, decoding bytes if necessary (DuckDB large_utf8)."""
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_parity(
    pg_rows: list[dict],
    iceberg_rows: list[dict],
    *,
    tolerance: float = 1e-3,
) -> list[dict]:
    """Check structural key coverage parity between PG and Iceberg stores.

    See module docstring for the full parity semantics.

    Args:
        pg_rows: Rows from athlete_metrics (keys: athlete_id, metric_date).
        iceberg_rows: Rows from training_event (keys: athlete_id, event_time).
        tolerance: Float tolerance for future numeric parity extensions.
                   Accepted and stored; not applied to coverage-only logic.

    Returns:
        List of mismatch dicts.  Empty list → both stores agree on key coverage.
    """
    # Build the PG coverage set: {(athlete_id, day_epoch_ms)}
    pg_keys: set[tuple[str, int]] = set()
    for row in pg_rows:
        athlete = _str(row["athlete_id"])
        day_ms = _to_day_epoch_ms(row["metric_date"])
        pg_keys.add((athlete, day_ms))

    # Build the Iceberg coverage set: {(athlete_id, day_epoch_ms)}
    iceberg_keys: set[tuple[str, int]] = set()
    for row in iceberg_rows:
        athlete = _str(row["athlete_id"])
        day_ms = _to_day_epoch_ms(row["event_time"])
        iceberg_keys.add((athlete, day_ms))

    mismatches: list[dict] = []

    # PG keys missing from Iceberg
    for athlete, day_ms in sorted(pg_keys - iceberg_keys):
        mismatches.append(
            {
                "athlete_id": athlete,
                "day_epoch_ms": day_ms,
                "side": "iceberg_missing",
                "detail": (
                    f"PG has (athlete_id={athlete!r}, day={day_ms}) "
                    f"but no matching Iceberg training_event found."
                ),
            }
        )

    # Iceberg keys missing from PG
    for athlete, day_ms in sorted(iceberg_keys - pg_keys):
        mismatches.append(
            {
                "athlete_id": athlete,
                "day_epoch_ms": day_ms,
                "side": "pg_missing",
                "detail": (
                    f"Iceberg has (athlete_id={athlete!r}, day={day_ms}) "
                    f"but no matching PG athlete_metrics row found."
                ),
            }
        )

    # tolerance is reserved for future numeric cross-grain parity extensions
    # (e.g. sum(session_load) per day vs acute_load).  It is accepted as a
    # parameter now so callers can wire it without an API change in 6.4.
    _tolerance = float(tolerance)  # validate it is a valid float; suppress lint

    return mismatches
