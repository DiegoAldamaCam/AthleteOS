"""Pure, pyflink-free PostgreSQL UPSERT sink helpers (task 6.1, PR5 Phase 6).

This module is deliberately pyflink-free (same pattern as jobs.metrics.compute)
so the UPSERT logic has full unit-test coverage without a Flink runtime or a
real PostgreSQL instance.

Public API
----------
epoch_ms_to_date(epoch_ms: int) -> datetime.date
    Convert a day-start epoch-ms value to the exact UTC calendar date.
    metric_date in the metrics stream is always a day-start epoch-ms
    (window_end - MILLIS_PER_DAY), so the conversion is deterministic.

build_upsert(record: dict) -> tuple[str, tuple]
    Return (sql, params) for a single metrics record:
    INSERT INTO athlete_metrics ... ON CONFLICT (athlete_id, metric_date)
    DO UPDATE SET ...
    acute_chronic_ratio None / float('nan') → params contains None (SQL NULL).

execute_upsert(cursor, record: dict) -> None
    Execute the UPSERT for a single record using a psycopg2-compatible cursor.
    Thin wrapper around cursor.execute(sql, params) — injectable for unit tests
    via a fake/mock cursor (no real DB required).

Design notes
------------
- Retry / backoff / DLQ-on-exhaustion wiring happens at the Flink integration
  level (work-unit 6.4), not here.  This layer is the pure "build + execute"
  primitive that the Flink checkpoint callback will call per batch.
- psycopg2-binary is already a project dependency (pyproject.toml).
"""

from __future__ import annotations

import datetime
import math
from typing import Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MILLIS_PER_SECOND: int = 1_000
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

# UPSERT SQL template (parameterized with %s — psycopg2 style).
# Parameter order matches _record_to_params().
_UPSERT_SQL: str = """
INSERT INTO athlete_metrics
    (athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
     acute_chronic_ratio, deload_flag)
VALUES
    (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (athlete_id, metric_date)
DO UPDATE SET
    acute_load          = EXCLUDED.acute_load,
    chronic_load_28d    = EXCLUDED.chronic_load_28d,
    chronic_load_42d    = EXCLUDED.chronic_load_42d,
    acute_chronic_ratio = EXCLUDED.acute_chronic_ratio,
    deload_flag         = EXCLUDED.deload_flag
""".strip()


# ---------------------------------------------------------------------------
# epoch_ms_to_date
# ---------------------------------------------------------------------------


def epoch_ms_to_date(epoch_ms: int) -> datetime.date:
    """Convert a day-start epoch-ms value to the exact UTC calendar date.

    metric_date in the metrics stream is always the start of a UTC calendar
    day expressed as epoch-milliseconds (window_end - MILLIS_PER_DAY from the
    Flink window operator in jobs.metrics.main). The conversion is therefore
    deterministic: divide by 1000 to get epoch-seconds, then compute the UTC
    date via datetime.

    Args:
        epoch_ms: Day-start epoch-milliseconds (non-negative integer).

    Returns:
        The UTC calendar date that epoch_ms falls on (the day that starts at
        that timestamp — since it IS the day-start, this is unambiguous).

    Examples:
        >>> epoch_ms_to_date(0)
        datetime.date(1970, 1, 1)
        >>> epoch_ms_to_date(1704067200000)  # 2024-01-01 00:00:00 UTC
        datetime.date(2024, 1, 1)
    """
    epoch_seconds = epoch_ms / _MILLIS_PER_SECOND
    dt_utc = datetime.datetime.fromtimestamp(epoch_seconds, tz=datetime.timezone.utc)
    return dt_utc.date()


# ---------------------------------------------------------------------------
# build_upsert
# ---------------------------------------------------------------------------


def _sanitize_acr(value: Any) -> "float | None":
    """Return None for None or NaN; otherwise return the float value.

    Both None (chronic_load_28d == 0 sentinel) and float('nan') (IEEE-754
    wire encoding from the Flink operator) map to SQL NULL as per the
    approved PR5 decision.
    """
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return float(value)


def build_upsert(record: dict) -> tuple[str, tuple]:
    """Build the parameterized UPSERT SQL and params for one metrics record.

    Args:
        record: A metrics dict with keys:
            athlete_id (str), metric_date (int epoch-ms),
            acute_load (float), chronic_load_28d (float),
            chronic_load_42d (float), acute_chronic_ratio (float | None),
            deload_flag (int).

    Returns:
        (sql, params) where sql is the UPSERT string and params is a tuple
        of 7 bound values in the order matching the SQL placeholders.
        acute_chronic_ratio None/nan -> params contains None (SQL NULL).
    """
    metric_date_val: datetime.date = epoch_ms_to_date(int(record["metric_date"]))
    acr: "float | None" = _sanitize_acr(record.get("acute_chronic_ratio"))

    params: tuple = (
        str(record["athlete_id"]),
        metric_date_val,
        float(record["acute_load"]),
        float(record["chronic_load_28d"]),
        float(record["chronic_load_42d"]),
        acr,
        int(record["deload_flag"]),
    )
    return _UPSERT_SQL, params


# ---------------------------------------------------------------------------
# execute_upsert
# ---------------------------------------------------------------------------


def execute_upsert(cursor: Any, record: dict) -> None:
    """Execute the UPSERT for a single metrics record.

    Args:
        cursor: A psycopg2-compatible cursor (or a compatible fake for tests).
                The cursor must expose ``execute(sql, params)``; this function
                does not commit — the caller owns transaction management.
        record: A metrics dict (same shape as build_upsert expects).

    Design: thin wrapper so the DB I/O is injectable. The per-checkpoint
    batching loop, retry/backoff, and DLQ-on-exhaustion live in the Flink
    integration layer (work-unit 6.4, jobs/metrics/main.py).
    """
    sql, params = build_upsert(record)
    cursor.execute(sql, params)
