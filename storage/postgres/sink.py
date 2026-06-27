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

upsert_with_retry(record, conn, conn_factory, max_retries, base_backoff_s) -> None
    Execute the UPSERT for one record with exponential-backoff retry and
    automatic reconnect on OperationalError / InterfaceError.  Extracted from
    _PgUpsertFn.process_element (jobs/metrics/main.py) so the reconnect path
    is unit-testable without a Flink runtime.

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
import time
from typing import Any, Callable

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MILLIS_PER_SECOND: int = 1_000

# UPSERT SQL template (parameterized with %s — psycopg2 style).
# Parameter order matches build_upsert() params tuple (10 fields, positions 0-9).
# WARNING-1: column list, VALUES placeholders, and DO UPDATE SET MUST stay in
# exact field-order agreement (athlete_id[0]..deload_flag[6]..coaching_flags[9]).
_UPSERT_SQL: str = """
INSERT INTO athlete_metrics
    (athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
     acute_chronic_ratio, deload_flag,
     fatigue_score, readiness_score, coaching_flags)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (athlete_id, metric_date)
DO UPDATE SET
    acute_load          = EXCLUDED.acute_load,
    chronic_load_28d    = EXCLUDED.chronic_load_28d,
    chronic_load_42d    = EXCLUDED.chronic_load_42d,
    acute_chronic_ratio = EXCLUDED.acute_chronic_ratio,
    deload_flag         = EXCLUDED.deload_flag,
    fatigue_score       = EXCLUDED.fatigue_score,
    readiness_score     = EXCLUDED.readiness_score,
    coaching_flags      = EXCLUDED.coaching_flags
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


def _sanitize_float(value: Any) -> "float | None":
    """Return None for None or NaN; otherwise return the float value.

    Both None (chronic_load_28d == 0 sentinel) and float('nan') (IEEE-754
    wire encoding from the Flink operator) map to SQL NULL as per the
    approved PR5 decision.

    FIX 4: Renamed from _sanitize_acr — the helper is reused for
    fatigue_score and readiness_score in addition to acute_chronic_ratio.
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
            deload_flag (int),
            fatigue_score (float | nan | None),  — nan/None -> SQL NULL
            readiness_score (float | nan | None), — nan/None -> SQL NULL
            coaching_flags (str),                — JSON string, never None

    Returns:
        (sql, params) where sql is the UPSERT string and params is a tuple
        of 10 bound values in the order matching the SQL placeholders.
        acute_chronic_ratio, fatigue_score, readiness_score None/nan -> None (SQL NULL).
        coaching_flags is passed through as a JSON string; empty list -> "[]".
    """
    metric_date_val: datetime.date = epoch_ms_to_date(int(record["metric_date"]))
    acr: "float | None" = _sanitize_float(record.get("acute_chronic_ratio"))
    fatigue: "float | None" = _sanitize_float(record.get("fatigue_score"))
    readiness: "float | None" = _sanitize_float(record.get("readiness_score"))
    # coaching_flags is a JSON string from the Row (json.dumps in process_element);
    # it is never None in normal flow — the Row always carries "[]" at minimum.
    coaching_flags_val: str = str(record.get("coaching_flags") or "[]")

    params: tuple = (
        str(record["athlete_id"]),
        metric_date_val,
        float(record["acute_load"]),
        float(record["chronic_load_28d"]),
        float(record["chronic_load_42d"]),
        acr,
        int(record["deload_flag"]),
        fatigue,
        readiness,
        coaching_flags_val,
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


# ---------------------------------------------------------------------------
# upsert_with_retry
# ---------------------------------------------------------------------------


class _PsycopgLike:
    """Structural type hint for objects that look like psycopg2 connections."""

    def cursor(self) -> Any: ...  # noqa: E704
    def commit(self) -> None: ...  # noqa: E704
    def rollback(self) -> None: ...  # noqa: E704
    def close(self) -> None: ...  # noqa: E704


def upsert_with_retry(
    record: dict,
    conn: Any,
    conn_factory: Callable[[], Any],
    max_retries: int = 3,
    base_backoff_s: float = 0.5,
) -> Any:
    """Execute one metrics UPSERT with exponential-backoff retry and reconnect.

    Extracted from _PgUpsertFn.process_element in jobs/metrics/main.py so the
    reconnect-on-OperationalError path is fully unit-testable without a Flink
    runtime or a real PostgreSQL instance.

    The function is intentionally pyflink-free and psycopg2-import-free at the
    module level: psycopg2 is imported lazily inside the function body.  Tests
    that use fake connections do not need psycopg2 installed.

    Args:
        record:         A metrics dict (same shape as build_upsert / execute_upsert).
        conn:           An open psycopg2-compatible connection.  The object is
                        mutated in-place: on OperationalError / InterfaceError
                        the stale connection is closed and ``conn`` is replaced
                        by a fresh connection from ``conn_factory``.  The *caller*
                        must store the returned connection to track the new object.
        conn_factory:   Zero-argument callable that returns a fresh psycopg2-
                        compatible connection.  Called automatically when the
                        current connection is detected as dead.
        max_retries:    Total number of attempts (default 3).  Must be >= 1.
        base_backoff_s: Base sleep interval in seconds for exponential back-off.
                        Sleep = base_backoff_s * 2^attempt before each retry.
                        Default 0.5 s → 0.5 / 1.0 / 2.0 s between the first
                        three attempts.

    Returns:
        The (possibly new) connection object after the final attempt.  Always
        return the connection so callers can update their reference even after
        a successful reconnect.

    Raises:
        Exception: Re-raises the last exception if all ``max_retries`` attempts
                   fail (including reconnect attempts).  The caller is responsible
                   for logging, incrementing counters, and deciding whether to drop
                   or DLQ the record.

    Design notes:
    - Dead-connection detection: psycopg2 sets conn.closed != 0 on any fatal
      connection error (server-side kill, network loss, idle-in-transaction
      timeout).  We also treat OperationalError and InterfaceError as signals
      that the connection may be broken, even when conn.closed == 0, because
      some proxy / PgBouncer configurations close the transport without
      updating the Python connection object.
    - Transaction management: we issue rollback() before reconnecting to
      release server-side resources.  On a dead connection rollback() may
      raise; we silently swallow that to ensure the reconnect proceeds.
    - Idempotency: the UPSERT SQL uses ON CONFLICT DO UPDATE, so replaying
      the same record after a transient failure is safe.
    """
    # Lazy import: psycopg2 is only available in the Flink runtime.
    # Unit tests that use fake connections bypass this import entirely.
    try:
        import psycopg2 as _psycopg2  # type: ignore[import]
        _OPERATIONAL_ERROR = _psycopg2.OperationalError
        _INTERFACE_ERROR = _psycopg2.InterfaceError
    except ImportError:
        # psycopg2 not installed (e.g. unit test environment).  Use Exception
        # as a fallback so the reconnect logic is still reachable via fakes.
        _OPERATIONAL_ERROR = Exception  # type: ignore[assignment,misc]
        _INTERFACE_ERROR = Exception  # type: ignore[assignment,misc]

    last_exc: BaseException | None = None
    for attempt in range(max_retries):
        try:
            cur = conn.cursor()
            execute_upsert(cur, record)
            conn.commit()
            cur.close()
            return conn
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            # Rollback any partial transaction; ignore rollback errors on
            # dead connections (the close / reconnect below clears them).
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            # Reconnect if the connection is dead or the error suggests it.
            conn_dead = (
                getattr(conn, "closed", 0) != 0
                or isinstance(exc, (_OPERATIONAL_ERROR, _INTERFACE_ERROR))
            )
            if conn_dead and attempt < max_retries - 1:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = conn_factory()
            if attempt < max_retries - 1:
                time.sleep(base_backoff_s * (2 ** attempt))

    assert last_exc is not None  # always set after at least one failed attempt
    raise last_exc
