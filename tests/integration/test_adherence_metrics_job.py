"""Integration tests for the adherence_metrics job (ADH-J1..J5).

These tests exercise the full run() pipeline against real PostgreSQL + a
local Iceberg warehouse written via DuckDB-backed Parquet files.

Skip conditions (clean SKIP, not error):
  1. Docker daemon not reachable → skip (pg container cannot start)
  2. duckdb not installed → skip (no Iceberg read path)
  3. pyarrow not installed → skip (Parquet write needs pyarrow)

On Python 3.14 without pyarrow installed locally, all tests here will cleanly
skip. CI (Python 3.11 + full deps) runs them for real.
"""

from __future__ import annotations

import datetime
import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Dependency gates — clean SKIP when deps are absent
# ---------------------------------------------------------------------------

try:
    import duckdb as _duckdb_mod  # noqa: F401
    _duckdb_available = True
except ImportError:
    _duckdb_available = False

try:
    import pyarrow as _pyarrow_mod  # noqa: F401
    _pyarrow_available = True
except ImportError:
    _pyarrow_available = False

_DEPS_AVAILABLE = _duckdb_available and _pyarrow_available

# Docker gate
from tests.conftest import requires_docker

requires_docker()

if not _DEPS_AVAILABLE:
    pytest.skip(
        "duckdb and/or pyarrow not installed; adherence metrics job integration tests skipped",
        allow_module_level=True,
    )

try:
    import psycopg2
except ImportError:
    pytest.skip("psycopg2 not installed; adherence metrics job tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Imports (only reached when deps are available)
# ---------------------------------------------------------------------------

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# DDL for the athlete_metrics table (matches post-ADH1 schema)
# ---------------------------------------------------------------------------

_CREATE_ATHLETE_METRICS = """
CREATE TABLE IF NOT EXISTS athlete_metrics (
    athlete_id          TEXT        NOT NULL,
    metric_date         DATE        NOT NULL,
    acute_load          FLOAT       NULL,
    chronic_load_28d    FLOAT       NULL,
    chronic_load_42d    FLOAT       NULL,
    acute_chronic_ratio FLOAT       NULL,
    deload_flag         INT         NULL,
    fatigue_score       FLOAT       NULL,
    readiness_score     FLOAT       NULL,
    coaching_flags      TEXT        NULL,
    recovery_score      FLOAT       NULL,
    adherence_score     FLOAT       NULL,
    PRIMARY KEY (athlete_id, metric_date)
);
"""

_CREATE_PLANNING_BLOCKS = """
CREATE TABLE IF NOT EXISTS planning_blocks (
    athlete_id                  TEXT        NOT NULL,
    block_id                    TEXT        NOT NULL,
    ingest_time                 BIGINT      NOT NULL,
    goal                        TEXT,
    start_date                  DATE        NOT NULL,
    end_date                    DATE        NOT NULL,
    planned_sessions_per_week   INT         NOT NULL DEFAULT 0,
    weekly_volume_targets       TEXT,
    PRIMARY KEY (athlete_id, block_id, ingest_time)
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EPOCH_MS_PER_DAY = 86_400_000


def _date_to_ms(d: datetime.date) -> int:
    """Convert a date to UTC midnight epoch-ms."""
    return int(datetime.datetime(d.year, d.month, d.day, tzinfo=datetime.timezone.utc).timestamp() * 1000)


def _write_iceberg_parquet(
    warehouse_path: Path,
    events: list[dict],
) -> None:
    """Write training events as Parquet files in a warehouse-like directory tree.

    Mimics the Iceberg layout: warehouse/canonical.db/training_event/data/*.parquet
    The read_training_events glob finds *.parquet under warehouse_path recursively.
    """
    table_dir = warehouse_path / "canonical.db" / "training_event" / "data"
    table_dir.mkdir(parents=True, exist_ok=True)

    schema = pa.schema([
        pa.field("athlete_id", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("event_time", pa.int64()),
        pa.field("event_type", pa.string()),
        pa.field("session_load", pa.float64()),
    ])

    table = pa.table(
        {
            "athlete_id": [e["athlete_id"] for e in events],
            "event_id": [e["event_id"] for e in events],
            "event_time": [e["event_time"] for e in events],
            "event_type": [e.get("event_type", "training") for e in events],
            "session_load": [e["session_load"] for e in events],
        },
        schema=schema,
    )
    pq.write_table(table, table_dir / "part-0.parquet")


def _insert_planning_block(
    conn: Any,
    athlete_id: str,
    block_id: str,
    ingest_time: int,
    start_date: datetime.date,
    end_date: datetime.date,
    planned_sessions_per_week: int,
    weekly_volume_targets: str = "{}",
    goal: str = "test",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO planning_blocks
                (athlete_id, block_id, ingest_time, goal, start_date, end_date,
                 planned_sessions_per_week, weekly_volume_targets)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (athlete_id, block_id, ingest_time) DO NOTHING
            """,
            (athlete_id, block_id, ingest_time, goal,
             start_date, end_date, planned_sessions_per_week, weekly_volume_targets),
        )


def _get_adherence_score(conn: Any, athlete_id: str, metric_date: datetime.date) -> Any:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT adherence_score FROM athlete_metrics WHERE athlete_id = %s AND metric_date = %s",
            (athlete_id, metric_date),
        )
        row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_container(docker_ok):
    if not docker_ok:
        pytest.skip("Docker not available")
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:16")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def pg_dsn(pg_container) -> str:
    return pg_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="function")
def pg_conn(pg_dsn):
    """Per-test connection with fresh tables."""
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS athlete_metrics")
        cur.execute("DROP TABLE IF EXISTS planning_blocks")
        cur.execute(_CREATE_ATHLETE_METRICS)
        cur.execute(_CREATE_PLANNING_BLOCKS)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def warehouse(tmp_path) -> Path:
    """Fresh temporary warehouse directory per test."""
    return tmp_path / "warehouse"


# ---------------------------------------------------------------------------
# ADH-J1: E4 — multiple plan revisions, latest ingest_time used
# ---------------------------------------------------------------------------


def test_adh_j1_latest_ingest_time_wins(pg_conn, warehouse, pg_dsn):
    """ADH-J1: When two planning_blocks rows exist for (A1, BLK-001),
    run() MUST use the one with the highest ingest_time (planned_sessions_per_week=5).
    """
    today = datetime.date(2026, 6, 28)
    start = datetime.date(2025, 6, 1)
    end = datetime.date(2025, 6, 28)  # end_date in the past → metric_date = end

    # Revision 1 (stale): planned_sessions_per_week=3
    _insert_planning_block(pg_conn, "A1", "BLK-001", 1000, start, end, 3)
    # Revision 2 (latest): planned_sessions_per_week=5
    _insert_planning_block(pg_conn, "A1", "BLK-001", 2000, start, end, 5)

    # 5 training events on 5 distinct days within the block
    events = [
        {"athlete_id": "A1", "event_id": f"e{i}", "event_time": _date_to_ms(start) + i * _EPOCH_MS_PER_DAY, "session_load": 50.0}
        for i in range(5)
    ]
    _write_iceberg_parquet(warehouse, events)

    from jobs.adherence_metrics.main import AdherenceJobConfig, run
    cfg = AdherenceJobConfig(pg_dsn=pg_dsn, warehouse_path=str(warehouse), today_date=today)
    run(cfg)

    metric_date = min(end, today)  # end < today → metric_date = end
    score = _get_adherence_score(pg_conn, "A1", metric_date)
    assert score is not None, "ADH-J1: adherence_score must be written for A1"

    # With revision 2 (psw=5): block is 28 days → ceil(28/7)=4 weeks
    # planned_sessions = 5×4 = 20
    # actual_sessions = 5 distinct days
    # no volume targets → sessions-only: min(5/20, 1.0) = 0.25
    assert abs(score - 0.25) < 0.01, (
        f"ADH-J1: with latest revision (psw=5), expected score≈0.25 (5/20), got {score}"
    )


# ---------------------------------------------------------------------------
# ADH-J2: metric_date — completed block uses end_date
# ---------------------------------------------------------------------------


def test_adh_j2_metric_date_is_end_date_when_block_complete(pg_conn, warehouse, pg_dsn):
    """ADH-J2: A block with end_date in the past → metric_date = end_date."""
    today = datetime.date(2026, 6, 28)
    start = datetime.date(2025, 5, 1)
    end = datetime.date(2025, 5, 28)  # past end_date

    _insert_planning_block(pg_conn, "A1", "BLK-002", 1000, start, end, 4)

    events = [
        {"athlete_id": "A1", "event_id": "e0", "event_time": _date_to_ms(start), "session_load": 60.0},
    ]
    _write_iceberg_parquet(warehouse, events)

    from jobs.adherence_metrics.main import AdherenceJobConfig, run
    cfg = AdherenceJobConfig(pg_dsn=pg_dsn, warehouse_path=str(warehouse), today_date=today)
    run(cfg)

    # MUST be upserted at end_date (2025-05-28), NOT today
    score_at_end = _get_adherence_score(pg_conn, "A1", end)
    score_at_today = _get_adherence_score(pg_conn, "A1", today)

    assert score_at_end is not None, f"ADH-J2: must upsert at metric_date=end_date ({end})"
    assert score_at_today is None, f"ADH-J2: must NOT upsert at today ({today})"


# ---------------------------------------------------------------------------
# ADH-J3: metric_date — in-progress block uses today
# ---------------------------------------------------------------------------


def test_adh_j3_metric_date_is_today_when_block_in_progress(pg_conn, warehouse, pg_dsn):
    """ADH-J3: A block with end_date in the future → metric_date = today."""
    today = datetime.date(2026, 6, 28)
    start = datetime.date(2026, 1, 1)
    end = datetime.date(2026, 12, 31)  # future end_date

    _insert_planning_block(pg_conn, "A1", "BLK-003", 1000, start, end, 3)

    events = [
        {"athlete_id": "A1", "event_id": "e0", "event_time": _date_to_ms(start), "session_load": 50.0},
    ]
    _write_iceberg_parquet(warehouse, events)

    from jobs.adherence_metrics.main import AdherenceJobConfig, run
    cfg = AdherenceJobConfig(pg_dsn=pg_dsn, warehouse_path=str(warehouse), today_date=today)
    run(cfg)

    # MUST be upserted at today, NOT at end_date
    score_at_today = _get_adherence_score(pg_conn, "A1", today)
    score_at_end = _get_adherence_score(pg_conn, "A1", end)

    assert score_at_today is not None, f"ADH-J3: must upsert at metric_date=today ({today})"
    assert score_at_end is None, f"ADH-J3: must NOT upsert at end_date ({end})"


# ---------------------------------------------------------------------------
# ADH-J4: E2 — no plan → no UPSERT
# ---------------------------------------------------------------------------


def test_adh_j4_no_plan_means_no_upsert(pg_conn, warehouse, pg_dsn):
    """ADH-J4: Athlete A2 has no planning_blocks rows → no adherence_score written."""
    today = datetime.date(2026, 6, 28)

    # A1 has a block (so the job is not trivially a no-op)
    _insert_planning_block(pg_conn, "A1", "BLK-004", 1000,
                           datetime.date(2025, 6, 1), datetime.date(2025, 6, 28), 4)

    events = [
        {"athlete_id": "A2", "event_id": "e0", "event_time": _date_to_ms(datetime.date(2025, 6, 1)), "session_load": 50.0},
    ]
    _write_iceberg_parquet(warehouse, events)

    from jobs.adherence_metrics.main import AdherenceJobConfig, run
    cfg = AdherenceJobConfig(pg_dsn=pg_dsn, warehouse_path=str(warehouse), today_date=today)
    run(cfg)

    # A2 has no plan → no UPSERT issued
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM athlete_metrics WHERE athlete_id = %s", ("A2",))
        count = cur.fetchone()[0]

    assert count == 0, f"ADH-J4: A2 has no plan → 0 rows in athlete_metrics, got {count}"


# ---------------------------------------------------------------------------
# ADH-J5: End-to-end happy path — score ≈ 0.1875
# ---------------------------------------------------------------------------


def test_adh_j5_end_to_end_happy_path(pg_conn, warehouse, pg_dsn):
    """ADH-J5: Full pipeline with known inputs → adherence_score ≈ 0.1875.

    Planning block:
      planned_sessions_per_week=4, weekly_volume_targets='{"strength": 200}',
      start_date=2025-06-01, end_date=2025-06-28 (4 weeks exact)
    Iceberg events:
      3 sessions on 3 distinct days, session_load=50.0 each

    Computed:
      block_weeks = ceil(28/7) = 4
      planned_sessions = 4×4 = 16
      target_volume = 200×4 = 800
      actual_sessions = 3
      actual_volume = 150.0
      score = 0.5×min(3/16,1.0) + 0.5×min(150/800,1.0)
            = 0.5×0.1875 + 0.5×0.1875
            = 0.1875
    """
    today = datetime.date(2026, 6, 28)
    start = datetime.date(2025, 6, 1)
    end = datetime.date(2025, 6, 28)

    _insert_planning_block(
        pg_conn, "A1", "BLK-005", 1000,
        start, end,
        planned_sessions_per_week=4,
        weekly_volume_targets='{"strength": 200}',
    )

    events = [
        {"athlete_id": "A1", "event_id": "e0", "event_time": _date_to_ms(datetime.date(2025, 6, 3)), "session_load": 50.0},
        {"athlete_id": "A1", "event_id": "e1", "event_time": _date_to_ms(datetime.date(2025, 6, 10)), "session_load": 50.0},
        {"athlete_id": "A1", "event_id": "e2", "event_time": _date_to_ms(datetime.date(2025, 6, 17)), "session_load": 50.0},
    ]
    _write_iceberg_parquet(warehouse, events)

    from jobs.adherence_metrics.main import AdherenceJobConfig, run
    cfg = AdherenceJobConfig(pg_dsn=pg_dsn, warehouse_path=str(warehouse), today_date=today)
    run(cfg)

    metric_date = end  # end_date in the past → metric_date = end_date
    score = _get_adherence_score(pg_conn, "A1", metric_date)

    assert score is not None, "ADH-J5: adherence_score must be written"
    assert abs(score - 0.1875) < 0.001, (
        f"ADH-J5: expected adherence_score≈0.1875, got {score}"
    )
