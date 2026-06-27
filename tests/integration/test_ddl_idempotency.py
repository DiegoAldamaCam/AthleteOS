"""Integration tests for DDL idempotency — W3-9.

Verifies that running storage/postgres/ddl.sql twice against a live PostgreSQL
instance does not raise an error and preserves existing data.

Covers:
  W3-9: DDL idempotency — ADD COLUMN IF NOT EXISTS recovery_score + four DROP NOT NULL
        statements re-run successfully; existing rows are unaffected.

Docker-gated: skipped automatically when Docker daemon is not reachable.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.conftest import requires_docker

requires_docker()

try:
    import psycopg2
except ImportError:
    pytest.skip("psycopg2 not installed; DDL idempotency tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DDL path
# ---------------------------------------------------------------------------

_DDL_PATH = Path(__file__).resolve().parents[2] / "storage" / "postgres" / "ddl.sql"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_container(docker_ok):
    """A fresh PostgresContainer scoped to this test module."""
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


@pytest.fixture(scope="module")
def pg_conn(pg_dsn):
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_ddl(conn) -> None:
    """Execute the full DDL script against conn."""
    ddl_text = _DDL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(ddl_text)


# ---------------------------------------------------------------------------
# W3-9: DDL idempotency
# ---------------------------------------------------------------------------


def test_ddl_runs_twice_without_error(pg_conn):
    """W3-9 (part 1): Running the full DDL script twice must not raise any exception.

    The ADD COLUMN IF NOT EXISTS and DROP NOT NULL (idempotent on nullable col)
    guards ensure the second run is a no-op.
    """
    # First run — creates the table and adds all columns
    _run_ddl(pg_conn)

    # Second run — must succeed without error (idempotency check)
    _run_ddl(pg_conn)


def test_ddl_recovery_score_column_exists_after_ddl(pg_conn):
    """W3-9 (part 2): recovery_score column must exist and be nullable after DDL."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'athlete_metrics'
              AND column_name = 'recovery_score'
            """
        )
        row = cur.fetchone()
    assert row is not None, "recovery_score column must exist after DDL"
    col_name, is_nullable = row
    assert col_name == "recovery_score"
    assert is_nullable == "YES", (
        f"recovery_score must be nullable (IS_NULLABLE='YES'), got {is_nullable!r}"
    )


def test_ddl_load_columns_are_nullable_after_adr19(pg_conn):
    """W3-9 (part 3): ADR-19 DROP NOT NULL — the four load columns must be nullable."""
    target_cols = {"acute_load", "chronic_load_28d", "chronic_load_42d", "deload_flag"}
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'athlete_metrics'
              AND column_name = ANY(%s)
            """,
            (list(target_cols),),
        )
        rows = cur.fetchall()
    found = {row[0]: row[1] for row in rows}
    for col in target_cols:
        assert col in found, f"Column {col!r} not found in athlete_metrics"
        assert found[col] == "YES", (
            f"Column {col!r} must be nullable after ADR-19 DROP NOT NULL, "
            f"got is_nullable={found[col]!r}"
        )


def test_ddl_preserves_existing_data(pg_conn):
    """W3-9 (part 4): Running DDL twice preserves pre-existing rows.

    Seeds a row before the second run; that row must survive.
    """
    import datetime

    with pg_conn.cursor() as cur:
        # Insert a test row using partial INSERT (only recovery_score, no load cols)
        cur.execute(
            """
            INSERT INTO athlete_metrics (athlete_id, metric_date, recovery_score)
            VALUES (%s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO UPDATE
                SET recovery_score = EXCLUDED.recovery_score
            """,
            ("DDL_TEST_ATHLETE", datetime.date(2025, 6, 1), 77.5),
        )

    # Run DDL again — must preserve data
    _run_ddl(pg_conn)

    with pg_conn.cursor() as cur:
        cur.execute(
            "SELECT recovery_score FROM athlete_metrics "
            "WHERE athlete_id = %s AND metric_date = %s",
            ("DDL_TEST_ATHLETE", datetime.date(2025, 6, 1)),
        )
        row = cur.fetchone()

    assert row is not None, "Pre-existing row must survive DDL re-run"
    assert abs(row[0] - 77.5) < 0.01, (
        f"recovery_score must be preserved as 77.5 after DDL re-run, got {row[0]!r}"
    )
