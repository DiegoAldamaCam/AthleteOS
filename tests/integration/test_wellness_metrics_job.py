"""Integration tests for wellness_metrics job UPSERT logic (W3-7, W3-8, W3-FIRSTWRITE).

These tests exercise the PostgreSQL UPSERT behavior directly (no Flink runtime):
they seed rows with psycopg2, call build_recovery_upsert / build_upsert via
upsert_with_retry, and assert column non-overlap and first-write correctness.

W3-7:  Pre-seeded row with fatigue_score=75; recovery UPSERT=80 → fatigue intact.
W3-8:  Pre-seeded row with recovery_score=80; load UPSERT fatigue=72 → recovery intact.
W3-FIRSTWRITE: NO prior row; recovery UPSERT=73.29 → row created with load cols NULL.

Docker-gated: skipped when Docker daemon is not reachable.
"""

from __future__ import annotations

import datetime

import pytest

from tests.conftest import requires_docker

requires_docker()

try:
    import psycopg2
except ImportError:
    pytest.skip("psycopg2 not installed; wellness metrics job tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Local DDL — creates athlete_metrics with ADR-19 column schema
# (load columns nullable so W3-FIRSTWRITE can INSERT without them)
# ---------------------------------------------------------------------------

_CREATE_TABLE = """
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
    PRIMARY KEY (athlete_id, metric_date)
);
"""


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


@pytest.fixture(scope="module")
def pg_conn(pg_dsn):
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DATE_W3_7 = datetime.date(2025, 3, 10)
_DATE_W3_8 = datetime.date(2025, 3, 11)
_DATE_FIRSTWRITE = datetime.date(2025, 3, 12)

# epoch_ms for each test date (UTC midnight)
_EPOCH_W3_7 = int(datetime.datetime(_DATE_W3_7.year, _DATE_W3_7.month, _DATE_W3_7.day,
                                     tzinfo=datetime.timezone.utc).timestamp() * 1000)
_EPOCH_W3_8 = int(datetime.datetime(_DATE_W3_8.year, _DATE_W3_8.month, _DATE_W3_8.day,
                                     tzinfo=datetime.timezone.utc).timestamp() * 1000)
_EPOCH_FIRSTWRITE = int(datetime.datetime(_DATE_FIRSTWRITE.year, _DATE_FIRSTWRITE.month,
                                           _DATE_FIRSTWRITE.day,
                                           tzinfo=datetime.timezone.utc).timestamp() * 1000)


def _seed_full_row(conn, athlete_id: str, metric_date: datetime.date,
                   fatigue_score: float = 75.0, recovery_score: float = None) -> None:
    """Insert a full load-metrics row (all load columns populated)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO athlete_metrics
                (athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
                 acute_chronic_ratio, deload_flag, fatigue_score, readiness_score,
                 coaching_flags, recovery_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO NOTHING
            """,
            (athlete_id, metric_date, 100.0, 90.0, 85.0, 1.1, 0,
             fatigue_score, 60.0, "[]", recovery_score),
        )


def _do_recovery_upsert(conn, athlete_id: str, epoch_ms: int, recovery_score: float) -> None:
    """Execute the recovery-score UPSERT via build_recovery_upsert + psycopg2."""
    from storage.postgres.sink import build_recovery_upsert, upsert_with_retry

    record = {
        "athlete_id": athlete_id,
        "metric_date": epoch_ms,
        "recovery_score": recovery_score,
    }
    upsert_with_retry(record, conn, lambda: psycopg2.connect(conn.dsn),
                      max_retries=1, base_backoff_s=0.0)


def _do_load_upsert(conn, athlete_id: str, epoch_ms: int, fatigue_score: float) -> None:
    """Execute the load-metrics UPSERT via build_upsert + psycopg2."""
    from storage.postgres.sink import build_upsert, upsert_with_retry

    record = {
        "athlete_id": athlete_id,
        "metric_date": epoch_ms,
        "acute_load": 100.0,
        "chronic_load_28d": 90.0,
        "chronic_load_42d": 85.0,
        "acute_chronic_ratio": 1.1,
        "deload_flag": 0,
        "fatigue_score": fatigue_score,
        "readiness_score": 60.0,
        "coaching_flags": "[]",
    }

    # Wrap build_upsert so upsert_with_retry calls it via execute_upsert (load path)
    # We call upsert_with_retry with the load record — it calls build_upsert internally
    upsert_with_retry(record, conn, lambda: psycopg2.connect(conn.dsn),
                      max_retries=1, base_backoff_s=0.0)


def _fetch_row(conn, athlete_id: str, metric_date: datetime.date) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT athlete_id, metric_date, acute_load, chronic_load_28d,
                   chronic_load_42d, deload_flag, fatigue_score, recovery_score
            FROM athlete_metrics
            WHERE athlete_id = %s AND metric_date = %s
            """,
            (athlete_id, metric_date),
        )
        row = cur.fetchone()
    if row is None:
        return None
    keys = ["athlete_id", "metric_date", "acute_load", "chronic_load_28d",
            "chronic_load_42d", "deload_flag", "fatigue_score", "recovery_score"]
    return dict(zip(keys, row))


# ---------------------------------------------------------------------------
# W3-7: recovery UPSERT does NOT overwrite fatigue_score
# ---------------------------------------------------------------------------


def test_recovery_upsert_preserves_fatigue_score(pg_conn):
    """W3-7: Pre-seed row with fatigue_score=75.0; UPSERT recovery=80.0 → fatigue intact.

    Proves the recovery UPSERT's DO UPDATE SET only touches recovery_score.
    """
    athlete_id = "W3_7_ATHLETE"
    _seed_full_row(pg_conn, athlete_id, _DATE_W3_7, fatigue_score=75.0)

    _do_recovery_upsert(pg_conn, athlete_id, _EPOCH_W3_7, recovery_score=80.0)

    row = _fetch_row(pg_conn, athlete_id, _DATE_W3_7)
    assert row is not None, "Row must exist after recovery UPSERT"
    assert abs(row["recovery_score"] - 80.0) < 0.01, (
        f"recovery_score must be 80.0, got {row['recovery_score']!r}"
    )
    assert abs(row["fatigue_score"] - 75.0) < 0.01, (
        f"fatigue_score must remain 75.0 after recovery UPSERT, got {row['fatigue_score']!r} "
        "(W3-7: recovery UPSERT must not clobber fatigue_score)"
    )


# ---------------------------------------------------------------------------
# W3-8: load UPSERT does NOT overwrite recovery_score
# ---------------------------------------------------------------------------


def test_load_upsert_preserves_recovery_score(pg_conn):
    """W3-8: Pre-seed row with recovery_score=80.0; load UPSERT fatigue=72.0 → recovery intact.

    Proves the load UPSERT's DO UPDATE SET never names recovery_score.
    """
    athlete_id = "W3_8_ATHLETE"
    _seed_full_row(pg_conn, athlete_id, _DATE_W3_8, fatigue_score=60.0, recovery_score=80.0)

    _do_load_upsert(pg_conn, athlete_id, _EPOCH_W3_8, fatigue_score=72.0)

    row = _fetch_row(pg_conn, athlete_id, _DATE_W3_8)
    assert row is not None
    assert abs(row["fatigue_score"] - 72.0) < 0.01, (
        f"fatigue_score must be updated to 72.0, got {row['fatigue_score']!r}"
    )
    assert abs(row["recovery_score"] - 80.0) < 0.01, (
        f"recovery_score must remain 80.0 after load UPSERT, got {row['recovery_score']!r} "
        "(W3-8: load UPSERT must not clobber recovery_score)"
    )


# ---------------------------------------------------------------------------
# W3-FIRSTWRITE (CRITICAL): recovery UPSERT creates row with load cols NULL
# ---------------------------------------------------------------------------


def test_recovery_first_write_no_prior_load_row(pg_conn):
    """W3-FIRSTWRITE: NO pre-existing row; UPSERT recovery=73.29 → row created.

    This is the ADR-19 validation test: without DROP NOT NULL on the four load
    columns, this INSERT would raise NotNullViolation BEFORE ON CONFLICT is
    evaluated. With ADR-19, the INSERT succeeds and load columns default to NULL.

    Asserts:
      - Row is created (was not there before)
      - recovery_score == 73.29 ± 0.01
      - acute_load IS NULL
      - chronic_load_28d IS NULL
      - chronic_load_42d IS NULL
      - deload_flag IS NULL
    """
    athlete_id = "W3_FIRSTWRITE_ATHLETE"

    # Confirm no prior row for this PK
    pre_row = _fetch_row(pg_conn, athlete_id, _DATE_FIRSTWRITE)
    assert pre_row is None, (
        "Precondition violated: row already exists for W3-FIRSTWRITE athlete. "
        "Use a unique athlete_id or clean up before the test."
    )

    # Recovery UPSERT — no load row pre-seeded (the whole point of ADR-19)
    _do_recovery_upsert(pg_conn, athlete_id, _EPOCH_FIRSTWRITE, recovery_score=73.29)

    row = _fetch_row(pg_conn, athlete_id, _DATE_FIRSTWRITE)
    assert row is not None, (
        "W3-FIRSTWRITE FAILED: no row created after recovery UPSERT. "
        "Possible NotNullViolation — check that ADR-19 DROP NOT NULL has been applied."
    )

    assert abs(row["recovery_score"] - 73.29) < 0.01, (
        f"recovery_score must be 73.29 ± 0.01, got {row['recovery_score']!r}"
    )
    assert row["acute_load"] is None, (
        f"acute_load must be NULL (ADR-19: no load row), got {row['acute_load']!r}"
    )
    assert row["chronic_load_28d"] is None, (
        f"chronic_load_28d must be NULL, got {row['chronic_load_28d']!r}"
    )
    assert row["chronic_load_42d"] is None, (
        f"chronic_load_42d must be NULL, got {row['chronic_load_42d']!r}"
    )
    assert row["deload_flag"] is None, (
        f"deload_flag must be NULL, got {row['deload_flag']!r}"
    )
