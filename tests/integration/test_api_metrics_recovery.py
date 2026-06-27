"""Integration tests for recovery_score HTTP serialization via FastAPI router (W3-10, W3-11).

Verifies that the FastAPI metrics endpoint:
  W3-10: Returns recovery_score as a float in the JSON response when the DB row has it set.
  W3-11: Returns recovery_score as JSON null (not omitted) when the DB row has recovery_score=NULL.

These tests prove the HTTP/router serialization layer — model + response_model= wiring — not just
the Pydantic model in isolation (which is already covered in tests/unit/test_api_metrics_recovery.py).

Uses a real PostgresContainer + TestClient, mirroring the pattern in test_api_metrics.py.
Docker-gated: skipped automatically when Docker daemon is not reachable.
"""

from __future__ import annotations

import os
from datetime import date

import psycopg2
import pytest

from tests.conftest import requires_docker

requires_docker()

try:
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip("starlette not installed; API recovery tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DDL — must include recovery_score (ADR-19: nullable)
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
    PRIMARY KEY (athlete_id, metric_date)
);
"""

# ---------------------------------------------------------------------------
# Fixtures — isolated container so this module does not share state with
# test_api_metrics.py (which uses a different DDL without recovery_score)
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
        cur.execute(_CREATE_ATHLETE_METRICS)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def seeded_db(pg_conn) -> None:
    """Seed two rows: one with recovery_score=73.29, one with recovery_score=NULL."""
    with pg_conn.cursor() as cur:
        # W3-10 row: recovery_score present
        cur.execute(
            """
            INSERT INTO athlete_metrics
                (athlete_id, metric_date, recovery_score)
            VALUES (%s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO UPDATE
                SET recovery_score = EXCLUDED.recovery_score
            """,
            ("RS_ATHLETE_W3_10", date(2025, 5, 1), 73.29),
        )
        # W3-11 row: recovery_score NULL (wellness-only row, no load data yet)
        cur.execute(
            """
            INSERT INTO athlete_metrics
                (athlete_id, metric_date, recovery_score)
            VALUES (%s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO UPDATE
                SET recovery_score = EXCLUDED.recovery_score
            """,
            ("RS_ATHLETE_W3_11", date(2025, 5, 2), None),
        )


@pytest.fixture(scope="module")
def api_client(pg_dsn, seeded_db):
    """TestClient with DATABASE_URL pointed at the test container.

    Reloads api modules to pick up the fresh DATABASE_URL even when another
    integration module has already imported api.config in this session.
    """
    _env_keys = ("DATABASE_URL", "CORS_ORIGINS", "KAFKA_BOOTSTRAP_SERVERS")
    _env_backup = {k: os.environ.get(k) for k in _env_keys}

    os.environ["DATABASE_URL"] = pg_dsn
    os.environ["CORS_ORIGINS"] = "http://localhost:5173"
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"

    import importlib

    try:
        import api.config as _cfg
        importlib.reload(_cfg)
        import api.db as _db
        importlib.reload(_db)
        import api.routers.metrics as _rm
        importlib.reload(_rm)
        import api.main as _main
        importlib.reload(_main)
    except (ImportError, AttributeError):
        pass

    from api.main import app  # noqa: PLC0415

    with TestClient(app) as client:
        yield client

    for key, value in _env_backup.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


# ---------------------------------------------------------------------------
# W3-10: GET /athletes/{id}/metrics returns recovery_score as float in JSON
# ---------------------------------------------------------------------------


def test_w3_10_get_metrics_returns_recovery_score_as_float(api_client):
    """W3-10: A DB row with recovery_score=73.29 → GET returns 200 with recovery_score≈73.29.

    Proves the FastAPI router serializes recovery_score through the full HTTP stack
    (response_model=list[MetricRow], JSON encoding) — not just the Pydantic model.
    """
    resp = api_client.get("/athletes/RS_ATHLETE_W3_10/metrics?from=2025-05-01&to=2025-05-01")
    assert resp.status_code == 200, (
        f"W3-10: Expected 200, got {resp.status_code}: {resp.text}"
    )
    data = resp.json()
    assert len(data) == 1, f"W3-10: Expected 1 row, got {len(data)}: {data}"
    row = data[0]

    assert "recovery_score" in row, (
        "W3-10: recovery_score field must be present in JSON response "
        "(FastAPI response_model must not omit it)"
    )
    assert row["recovery_score"] is not None, (
        f"W3-10: recovery_score must be a float, got None"
    )
    assert abs(row["recovery_score"] - 73.29) < 0.01, (
        f"W3-10: Expected recovery_score≈73.29, got {row['recovery_score']!r}"
    )


# ---------------------------------------------------------------------------
# W3-11: GET /athletes/{id}/metrics returns recovery_score as JSON null when NULL
# ---------------------------------------------------------------------------


def test_w3_11_get_metrics_returns_recovery_score_as_null(api_client):
    """W3-11: A DB row with recovery_score=NULL → response JSON has recovery_score: null.

    Proves FastAPI does NOT exclude None fields from the response (response_model_exclude_none
    is not set on this router). recovery_score must appear as null, not be omitted.
    """
    resp = api_client.get("/athletes/RS_ATHLETE_W3_11/metrics?from=2025-05-02&to=2025-05-02")
    assert resp.status_code == 200, (
        f"W3-11: Expected 200, got {resp.status_code}: {resp.text}"
    )
    data = resp.json()
    assert len(data) == 1, f"W3-11: Expected 1 row, got {len(data)}: {data}"
    row = data[0]

    assert "recovery_score" in row, (
        "W3-11: recovery_score field must be present in JSON response even when NULL "
        "(must not be omitted by response_model_exclude_none or similar)"
    )
    assert row["recovery_score"] is None, (
        f"W3-11: recovery_score=NULL in DB must appear as JSON null, "
        f"got {row['recovery_score']!r}"
    )
