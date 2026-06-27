"""Integration tests for GET /athletes/{id}/metrics — Domain A (7 scenarios).

Spec source: obs #65 (sdd/athleteos-phase7-web/spec), Domain A.
Design source: obs #66 (sdd/athleteos-phase7-web/design), Backend Design.

Uses a throwaway PostgresContainer seeded with `athlete_metrics` rows.
The FastAPI app is tested via httpx.AsyncClient (ASGI transport) so no real
HTTP port is needed and no Docker network is required for the app itself.

Docker-gated: skipped automatically when Docker daemon is not reachable.

All 7 spec scenarios covered:
  S1  Happy path — range with data (10 rows, ascending order)
  S2  Default range — last 90 days (no from/to supplied)
  S3  Athlete exists, no rows in range → 200 []
  S4  Sparse series — 2 rows with gap (no date fill)
  S5  Unknown athlete → 404
  S6  Invalid date format → 422
  S7  from > to → 422
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Generator

import psycopg2
import pytest

# ---------------------------------------------------------------------------
# Docker gate — skip module if Docker unavailable
# ---------------------------------------------------------------------------
from tests.conftest import requires_docker

requires_docker()

# ---------------------------------------------------------------------------
# Lazy imports for ASGI test client (httpx + starlette)
# ---------------------------------------------------------------------------
try:
    import httpx
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip("httpx / starlette not installed; API metrics tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DDL for the metrics serving table (matches Phase 6 PG sink schema)
# ---------------------------------------------------------------------------
_CREATE_ATHLETE_METRICS = """
CREATE TABLE IF NOT EXISTS athlete_metrics (
    athlete_id       TEXT        NOT NULL,
    metric_date      DATE        NOT NULL,
    acute_load       NUMERIC,
    chronic_load_28d NUMERIC,
    chronic_load_42d NUMERIC,
    acute_chronic_ratio NUMERIC,
    deload_flag      SMALLINT,
    PRIMARY KEY (athlete_id, metric_date)
);
"""

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
    """psycopg2 DSN from the container."""
    return pg_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://").replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
def pg_conn(pg_dsn):
    """A live psycopg2 connection to seed data."""
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    # Create schema once for the module
    with conn.cursor() as cur:
        cur.execute(_CREATE_ATHLETE_METRICS)
    yield conn
    conn.close()


def _insert_row(conn, athlete_id: str, metric_date: date, acute_load: float = 100.0) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO athlete_metrics
                (athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
                 acute_chronic_ratio, deload_flag)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO NOTHING
            """,
            (athlete_id, metric_date, acute_load, acute_load * 0.9, acute_load * 0.85,
             round(acute_load / (acute_load * 0.9), 4) if acute_load * 0.9 else None, 0),
        )


@pytest.fixture(scope="module")
def seeded_db(pg_conn) -> dict:
    """Seed athlete_metrics and return metadata for assertions."""
    # S1 / S3 / S4 athlete: A1
    # 10 rows from 2025-01-01 to 2025-01-10
    for i in range(10):
        _insert_row(pg_conn, "A1", date(2025, 1, 1) + timedelta(days=i))

    # S4 sparse athlete: A2 — only Jan 1 and Jan 5 (gap Jan 2-4)
    _insert_row(pg_conn, "A2", date(2025, 1, 1))
    _insert_row(pg_conn, "A2", date(2025, 1, 5))

    # S2 default-range athlete: A3 — one row within last 90 days
    recent_date = date.today() - timedelta(days=10)
    _insert_row(pg_conn, "A3", recent_date)

    return {"happy_athlete": "A1", "sparse_athlete": "A2", "recent_athlete": "A3"}


@pytest.fixture(scope="module")
def api_client(pg_dsn, seeded_db):
    """TestClient wrapping the FastAPI app, with DATABASE_URL injected.

    Reloads api.config / api.main so the app's cached Settings pick up THIS
    module's DATABASE_URL even when another integration module (e.g. test_api_dlq)
    ran earlier in the same session and left a stale config module cached.
    Restores the prior env on teardown to avoid leaking into later modules.
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
# S1 — Happy path: range with data
# ---------------------------------------------------------------------------


def test_happy_path_returns_10_rows_in_order(api_client, seeded_db):
    """S1: 10 rows in ascending metric_date order, all 7 fields present."""
    resp = api_client.get("/athletes/A1/metrics?from=2025-01-01&to=2025-01-10")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 10
    # Ascending order
    dates = [row["metric_date"] for row in data]
    assert dates == sorted(dates)
    # All 7 fields in every row
    required_fields = {
        "athlete_id", "metric_date", "acute_load",
        "chronic_load_28d", "chronic_load_42d",
        "acute_chronic_ratio", "deload_flag",
    }
    for row in data:
        assert required_fields.issubset(row.keys()), f"Missing fields in row: {row}"
    # athlete_id matches
    assert all(row["athlete_id"] == "A1" for row in data)


# ---------------------------------------------------------------------------
# S2 — Default range: last 90 days (no from/to)
# ---------------------------------------------------------------------------


def test_default_range_returns_rows_within_90_days(api_client, seeded_db):
    """S2: No from/to supplied → rows within last 90 days, 200 response."""
    resp = api_client.get("/athletes/A3/metrics")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    # All returned dates must be within last 90 days
    today = date.today()
    cutoff = today - timedelta(days=90)
    for row in data:
        row_date = date.fromisoformat(row["metric_date"])
        assert cutoff <= row_date <= today, f"Date {row_date} outside default 90d window"


# ---------------------------------------------------------------------------
# S3 — Athlete exists, no rows in requested range → 200 []
# ---------------------------------------------------------------------------


def test_athlete_exists_no_rows_in_range_returns_empty_array(api_client, seeded_db):
    """S3: A1 exists but has no rows in year 2020 → 200 with []."""
    resp = api_client.get("/athletes/A1/metrics?from=2020-01-01&to=2020-12-31")
    assert resp.status_code == 200
    data = resp.json()
    assert data == [], f"Expected empty array, got: {data}"


# ---------------------------------------------------------------------------
# S4 — Sparse series: only present rows returned (no fill)
# ---------------------------------------------------------------------------


def test_sparse_series_returns_only_existing_rows(api_client, seeded_db):
    """S4: A2 has rows on Jan 1 and Jan 5 only → exactly 2 rows returned."""
    resp = api_client.get("/athletes/A2/metrics?from=2025-01-01&to=2025-01-05")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2, f"Expected 2 rows (sparse, no fill), got {len(data)}: {data}"
    dates = [row["metric_date"] for row in data]
    assert "2025-01-01" in dates
    assert "2025-01-05" in dates


# ---------------------------------------------------------------------------
# S5 — Unknown athlete → 404
# ---------------------------------------------------------------------------


def test_unknown_athlete_returns_404(api_client):
    """S5: No athlete with id UNKNOWN → HTTP 404."""
    resp = api_client.get("/athletes/UNKNOWN/metrics")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# S6 — Invalid date format → 422
# ---------------------------------------------------------------------------


def test_invalid_date_format_returns_422(api_client):
    """S6: from=not-a-date → HTTP 422 with structured error body."""
    resp = api_client.get("/athletes/A1/metrics?from=not-a-date")
    assert resp.status_code == 422
    body = resp.json()
    # FastAPI wraps validation errors in {"detail": [...]}
    assert "detail" in body, f"Expected 'detail' key in 422 body, got: {body}"


# ---------------------------------------------------------------------------
# S7 — from > to → 422
# ---------------------------------------------------------------------------


def test_from_after_to_returns_422(api_client):
    """S7: from=2025-12-31 > to=2025-01-01 → HTTP 422 with structured error body."""
    resp = api_client.get("/athletes/A1/metrics?from=2025-12-31&to=2025-01-01")
    assert resp.status_code == 422
    body = resp.json()
    assert "detail" in body, f"Expected 'detail' key in 422 body, got: {body}"
