"""Integration tests for GET /athletes — sc-1.1..sc-1.5.

Spec: obs #255, athletes-list-api specification.
Design: obs #256 — endpoint, fixture chain, importlib.reload pattern.

Uses a module-scoped PostgresContainer (postgres:16) — NOT redpanda/kafka.
Mirrors tests/integration/test_api_metrics.py exactly:
  - same fixture chain: pg_container → pg_dsn → pg_conn → seeded_db → api_client
  - same requires_docker() guard
  - same importlib.reload pattern (api.config / api.db / api.routers.athletes / api.main)

Docker-gated: skipped automatically when Docker daemon is not reachable.

Scenarios covered:
  sc-1.1  Happy path — data present, sorted
  sc-1.2  Alphabetical ordering enforced (insert order A3,A1,A2 → response [A1,A2,A3])
  sc-1.3  Empty database → HTTP 200 + {"athletes": []}
  sc-1.4  Response shape — exactly one top-level key "athletes", value is list[str]
"""

from __future__ import annotations

import os
from typing import Generator

import psycopg2
import pytest

# ---------------------------------------------------------------------------
# Docker gate — skip module if Docker unavailable
# ---------------------------------------------------------------------------
from tests.conftest import requires_docker

requires_docker()

# ---------------------------------------------------------------------------
# Lazy imports for ASGI test client
# ---------------------------------------------------------------------------
try:
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip("starlette not installed; API athletes tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DDL (matches existing athlete_metrics schema)
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
    fatigue_score    FLOAT,
    readiness_score  FLOAT,
    recovery_score   FLOAT,
    adherence_score  FLOAT,
    coaching_flags   TEXT,
    PRIMARY KEY (athlete_id, metric_date)
);
"""

# ---------------------------------------------------------------------------
# Module-scoped fixtures (mirror test_api_metrics.py exactly)
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
    return (
        pg_container.get_connection_url()
        .replace("postgresql+psycopg2://", "postgresql://")
    )


@pytest.fixture(scope="module")
def pg_conn(pg_dsn):
    """A live psycopg2 connection to seed data."""
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_CREATE_ATHLETE_METRICS)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def seeded_db(pg_conn) -> None:
    """Seed athlete_metrics with A3, A1, A2 rows (insert order intentionally scrambled).

    sc-1.2: Alphabetical ordering must be enforced by ORDER BY in the SQL,
    not by insert order. Seeding A3 first, then A1, then A2 proves this.
    """
    from datetime import date

    _seed_row(pg_conn, "A3", date(2025, 1, 1))
    _seed_row(pg_conn, "A1", date(2025, 1, 1))
    _seed_row(pg_conn, "A2", date(2025, 1, 1))


def _seed_row(conn, athlete_id: str, metric_date) -> None:
    """Insert a minimal athlete_metrics row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO athlete_metrics
                (athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
                 acute_chronic_ratio, deload_flag, fatigue_score, readiness_score, coaching_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO NOTHING
            """,
            (athlete_id, metric_date, 100.0, 90.0, 85.0, 1.1, 0, 20.0, 65.0, "[]"),
        )


@pytest.fixture(scope="module")
def api_client(pg_dsn, seeded_db):
    """TestClient wrapping the FastAPI app, with DATABASE_URL injected.

    Reloads api.config / api.db / api.routers.athletes / api.main so the app's
    cached Settings pick up THIS module's DATABASE_URL even when another integration
    module ran earlier in the same session and left a stale config module cached.
    Restores prior env on teardown to avoid leaking into later modules.
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
        import api.routers.athletes as _ra
        importlib.reload(_ra)
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
# sc-1.1 — Happy path: data present → sorted list
# ---------------------------------------------------------------------------


def test_list_athletes_happy_path(api_client, seeded_db):
    """sc-1.1 + sc-1.2: Seeded A3,A1,A2 → response ["A1","A2","A3"] (sorted)."""
    resp = api_client.get("/athletes")
    assert resp.status_code == 200
    body = resp.json()
    assert body["athletes"] == ["A1", "A2", "A3"], (
        f"Expected sorted athletes ['A1','A2','A3'], got: {body['athletes']}"
    )


# ---------------------------------------------------------------------------
# sc-1.3 — Empty database → HTTP 200 with {"athletes": []}
# ---------------------------------------------------------------------------


def test_list_athletes_empty_db(pg_conn, api_client):
    """sc-1.3: Truncate table → GET /athletes returns 200 + {"athletes": []}."""
    with pg_conn.cursor() as cur:
        cur.execute("TRUNCATE athlete_metrics")

    resp = api_client.get("/athletes")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"athletes": []}, (
        f"Expected empty list on empty DB, got: {body}"
    )

    # Re-seed for subsequent tests in this module
    from datetime import date

    _seed_row(pg_conn, "A3", date(2025, 1, 1))
    _seed_row(pg_conn, "A1", date(2025, 1, 1))
    _seed_row(pg_conn, "A2", date(2025, 1, 1))


# ---------------------------------------------------------------------------
# sc-1.4 — Response shape: exactly one top-level key "athletes", value is list[str]
# ---------------------------------------------------------------------------


def test_list_athletes_response_shape(api_client, seeded_db):
    """sc-1.4: Response body has exactly one key 'athletes' whose value is list[str]."""
    resp = api_client.get("/athletes")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == {"athletes"}, (
        f"Expected exactly one top-level key 'athletes', got: {set(body.keys())}"
    )
    assert isinstance(body["athletes"], list), (
        f"'athletes' value must be a list, got {type(body['athletes'])}"
    )
    assert all(isinstance(a, str) for a in body["athletes"]), (
        f"All elements must be strings, got: {body['athletes']}"
    )
