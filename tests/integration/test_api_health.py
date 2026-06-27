"""Integration test for GET /health readiness probe — Scenario B6.

Spec source: obs #98 — Slice B, Readiness Health Check, Scenario "All dependencies healthy".
Design source: obs #99 — ADR H4 (/health = readiness, REPLACE), H5 (bounded timeouts).

This test runs ONLY when Docker is available. It starts a real PostgresContainer
and a real Redpanda (Kafka-compatible) container, points the FastAPI app at both,
and asserts GET /health returns HTTP 200.

This proves the readiness probe succeeds with real infrastructure — not just
mocked dependencies. It also validates the 503 behavior is intentional: when both
deps are up, the new /health returns 200, so the fastapi service healthcheck in
docker-compose passes and `web` (depends_on fastapi service_healthy) still starts.

B6: GET /health → 200 when both DB and Kafka are reachable (REAL containers).
"""

from __future__ import annotations

import os

import pytest

from tests.conftest import requires_docker

requires_docker()

try:
    import httpx  # noqa: F401
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip("httpx / starlette not installed; health integration test skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

_RELOAD_ENV_KEYS = ("DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS", "CORS_ORIGINS")


def _reload_api_modules() -> None:
    """Reload api.* so module-level Settings and singletons pick up current env."""
    import importlib

    try:
        import api.config as _cfg
        importlib.reload(_cfg)
        import api.kafka_admin as _ka
        _ka._admin_client_singleton = None  # noqa: SLF001
        importlib.reload(_ka)
        import api.main as _main
        importlib.reload(_main)
    except (ImportError, AttributeError):
        pass


@pytest.fixture(scope="module")
def pg_health(docker_ok):
    """A throwaway PostgresContainer for the /health integration test."""
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
def redpanda_health(docker_ok):
    """A throwaway Redpanda (Kafka) container for the /health integration test."""
    if not docker_ok:
        pytest.skip("Docker not available")
    from testcontainers.kafka import RedpandaContainer

    container = RedpandaContainer()
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def health_api_client(pg_health, redpanda_health):
    """TestClient for the FastAPI app with REAL DB + Kafka containers.

    Both containers must be up for /health to return 200 — this is the
    real-container proof of the B6 scenario.
    """
    pg_dsn = (
        pg_health.get_connection_url()
        .replace("postgresql+psycopg2://", "postgresql://")
    )
    bootstrap = redpanda_health.get_bootstrap_server()

    _env_backup = {k: os.environ.get(k) for k in _RELOAD_ENV_KEYS}

    os.environ["DATABASE_URL"] = pg_dsn
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
    os.environ["CORS_ORIGINS"] = "http://localhost:5173"

    _reload_api_modules()

    from api.main import app  # noqa: PLC0415

    with TestClient(app) as client:
        yield client

    # Restore env and reload to avoid leaking into later modules
    for key, value in _env_backup.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    _reload_api_modules()


# ---------------------------------------------------------------------------
# B6 — All dependencies healthy → 200
# ---------------------------------------------------------------------------


def test_health_200_when_both_deps_reachable(health_api_client):
    """B6: Both real DB and real Kafka up → GET /health returns HTTP 200.

    This is the contract that makes docker-compose `web` (depends_on fastapi
    service_healthy) viable: with both deps healthy at startup, /health returns
    200 and the healthcheck passes within start_period.
    """
    resp = health_api_client.get("/health")
    assert resp.status_code == 200, (
        f"Expected 200 from /health with real containers, got {resp.status_code}: {resp.text}"
    )
    body = resp.json()
    assert body.get("status") == "ok", f"Expected status='ok', got: {body}"
