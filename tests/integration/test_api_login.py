"""Integration tests for POST /login and JWT Bearer authentication (Phase 10).

Spec: obs #385 (sdd/athleteos-jwt-auth/spec).
Design: obs #386 (sdd/athleteos-jwt-auth/design).
Tasks: obs #388, Phase 10, tasks 10.1–10.3.

Scenarios:
  sc-23: seed user → POST /login → JWT → GET /athletes with Bearer → 200
  sc-24: coexistence regression — existing X-API-Key integration tests still pass

Docker-gated: skipped automatically when Docker daemon is not reachable.
The test applies users_ddl.sql + athlete_metrics DDL, seeds a test user via
hash_password(), calls POST /login, and then exercises a protected endpoint
with the returned Bearer token.

Coexistence (sc-24): X-API-Key tests in test_api_athletes.py / test_api_metrics.py
/ test_api_dlq.py are NOT modified — they run unchanged and must pass.
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
    pytest.skip("starlette not installed; API login tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DDL constants
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

_CREATE_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id           SERIAL PRIMARY KEY,
    username     TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Test credentials
_TEST_USERNAME = "integration_test_user"
_TEST_PASSWORD = "integration_test_password_secure_2026"

# ---------------------------------------------------------------------------
# Module-scoped fixtures
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
    """A live psycopg2 connection; applies both DDLs idempotently."""
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(_CREATE_ATHLETE_METRICS)
        cur.execute(_CREATE_USERS)
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def seeded_db(pg_conn) -> None:
    """Seed:
    - One athlete_metrics row (so /athletes returns data).
    - One user row with a bcrypt-hashed password (for POST /login).
    """
    from api.jwt_utils import hash_password
    from datetime import date

    hashed = hash_password(_TEST_PASSWORD)
    with pg_conn.cursor() as cur:
        # Seed athlete_metrics
        cur.execute(
            """
            INSERT INTO athlete_metrics
                (athlete_id, metric_date, acute_load, chronic_load_28d, chronic_load_42d,
                 acute_chronic_ratio, deload_flag, fatigue_score, readiness_score, coaching_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (athlete_id, metric_date) DO NOTHING
            """,
            ("int-test-athlete", date(2026, 1, 1), 100.0, 90.0, 85.0, 1.1, 0, 20.0, 65.0, "[]"),
        )
        # Seed user row
        cur.execute(
            """
            INSERT INTO users (username, password_hash)
            VALUES (%s, %s)
            ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash
            """,
            (_TEST_USERNAME, hashed),
        )


@pytest.fixture(scope="module")
def api_client(pg_dsn, seeded_db):
    """TestClient wrapping the FastAPI app with the container DATABASE_URL.

    Reloads api.config / api.db / api.routers.* / api.main so settings pick
    up THIS module's DATABASE_URL (mirrors test_api_athletes.py pattern).
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
        import api.jwt_utils as _jwt
        importlib.reload(_jwt)
        import api.security as _sec
        importlib.reload(_sec)
        import api.routers.athletes as _ra
        importlib.reload(_ra)
        import api.routers.login as _rl
        importlib.reload(_rl)
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
# sc-23: seed → POST /login → Bearer JWT → GET /athletes → 200
# ---------------------------------------------------------------------------


class TestLoginAndBearerAccess:
    """sc-23: Full flow — POST /login returns a valid Bearer JWT → protected endpoint 200."""

    def test_post_login_with_valid_credentials_returns_200(self, api_client: TestClient) -> None:
        """sc-23 step 1: POST /login with correct username/password → 200."""
        response = api_client.post(
            "/login",
            json={"username": _TEST_USERNAME, "password": _TEST_PASSWORD},
        )
        assert response.status_code == 200, (
            f"POST /login expected 200 with valid credentials, got {response.status_code}: {response.text}"
        )

    def test_post_login_returns_access_token_and_token_type(self, api_client: TestClient) -> None:
        """sc-23 step 2: Response body contains access_token and token_type='bearer'."""
        response = api_client.post(
            "/login",
            json={"username": _TEST_USERNAME, "password": _TEST_PASSWORD},
        )
        assert response.status_code == 200
        body = response.json()
        assert "access_token" in body, f"Missing 'access_token' in response: {body}"
        assert body["token_type"] == "bearer", (
            f"Expected token_type='bearer', got: {body.get('token_type')!r}"
        )

    def test_post_login_access_token_is_decodable_jwt(self, api_client: TestClient) -> None:
        """sc-23 step 3: The returned access_token is a valid decodable JWT with 'sub'."""
        import jwt as pyjwt

        response = api_client.post(
            "/login",
            json={"username": _TEST_USERNAME, "password": _TEST_PASSWORD},
        )
        assert response.status_code == 200
        token = response.json()["access_token"]

        # Decode with the test secret (matches conftest setdefault)
        payload = pyjwt.decode(
            token,
            os.environ.get("JWT_SECRET", "test-jwt-secret-fixture"),
            algorithms=["HS256"],
        )
        assert payload.get("sub") == _TEST_USERNAME, (
            f"Expected sub='{_TEST_USERNAME}', got: {payload.get('sub')!r}"
        )
        assert "exp" in payload, "JWT payload must contain 'exp' claim"

    def test_bearer_token_grants_access_to_protected_endpoint(self, api_client: TestClient) -> None:
        """sc-23 step 4: Bearer JWT from /login → GET /athletes → 200 (authorized)."""
        # Step 1: login
        login_response = api_client.post(
            "/login",
            json={"username": _TEST_USERNAME, "password": _TEST_PASSWORD},
        )
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]

        # Step 2: use token on protected endpoint
        athletes_response = api_client.get(
            "/athletes",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert athletes_response.status_code == 200, (
            f"GET /athletes with valid Bearer expected 200, got {athletes_response.status_code}"
        )
        body = athletes_response.json()
        assert "athletes" in body, f"Response missing 'athletes' key: {body}"
        assert "int-test-athlete" in body["athletes"], (
            f"Seeded athlete not in response: {body['athletes']}"
        )


# ---------------------------------------------------------------------------
# sc-24: coexistence regression — X-API-Key still works on /athletes
# ---------------------------------------------------------------------------


class TestXApiKeyCoexistenceRegression:
    """sc-24: Existing X-API-Key integration test coexistence — /athletes still works with key.

    The test_api_athletes.py integration tests pass X-API-Key headers and must
    not be broken by the router swap. This class verifies the same behavior
    within this module's fixture context to prove coexistence.

    NOTE: test_api_athletes.py, test_api_metrics.py, test_api_dlq.py are NOT
    modified — they run unchanged. This class documents the coexistence guarantee.
    """

    def test_get_athletes_with_api_key_still_works(self, api_client: TestClient) -> None:
        """sc-24: GET /athletes with valid X-API-Key (no Bearer) → 200 (coexistence)."""
        response = api_client.get(
            "/athletes",
            headers={"X-API-Key": os.environ.get("API_KEY", "test-api-key-fixture")},
        )
        assert response.status_code == 200, (
            f"Coexistence regression: GET /athletes with X-API-Key expected 200, "
            f"got {response.status_code}"
        )
        assert "athletes" in response.json(), "Response missing 'athletes' key"

    def test_get_athletes_with_no_credentials_returns_401(self, api_client: TestClient) -> None:
        """sc-24 guard: no credentials → 401 (auth is still enforced after swap)."""
        response = api_client.get("/athletes")
        assert response.status_code == 401, (
            f"Expected 401 with no credentials, got {response.status_code}"
        )
