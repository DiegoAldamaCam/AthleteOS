"""Unit tests for router wiring — require_auth swap (Phase 8, sc-19..sc-22).

Design: obs #386 (sdd/athleteos-jwt-auth/design), ADR-4.
Tasks: obs #388, Phase 8, tasks 8.1–8.5.

Scenarios:
  sc-19: all 3 protected routers accept Bearer JWT → 200
  sc-20: all 3 protected routers accept X-API-Key (coexistence regression) → 200
  sc-21: all 3 protected routers reject no credential → 401
  sc-22: /health is open (no auth dependency); GET /health → 200 (no headers)

Each router gets its own isolated FastAPI test app with dependency_overrides for
get_db (and kafka for pipeline) so no real infra is needed. Auth is real — only
the downstream business dependencies are stubbed.

Isolation note: test_resilience.py uses importlib.reload(api.db) which creates a
new get_db function object. To remain resilient to that, each make_*_client function
imports get_db from the athletes/metrics module's current reference (not from api.db
directly), ensuring the override key always matches what the router actually uses.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.config import settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TEST_SECRET = "test-jwt-secret-fixture"
TEST_ALGORITHM = "HS256"
_VALID_API_KEY = settings.api_key


def _make_token(sub: str = "athlete_user") -> str:
    """Create a valid test JWT token."""
    exp = datetime.now(tz=timezone.utc) + timedelta(minutes=60)
    payload = {"sub": sub, "exp": exp}
    return jwt.encode(payload, TEST_SECRET, algorithm=TEST_ALGORITHM)


def _fake_conn_for_athletes() -> MagicMock:
    """Fake psycopg2 connection that returns empty athlete list."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda s: s
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


def _fake_conn_for_metrics() -> MagicMock:
    """Fake psycopg2 connection for metrics endpoint."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda s: s
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = (1,)  # athlete exists
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


# ---------------------------------------------------------------------------
# Build isolated test clients per router
#
# IMPORTANT: dependency_overrides keys must match the exact function object
# that the router module currently uses. If api.db was reloaded by another
# test (test_resilience.py does this), the router module may still hold the
# pre-reload reference. We import get_db from the router's own module scope
# to get the correct reference, then override that.
# ---------------------------------------------------------------------------

def _make_athletes_client() -> TestClient:
    """Isolated FastAPI app with athletes router; DB overridden."""
    # Reload the athletes router so it captures the current get_db reference
    import api.routers.athletes as _ra
    importlib.reload(_ra)
    # Now import get_db from api.db — after reload, they should be in sync
    import api.db as _db
    importlib.reload(_db)
    importlib.reload(_ra)  # reload again so athletes picks up the freshly-reloaded db

    app = FastAPI()
    app.include_router(_ra.router)
    app.dependency_overrides[_db.get_db] = lambda: _fake_conn_for_athletes()
    return TestClient(app, raise_server_exceptions=False)


def _make_metrics_client() -> TestClient:
    """Isolated FastAPI app with metrics router; DB overridden."""
    import api.routers.metrics as _rm
    import api.db as _db
    importlib.reload(_db)
    importlib.reload(_rm)

    app = FastAPI()
    app.include_router(_rm.router)
    app.dependency_overrides[_db.get_db] = lambda: _fake_conn_for_metrics()
    return TestClient(app, raise_server_exceptions=False)


def _make_pipeline_client() -> TestClient:
    """Isolated FastAPI app with pipeline router."""
    import api.routers.pipeline as _rp
    importlib.reload(_rp)

    app = FastAPI()
    app.include_router(_rp.router)
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# sc-19: Bearer JWT → 200 on all 3 protected routers
# ---------------------------------------------------------------------------

class TestBearerJwtOnProtectedRouters:
    """sc-19: Valid Bearer JWT must be accepted by all 3 protected routers."""

    def test_get_athletes_with_bearer_token(self) -> None:
        """sc-19: GET /athletes with valid Bearer → 200 (not 401)."""
        client = _make_athletes_client()
        token = _make_token()
        response = client.get("/athletes", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200

    def test_get_metrics_with_bearer_token(self) -> None:
        """sc-19: GET /athletes/{id}/metrics with valid Bearer → 200 or 404 (not 401)."""
        client = _make_metrics_client()
        token = _make_token()
        response = client.get(
            "/athletes/athlete-1/metrics",
            headers={"Authorization": f"Bearer {token}"},
        )
        # 200 (athlete exists from mock) — auth passes, not 401
        assert response.status_code in (200, 404)
        assert response.status_code != 401

    def test_get_pipeline_dlq_depth_with_bearer_token(self) -> None:
        """sc-19: GET /pipeline/dlq-depth with valid Bearer → 200 (not 401)."""
        from unittest.mock import patch

        client = _make_pipeline_client()
        token = _make_token()
        with patch("api.routers.pipeline.get_dlq_depths") as mock_dlq:
            mock_dlq.return_value = {"broker_reachable": True, "topics": {}}
            response = client.get(
                "/pipeline/dlq-depth",
                headers={"Authorization": f"Bearer {token}"},
            )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# sc-20: X-API-Key coexistence — all 3 protected routers still accept the key
# ---------------------------------------------------------------------------

class TestXApiKeyCoexistenceRegression:
    """sc-20: Existing X-API-Key auth must continue to work after router swap.

    These tests verify the coexistence fallback path: no Authorization header
    present → fall through to X-API-Key check → match → 200.
    """

    def test_get_athletes_with_api_key(self) -> None:
        """sc-20: GET /athletes with valid X-API-Key (no Bearer) → 200."""
        client = _make_athletes_client()
        response = client.get("/athletes", headers={"X-API-Key": _VALID_API_KEY})
        assert response.status_code == 200

    def test_get_metrics_with_api_key(self) -> None:
        """sc-20: GET /athletes/{id}/metrics with valid X-API-Key (no Bearer) → not 401."""
        client = _make_metrics_client()
        response = client.get(
            "/athletes/athlete-1/metrics",
            headers={"X-API-Key": _VALID_API_KEY},
        )
        assert response.status_code in (200, 404)
        assert response.status_code != 401

    def test_get_pipeline_dlq_depth_with_api_key(self) -> None:
        """sc-20: GET /pipeline/dlq-depth with valid X-API-Key (no Bearer) → 200."""
        from unittest.mock import patch

        client = _make_pipeline_client()
        with patch("api.routers.pipeline.get_dlq_depths") as mock_dlq:
            mock_dlq.return_value = {"broker_reachable": True, "topics": {}}
            response = client.get(
                "/pipeline/dlq-depth",
                headers={"X-API-Key": _VALID_API_KEY},
            )
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# sc-21: no credentials → 401 on all 3 protected routers
# ---------------------------------------------------------------------------

class TestNoCredentialReturns401:
    """sc-21: Requests with no auth headers must be rejected with 401."""

    def test_get_athletes_no_auth_returns_401(self) -> None:
        """sc-21: GET /athletes with no credentials → 401."""
        client = _make_athletes_client()
        response = client.get("/athletes")
        assert response.status_code == 401

    def test_get_metrics_no_auth_returns_401(self) -> None:
        """sc-21: GET /athletes/{id}/metrics with no credentials → 401."""
        client = _make_metrics_client()
        response = client.get("/athletes/athlete-1/metrics")
        assert response.status_code == 401

    def test_get_pipeline_no_auth_returns_401(self) -> None:
        """sc-21: GET /pipeline/dlq-depth with no credentials → 401."""
        client = _make_pipeline_client()
        response = client.get("/pipeline/dlq-depth")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# sc-22: /health is open — no auth dependency
# ---------------------------------------------------------------------------

class TestHealthEndpointIsOpen:
    """sc-22: GET /health must return 200 with no auth headers (open endpoint)."""

    def test_health_endpoint_requires_no_auth(self) -> None:
        """sc-22: /health is not protected — responds without Authorization or X-API-Key."""
        from unittest.mock import patch
        import api.main as _main
        importlib.reload(_main)
        from fastapi.testclient import TestClient as TC

        with (
            patch("api.main._probe_db"),
            patch("api.main._probe_kafka"),
        ):
            c = TC(_main.app, raise_server_exceptions=False)
            response = c.get("/health")
        assert response.status_code == 200

    def test_health_route_has_no_require_auth_dep(self) -> None:
        """sc-22: introspect that /health route does not depend on require_auth."""
        import api.main as _main
        import api.security as _sec
        importlib.reload(_sec)
        importlib.reload(_main)

        health_route = next(
            (r for r in _main.app.routes if getattr(r, "path", None) == "/health"),
            None,
        )
        assert health_route is not None, "/health route must exist in main app"
        route_deps = getattr(health_route, "dependencies", [])
        dep_funcs = [d.dependency for d in route_deps]
        assert _sec.require_auth not in dep_funcs, "/health must NOT have require_auth dependency"
