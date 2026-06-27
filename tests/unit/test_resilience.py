"""Unit tests for WU-B: connect_timeout, exception handler, and /health readiness.

Spec source: obs #98 — Slice B requirements.
Design source: obs #99 — ADR H2 (exception handler), H4 (/health readiness), H6 (connect_timeout).

Tests in this file are ALL pure unit tests: no Docker, no real DB, no real Kafka.
Readiness probe dependencies are overridden via FastAPI dependency_overrides.

Scenarios covered:
  B1  — DB connect_timeout propagated to psycopg2.connect
  B3  — Unhandled RuntimeError → HTTP 500 JSON {"detail": "Internal Server Error"}
  B4  — HTTPException 404 is NOT swallowed (still 404)
  B5  — HTTPException 422 is NOT swallowed (still 422)
  B7  — GET /health with DB probe failing → HTTP 503, body contains "db"
  B8  — GET /health with Kafka probe failing → HTTP 503, body contains "kafka"
  B6  — GET /health with both probes healthy → HTTP 200
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# B-1.1  connect_timeout propagated to psycopg2.connect
# ---------------------------------------------------------------------------

class TestConnectTimeout:
    """Settings.db_connect_timeout exists and db.py uses it."""

    def test_settings_has_db_connect_timeout_field(self):
        """api/config.py Settings must have db_connect_timeout with default 5.0."""
        from api.config import Settings

        s = Settings(database_url="postgresql://u:p@localhost/db")
        assert hasattr(s, "db_connect_timeout"), "Settings missing db_connect_timeout field"
        assert s.db_connect_timeout == 5.0, f"Expected 5.0, got {s.db_connect_timeout}"

    def test_db_connect_passes_connect_timeout_to_psycopg2(self, monkeypatch):
        """api/db.py get_db() must pass connect_timeout=settings.db_connect_timeout to psycopg2.connect."""
        import importlib
        import api.config as _cfg

        # Use a fresh Settings with a known timeout value
        captured_kwargs = {}

        class _FakeConn:
            def close(self):
                pass

        def _fake_connect(dsn, **kwargs):
            captured_kwargs.update(kwargs)
            return _FakeConn()

        import psycopg2
        monkeypatch.setattr(psycopg2, "connect", _fake_connect)

        # Force reload so db.py picks up the monkeypatched psycopg2
        import api.db as _db
        importlib.reload(_db)

        gen = _db.get_db()
        conn = next(gen)
        try:
            gen.close()
        except StopIteration:
            pass

        assert "connect_timeout" in captured_kwargs, (
            "psycopg2.connect() was not called with connect_timeout keyword argument"
        )
        assert captured_kwargs["connect_timeout"] == _db.settings.db_connect_timeout, (
            f"connect_timeout mismatch: expected {_db.settings.db_connect_timeout}, "
            f"got {captured_kwargs['connect_timeout']}"
        )


# ---------------------------------------------------------------------------
# B-1.2  Unhandled exception → structured 500 (no stack trace)
# ---------------------------------------------------------------------------

class TestGlobalExceptionHandler:
    """Exception handler: RuntimeError → 500 JSON; 404/422 unaffected."""

    @pytest.fixture
    def client(self):
        """TestClient for the FastAPI app with a synthetic crash route."""
        from fastapi import HTTPException
        from starlette.testclient import TestClient
        import importlib
        import os

        # Ensure clean settings without real DB needed
        os.environ.setdefault("CORS_ORIGINS", "http://localhost:5173")

        import api.main as _main
        importlib.reload(_main)
        app = _main.app

        # Add a synthetic route that raises RuntimeError (only for testing)
        @app.get("/_test_crash")
        def _crash():
            raise RuntimeError("boom — intentional crash for test")

        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

        # Remove the synthetic route after the test
        # (routes are registered, so we reload to clear it)
        app.routes[:] = [r for r in app.routes if not getattr(r, "path", "") == "/_test_crash"]

    def test_unhandled_runtime_error_returns_500_json(self, client):
        """B3: RuntimeError not caught by route → HTTP 500 with {"detail": ...}."""
        resp = client.get("/_test_crash")
        assert resp.status_code == 500, f"Expected 500, got {resp.status_code}"
        body = resp.json()
        assert "detail" in body, f"Expected 'detail' key in body, got: {body}"
        # Must NOT leak the stack trace
        assert "traceback" not in str(body).lower()
        assert "Traceback" not in str(body)

    def test_500_body_contains_generic_message_not_internal_error(self, client):
        """B3 triangulation: body detail must be a string (not the raw exception message)."""
        resp = client.get("/_test_crash")
        body = resp.json()
        # The detail must be a safe generic string, not 'boom — intentional crash for test'
        assert body["detail"] != "boom — intentional crash for test", (
            "Exception handler leaked the raw exception message to the client"
        )

    def test_404_not_swallowed_by_exception_handler(self, client):
        """B4: GET to a non-existent route still returns 404, not 500."""
        resp = client.get("/athletes/NONEXISTENT_ID_THAT_DOES_NOT_EXIST/metrics")
        assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"

    def test_422_not_swallowed_by_exception_handler(self, client):
        """B5: Invalid query param still returns 422, not 500."""
        resp = client.get("/athletes/A1/metrics?from=not-a-date")
        assert resp.status_code == 422, f"Expected 422, got {resp.status_code}"


# ---------------------------------------------------------------------------
# B-1.3 / B-1.4 / B-1.6  /health readiness probe (unit — mocked deps)
# ---------------------------------------------------------------------------

class TestHealthReadiness:
    """/health returns 200 both-ok; 503+body on DB down; 503+body on Kafka down."""

    @pytest.fixture
    def fresh_app(self):
        """Reload api.main so we get a clean app without leftover test routes."""
        import importlib
        import os

        os.environ.setdefault("CORS_ORIGINS", "http://localhost:5173")

        import api.main as _main
        importlib.reload(_main)
        return _main.app

    def test_health_200_when_both_deps_healthy(self, fresh_app):
        """B6: Both DB probe and Kafka probe succeed → HTTP 200."""
        from starlette.testclient import TestClient
        from api.main import _probe_db, _probe_kafka  # noqa: F401

        def _ok_db():
            pass  # no exception = healthy

        def _ok_kafka():
            pass  # no exception = healthy

        fresh_app.dependency_overrides[_probe_db] = _ok_db
        fresh_app.dependency_overrides[_probe_kafka] = _ok_kafka

        with TestClient(fresh_app) as client:
            resp = client.get("/health")

        fresh_app.dependency_overrides.clear()
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"

    def test_health_503_when_db_probe_fails(self, fresh_app):
        """B7: DB probe raises → HTTP 503, body names 'db'."""
        from starlette.testclient import TestClient
        from api.main import _probe_db, _probe_kafka  # noqa: F401

        def _fail_db():
            raise Exception("DB unreachable")

        def _ok_kafka():
            pass

        fresh_app.dependency_overrides[_probe_db] = _fail_db
        fresh_app.dependency_overrides[_probe_kafka] = _ok_kafka

        with TestClient(fresh_app) as client:
            resp = client.get("/health")

        fresh_app.dependency_overrides.clear()
        assert resp.status_code == 503, f"Expected 503, got {resp.status_code}"
        body = resp.json()
        assert "db" in str(body).lower(), f"Expected 'db' in 503 body, got: {body}"

    def test_health_503_when_kafka_probe_fails(self, fresh_app):
        """B8: Kafka probe raises → HTTP 503, body names 'kafka'."""
        from starlette.testclient import TestClient
        from api.main import _probe_db, _probe_kafka  # noqa: F401

        def _ok_db():
            pass

        def _fail_kafka():
            raise Exception("Kafka unreachable")

        fresh_app.dependency_overrides[_probe_db] = _ok_db
        fresh_app.dependency_overrides[_probe_kafka] = _fail_kafka

        with TestClient(fresh_app) as client:
            resp = client.get("/health")

        fresh_app.dependency_overrides.clear()
        assert resp.status_code == 503, f"Expected 503, got {resp.status_code}"
        body = resp.json()
        assert "kafka" in str(body).lower(), f"Expected 'kafka' in 503 body, got: {body}"
