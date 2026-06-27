"""Unit tests for WU-B: connect_timeout, exception handler, and /health readiness.

Spec source: obs #98 — Slice B requirements.
Design source: obs #99 — ADR H2 (exception handler), H4 (/health readiness), H6 (connect_timeout).

Tests in this file are ALL pure unit tests: no Docker, no real DB, no real Kafka.
/health probe functions are monkeypatched at the api.main module level for speed.

Scenarios covered:
  B1  — DB connect_timeout propagated to psycopg2.connect (Settings field + db.py kwarg)
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

        captured_kwargs: dict = {}

        class _FakeConn:
            def close(self):
                pass

        def _fake_connect(dsn, **kwargs):
            captured_kwargs.update(kwargs)
            return _FakeConn()

        import psycopg2
        monkeypatch.setattr(psycopg2, "connect", _fake_connect)

        # Reload db.py so it picks up the monkeypatched psycopg2
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
        assert captured_kwargs["connect_timeout"] == int(_db.settings.db_connect_timeout), (
            f"connect_timeout mismatch: expected {int(_db.settings.db_connect_timeout)}, "
            f"got {captured_kwargs['connect_timeout']}"
        )


# ---------------------------------------------------------------------------
# B-1.2  Unhandled exception → structured 500 (no stack trace)
# ---------------------------------------------------------------------------

class TestGlobalExceptionHandler:
    """Exception handler: RuntimeError → 500 JSON; 404/422 unaffected."""

    @pytest.fixture
    def client(self):
        """TestClient with a synthetic crash route injected into the app.

        Also overrides get_db with a no-op stub so tests that hit DB-backed
        routes (e.g. /athletes/{id}/metrics for 422) don't fail due to missing
        local Postgres — the spec behavior under test is FastAPI's validation
        layer, which fires before (or concurrently with) dependency resolution.
        """
        import importlib
        import os
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        import api.main as _main
        from api.db import get_db

        os.environ.setdefault("CORS_ORIGINS", "http://localhost:5173")
        importlib.reload(_main)
        app = _main.app

        # Stub the DB dependency so no real connection is attempted
        def _fake_db():
            mock_conn = MagicMock()
            mock_conn.__enter__ = lambda s: s
            mock_conn.__exit__ = MagicMock(return_value=False)
            yield mock_conn

        app.dependency_overrides[get_db] = _fake_db

        # Inject a route that raises RuntimeError — only lives for this test
        @app.get("/_test_crash")
        def _crash():
            raise RuntimeError("boom — intentional crash for test")

        from starlette.testclient import TestClient
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

        # Clean up: remove overrides and synthetic route
        app.dependency_overrides.pop(get_db, None)
        app.routes[:] = [r for r in app.routes if getattr(r, "path", "") != "/_test_crash"]

    def test_unhandled_runtime_error_returns_500_json(self, client):
        """B3: RuntimeError not caught by route → HTTP 500 with {"detail": ...}."""
        resp = client.get("/_test_crash")
        assert resp.status_code == 500, f"Expected 500, got {resp.status_code}"
        body = resp.json()
        assert "detail" in body, f"Expected 'detail' key in body, got: {body}"
        # Must NOT leak the stack trace in the response body
        assert "traceback" not in str(body).lower()
        assert "Traceback" not in str(body)

    def test_500_body_detail_is_generic_not_raw_exception(self, client):
        """B3 triangulation: body detail must be generic, not the raw exception message."""
        resp = client.get("/_test_crash")
        body = resp.json()
        # The detail must not expose the raw RuntimeError message to clients
        assert body["detail"] != "boom — intentional crash for test", (
            "Exception handler leaked the raw exception message to the client"
        )

    def test_404_not_swallowed_by_exception_handler(self, client):
        """B4: GET to a path that does not exist returns 404, not 500.

        Uses a path that has no registered route — Starlette returns 404 before
        any dependency or exception handler is invoked. This proves the broad
        Exception handler does NOT intercept Starlette's own 404 responses.
        """
        resp = client.get("/athletes/A1/metrics/does-not-exist/extra-segment")
        assert resp.status_code == 404, f"Expected 404, got {resp.status_code}"

    def test_422_not_swallowed_by_exception_handler(self, client):
        """B5: Invalid query param type (from=not-a-date) returns 422, not 500.

        FastAPI's RequestValidationError handler fires before any route handler
        or dependency executes — the broad Exception handler must not intercept it.
        """
        resp = client.get("/athletes/A1/metrics?from=not-a-date")
        assert resp.status_code == 422, f"Expected 422, got {resp.status_code}"


# ---------------------------------------------------------------------------
# B-1.3 / B-1.4 / B-1.6  /health readiness probe (unit — monkeypatched)
# ---------------------------------------------------------------------------

class TestHealthReadiness:
    """/health returns 200 when both probes pass; 503+body when either fails.

    Monkeypatches api.main._probe_db and api.main._probe_kafka so tests run
    without Docker, real DB, or real Kafka.
    """

    @pytest.fixture
    def health_client(self, monkeypatch):
        """TestClient with probe functions injectable via monkeypatch."""
        import importlib
        import os
        import api.main as _main

        os.environ.setdefault("CORS_ORIGINS", "http://localhost:5173")
        importlib.reload(_main)

        from starlette.testclient import TestClient
        with TestClient(_main.app, raise_server_exceptions=False) as c:
            yield c, monkeypatch, _main

    def test_health_200_when_both_deps_healthy(self, health_client):
        """B6: Both probes succeed → HTTP 200."""
        client, mp, main_mod = health_client

        mp.setattr(main_mod, "_probe_db", lambda: None)
        mp.setattr(main_mod, "_probe_kafka", lambda: None)

        resp = client.get("/health")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"

    def test_health_503_when_db_probe_fails(self, health_client):
        """B7: DB probe raises → HTTP 503, body contains 'db' key."""
        client, mp, main_mod = health_client

        def _fail_db():
            raise Exception("DB connection refused")

        mp.setattr(main_mod, "_probe_db", _fail_db)
        mp.setattr(main_mod, "_probe_kafka", lambda: None)

        resp = client.get("/health")
        assert resp.status_code == 503, f"Expected 503, got {resp.status_code}"
        body = resp.json()
        assert "db" in body, f"Expected 'db' key in 503 body, got: {body}"

    def test_health_503_when_kafka_probe_fails(self, health_client):
        """B8: Kafka probe raises → HTTP 503, body contains 'kafka' key."""
        client, mp, main_mod = health_client

        mp.setattr(main_mod, "_probe_db", lambda: None)

        def _fail_kafka():
            raise Exception("Kafka broker unreachable")

        mp.setattr(main_mod, "_probe_kafka", _fail_kafka)

        resp = client.get("/health")
        assert resp.status_code == 503, f"Expected 503, got {resp.status_code}"
        body = resp.json()
        assert "kafka" in body, f"Expected 'kafka' key in 503 body, got: {body}"
