"""Unit tests for the metrics endpoint's date helpers (no DB, no Docker).

Guards the spec contract that the default ``to`` boundary resolves in UTC, not
the host's local timezone. Regression guard for the bug where ``_today_utc()``
called ``date.today()`` (local time), which shifts the default window by a
calendar day on any non-UTC server.

Spec source: obs #65 (sdd/athleteos-phase7-web/spec), Domain A:
"to defaults to today (server time, UTC)".
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from unittest import mock

import pytest
from fastapi import FastAPI, Depends
from starlette.testclient import TestClient

from api.routers import metrics
from api.routers.metrics import _today_utc


def test_today_utc_returns_utc_date_not_local():
    """At an instant where UTC and a positive-offset local clock differ by a day,
    _today_utc() must return the UTC date.

    23:30 on 2025-06-30 UTC is already 2025-07-01 in, e.g., UTC+1. A local-time
    implementation would return 2025-07-01; the UTC contract requires 2025-06-30.
    """
    fixed_utc = datetime(2025, 6, 30, 23, 30, tzinfo=timezone.utc)

    class _FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz is timezone.utc, "_today_utc must request UTC explicitly"
            return fixed_utc

    with mock.patch("api.routers.metrics.datetime", _FixedDatetime):
        assert _today_utc() == date(2025, 6, 30)


def test_today_utc_matches_current_utc_date():
    """Sanity: with no mocking, the helper agrees with the real UTC date."""
    assert _today_utc() == datetime.now(timezone.utc).date()


# ---------------------------------------------------------------------------
# sc-1 (api-auth) — Missing X-API-Key → 401 on /athletes/{id}/metrics
# sc-5 — Correct X-API-Key → 200 on /athletes/{id}/metrics
# ---------------------------------------------------------------------------


def _fake_db_cursor(rows=None):
    """Return a fake psycopg2 connection/cursor."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda self: self
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = ("A1",)  # athlete exists
    mock_cursor.fetchall.return_value = rows or []
    mock_cursor.description = []

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


def _make_metrics_client_with_auth():
    """TestClient with REAL require_api_key wired on the metrics router."""
    from api.security import require_api_key
    from api.db import get_db

    app = FastAPI()
    app.include_router(metrics.router)
    app.dependency_overrides[get_db] = lambda: _fake_db_cursor()
    return TestClient(app, raise_server_exceptions=False)


def _make_metrics_client_auth_overridden():
    """TestClient with require_api_key overridden (happy-path)."""
    from api.security import require_api_key
    from api.db import get_db

    app = FastAPI()
    app.include_router(metrics.router)
    app.dependency_overrides[get_db] = lambda: _fake_db_cursor()
    app.dependency_overrides[require_api_key] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


def test_metrics_missing_key_returns_401():
    """sc-1 (api-auth): GET /athletes/{id}/metrics without X-API-Key → 401."""
    client = _make_metrics_client_with_auth()
    resp = client.get("/athletes/A1/metrics")
    assert resp.status_code == 401, (
        f"Expected 401 when X-API-Key is absent, got {resp.status_code}"
    )


def test_metrics_missing_key_not_422():
    """S2 guard: missing key on /athletes/{id}/metrics must NOT return 422."""
    client = _make_metrics_client_with_auth()
    resp = client.get("/athletes/A1/metrics")
    assert resp.status_code != 422, (
        "GET /athletes/{id}/metrics returned 422 for missing key — must be 401"
    )


def test_metrics_missing_key_body_has_detail():
    """S1 guard: 401 body must be JSON with a 'detail' key."""
    client = _make_metrics_client_with_auth()
    resp = client.get("/athletes/A1/metrics")
    body = resp.json()
    assert "detail" in body, f"Expected 'detail' in 401 body, got: {body}"


def test_metrics_correct_key_returns_200():
    """sc-5: correct X-API-Key → GET /athletes/{id}/metrics returns 200."""
    client = _make_metrics_client_auth_overridden()
    resp = client.get("/athletes/A1/metrics")
    assert resp.status_code == 200, (
        f"Expected 200 with auth overridden, got {resp.status_code}"
    )
