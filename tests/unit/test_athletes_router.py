"""Unit tests for GET /athletes — no DB, dependency-overridden.

Spec: obs #255, sc-1.3 (empty → 200 + []) and sc-1.4 (response shape: single key "athletes").
Design: obs #256 — router returns {"athletes": [r[0] for r in cursor.fetchall()]}.

These tests run without Docker; the DB dependency is overridden so the route
returns a predictable result from an in-memory fake cursor.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from api.routers import athletes


# ---------------------------------------------------------------------------
# Helpers — fake DB dependency
# ---------------------------------------------------------------------------


def _fake_db_with_rows(rows: list[tuple]) -> MagicMock:
    """Return a fake psycopg2 connection whose cursor.fetchall() yields `rows`."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda self: self
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchall.return_value = rows

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


def _make_client(rows: list[tuple]) -> TestClient:
    """Build a TestClient with the DB dependency overridden to return `rows`.

    Also overrides require_auth so existing data/shape tests focus on DB
    behavior, not auth behavior. Auth-specific tests use _make_auth_client().
    """
    from fastapi import FastAPI
    from api.db import get_db
    from api.security import require_auth

    app = FastAPI()
    app.include_router(athletes.router)
    app.dependency_overrides[get_db] = lambda: _fake_db_with_rows(rows)
    app.dependency_overrides[require_auth] = lambda: None
    return TestClient(app)


# ---------------------------------------------------------------------------
# sc-1.3 — Empty database → HTTP 200 with {"athletes": []}
# ---------------------------------------------------------------------------


def test_list_athletes_empty_db_returns_200_with_empty_list():
    """sc-1.3: No rows → GET /athletes returns 200 + {"athletes": []}."""
    client = _make_client([])
    resp = client.get("/athletes")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"athletes": []}, f"Expected empty athletes list, got: {body}"


# ---------------------------------------------------------------------------
# sc-1.4 — Response shape: exactly one top-level key "athletes"
# ---------------------------------------------------------------------------


def test_list_athletes_response_shape_single_key():
    """sc-1.4: Response body has exactly one top-level key 'athletes' whose value is a list."""
    client = _make_client([("A1",), ("A2",)])
    resp = client.get("/athletes")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body.keys()) == {"athletes"}, (
        f"Expected exactly one top-level key 'athletes', got keys: {set(body.keys())}"
    )
    assert isinstance(body["athletes"], list), (
        f"'athletes' value must be a list, got: {type(body['athletes'])}"
    )
    assert all(isinstance(a, str) for a in body["athletes"]), (
        f"All elements in 'athletes' must be strings, got: {body['athletes']}"
    )


# ---------------------------------------------------------------------------
# sc-1.1 / sc-1.2 — Data present → 200 + sorted distinct athletes
# ---------------------------------------------------------------------------


def test_list_athletes_returns_sorted_athletes():
    """sc-1.1 + sc-1.2: Rows present → sorted athlete IDs returned."""
    # DB already returns rows in sorted order (ORDER BY in SQL); router just maps r[0]
    client = _make_client([("A1",), ("A2",), ("A3",)])
    resp = client.get("/athletes")
    assert resp.status_code == 200
    body = resp.json()
    assert body["athletes"] == ["A1", "A2", "A3"], (
        f"Expected sorted athlete list ['A1','A2','A3'], got: {body['athletes']}"
    )


# ---------------------------------------------------------------------------
# sc-1 (api-auth) — Missing X-API-Key header → 401, NOT 422 (ADR-A3, S1, S2)
# ---------------------------------------------------------------------------


def _make_auth_client() -> TestClient:
    """Build a TestClient with the REAL require_auth dep wired (NOT overridden)."""
    from fastapi import FastAPI
    from api.db import get_db

    app = FastAPI()
    app.include_router(athletes.router)
    # Override DB so the route would return data IF it gets past auth
    app.dependency_overrides[get_db] = lambda: _fake_db_with_rows([("A1",)])
    return TestClient(app, raise_server_exceptions=False)


def test_list_athletes_missing_key_returns_401():
    """sc-1 (api-auth): GET /athletes without X-API-Key → 401, not 422."""
    client = _make_auth_client()
    resp = client.get("/athletes")
    assert resp.status_code == 401, (
        f"Expected 401 when X-API-Key is absent, got {resp.status_code}"
    )


def test_list_athletes_missing_key_not_422():
    """S2 guard: missing key on /athletes must NOT return 422."""
    client = _make_auth_client()
    resp = client.get("/athletes")
    assert resp.status_code != 422, (
        "GET /athletes returned 422 for missing key — Header must use default=None"
    )


def test_list_athletes_missing_key_body_has_detail():
    """S1 guard: 401 body must be JSON with a 'detail' key."""
    client = _make_auth_client()
    resp = client.get("/athletes")
    body = resp.json()
    assert "detail" in body, f"Expected 'detail' in 401 body, got: {body}"


def test_list_athletes_correct_key_returns_200():
    """sc-4: correct X-API-Key → GET /athletes returns 200."""
    client = _make_auth_client()
    resp = client.get("/athletes", headers={"X-API-Key": "test-api-key-fixture"})
    assert resp.status_code == 200, (
        f"Expected 200 with correct key, got {resp.status_code}"
    )
