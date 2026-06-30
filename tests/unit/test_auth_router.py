"""Unit tests for POST /login — api/routers/login.py.

Spec: obs #385 (sdd/athleteos-jwt-auth/spec)
  sc-6: valid credentials → 200 + {access_token, token_type: "bearer"} + decodable JWT
  sc-7: wrong password → 401 generic "Invalid credentials"
  sc-8: unknown username → 401 generic same message (timing-safe dummy verify)
  sc-9: missing body / missing fields → 422

Design: obs #386 (sdd/athleteos-jwt-auth/design)
  Login flow: parse → SELECT password_hash WHERE username;
    if no row → verify_password(password, _DUMMY_HASH) (discard) → 401 generic
    if row → verify_password; fail → 401 generic; pass → create_access_token → 200.

These tests run without Docker; DB is overridden via dependency injection.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import jwt as pyjwt
import pytest
from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers — fake DB dependency
# ---------------------------------------------------------------------------


def _fake_db_returning(rows: list[tuple]) -> MagicMock:
    """Return a fake psycopg2 connection whose cursor.fetchone() yields `rows[0]` or None."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = lambda self: self
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = rows[0] if rows else None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn


def _make_client(db_rows: list[tuple]) -> TestClient:
    """Build a TestClient for the login router with the DB overridden."""
    from fastapi import FastAPI
    from api.db import get_db
    from api.routers import login

    app = FastAPI()
    app.include_router(login.router)
    app.dependency_overrides[get_db] = lambda: _fake_db_returning(db_rows)
    return TestClient(app)


# ---------------------------------------------------------------------------
# sc-6: valid credentials → 200 + access_token + decodable JWT
# ---------------------------------------------------------------------------


class TestLoginValidCredentials:
    """sc-6: valid credentials return 200 with access_token and token_type='bearer'."""

    def _make_client_with_user(self, password: str) -> TestClient:
        """Set up fake DB returning a user row with the given password hashed."""
        from api.jwt_utils import hash_password

        hashed = hash_password(password)
        return _make_client([(hashed,)])  # row = (password_hash,)

    def test_valid_creds_returns_200(self):
        """sc-6 (part 1): valid credentials → HTTP 200."""
        client = self._make_client_with_user("pass123")
        response = client.post("/login", json={"username": "alice", "password": "pass123"})
        assert response.status_code == 200, (
            f"Expected 200 for valid credentials, got {response.status_code}: {response.text}"
        )

    def test_valid_creds_returns_access_token(self):
        """sc-6 (part 2): response body contains access_token (non-empty string)."""
        client = self._make_client_with_user("pass123")
        response = client.post("/login", json={"username": "alice", "password": "pass123"})
        body = response.json()
        assert "access_token" in body, f"Missing 'access_token' in response: {body}"
        assert isinstance(body["access_token"], str) and body["access_token"], (
            f"access_token must be a non-empty string, got: {body['access_token']!r}"
        )

    def test_valid_creds_returns_bearer_token_type(self):
        """sc-6 (part 3): response body has token_type='bearer'."""
        client = self._make_client_with_user("pass123")
        response = client.post("/login", json={"username": "alice", "password": "pass123"})
        body = response.json()
        assert body.get("token_type") == "bearer", (
            f"Expected token_type='bearer', got: {body.get('token_type')!r}"
        )

    def test_valid_creds_token_has_correct_sub(self):
        """sc-6 (part 4): decoded JWT contains sub=username."""
        client = self._make_client_with_user("pass123")
        response = client.post("/login", json={"username": "alice", "password": "pass123"})
        body = response.json()
        secret = os.environ["JWT_SECRET"]
        payload = pyjwt.decode(body["access_token"], secret, algorithms=["HS256"])
        assert payload.get("sub") == "alice", (
            f"Expected sub='alice', got {payload.get('sub')!r}"
        )

    def test_valid_creds_token_has_future_exp(self):
        """sc-6 (part 5): decoded JWT exp is in the future."""
        from datetime import datetime, timezone

        client = self._make_client_with_user("pass123")
        response = client.post("/login", json={"username": "alice", "password": "pass123"})
        body = response.json()
        secret = os.environ["JWT_SECRET"]
        payload = pyjwt.decode(body["access_token"], secret, algorithms=["HS256"])
        now_ts = datetime.now(tz=timezone.utc).timestamp()
        assert payload["exp"] > now_ts, (
            f"Token exp={payload['exp']} must be in the future (now={now_ts:.0f})"
        )


# ---------------------------------------------------------------------------
# sc-7: wrong password → 401 generic
# ---------------------------------------------------------------------------


class TestLoginWrongPassword:
    """sc-7: wrong password → 401 with generic error, no access_token."""

    def test_wrong_password_returns_401(self):
        """sc-7 (part 1): wrong password → HTTP 401."""
        from api.jwt_utils import hash_password

        hashed = hash_password("correct-password")
        client = _make_client([(hashed,)])
        response = client.post("/login", json={"username": "alice", "password": "badpass"})
        assert response.status_code == 401, (
            f"Expected 401 for wrong password, got {response.status_code}"
        )

    def test_wrong_password_response_has_no_access_token(self):
        """sc-7 (part 2): 401 response must not contain access_token."""
        from api.jwt_utils import hash_password

        hashed = hash_password("correct-password")
        client = _make_client([(hashed,)])
        response = client.post("/login", json={"username": "alice", "password": "badpass"})
        body = response.json()
        assert "access_token" not in body, (
            f"401 response must not contain access_token, got: {body}"
        )

    def test_wrong_password_error_message_is_generic(self):
        """sc-7 (part 3): error message is generic — must not say 'wrong password'."""
        from api.jwt_utils import hash_password

        hashed = hash_password("correct-password")
        client = _make_client([(hashed,)])
        response = client.post("/login", json={"username": "alice", "password": "badpass"})
        detail = response.json().get("detail", "")
        assert "Invalid credentials" in detail or detail == "Invalid credentials", (
            f"Expected generic 'Invalid credentials' message, got: {detail!r}"
        )
        assert "wrong" not in detail.lower(), (
            f"Error must not say 'wrong', got: {detail!r}"
        )
        assert "password" not in detail.lower(), (
            f"Error must not mention 'password', got: {detail!r}"
        )


# ---------------------------------------------------------------------------
# sc-8: unknown username → 401 generic, timing-safe
# ---------------------------------------------------------------------------


class TestLoginUnknownUser:
    """sc-8: unknown username → 401 with same generic message as sc-7."""

    def test_unknown_user_returns_401(self):
        """sc-8 (part 1): unknown username → HTTP 401."""
        # DB returns no row (empty list)
        client = _make_client([])
        response = client.post("/login", json={"username": "ghost", "password": "anypass"})
        assert response.status_code == 401, (
            f"Expected 401 for unknown user, got {response.status_code}"
        )

    def test_unknown_user_error_message_identical_to_wrong_password(self):
        """sc-8 (part 2): sc-7 and sc-8 MUST return the same error message (anti-enumeration)."""
        from api.jwt_utils import hash_password

        hashed = hash_password("correct-password")
        # sc-7 case: user found, wrong password
        client_found = _make_client([(hashed,)])
        resp_wrong_pw = client_found.post(
            "/login", json={"username": "alice", "password": "badpass"}
        )
        # sc-8 case: user not found
        client_unknown = _make_client([])
        resp_unknown = client_unknown.post(
            "/login", json={"username": "ghost", "password": "anypass"}
        )
        assert resp_wrong_pw.json().get("detail") == resp_unknown.json().get("detail"), (
            "sc-7 and sc-8 must return identical error messages to prevent user enumeration. "
            f"sc-7: {resp_wrong_pw.json().get('detail')!r}, "
            f"sc-8: {resp_unknown.json().get('detail')!r}"
        )


# ---------------------------------------------------------------------------
# sc-9: malformed / missing body → 422
# ---------------------------------------------------------------------------


class TestLoginMissingBody:
    """sc-9: missing body or missing required fields → 422."""

    def test_no_body_returns_422(self):
        """sc-9 (part 1): no body → HTTP 422."""
        client = _make_client([])
        response = client.post("/login")
        assert response.status_code == 422, (
            f"Expected 422 for missing body, got {response.status_code}"
        )

    def test_missing_password_returns_422(self):
        """sc-9 (part 2): body with only username → 422."""
        client = _make_client([])
        response = client.post("/login", json={"username": "alice"})
        assert response.status_code == 422, (
            f"Expected 422 for missing password, got {response.status_code}"
        )

    def test_missing_username_returns_422(self):
        """sc-9 (part 3): body with only password → 422."""
        client = _make_client([])
        response = client.post("/login", json={"password": "secret"})
        assert response.status_code == 422, (
            f"Expected 422 for missing username, got {response.status_code}"
        )
