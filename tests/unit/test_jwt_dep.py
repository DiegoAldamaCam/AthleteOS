"""Unit tests for require_auth coexistence dependency (Phase 7, sc-10..sc-16).

Design: obs #386 (sdd/athleteos-jwt-auth/design), ADR-4.
Tasks: obs #388, Phase 7, tasks 7.1–7.2.

Security scenarios:
  sc-10: valid Bearer JWT → 200
  sc-11: expired JWT → 401
  sc-12: tampered JWT → 401
  sc-13: malformed Authorization header → 401
  W1:    empty-string Authorization: header → 401 (NOT X-API-Key fallback)
  sc-14: no Authorization + valid X-API-Key → 200
  sc-15: no Authorization + invalid X-API-Key → 401
  sc-16: valid JWT + no X-API-Key → 200

ADR-4: Bearer is authoritative — invalid Bearer NEVER falls through to X-API-Key.
W1: trigger is `authorization is not None` (NOT truthy) so empty-string → 401.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

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
_BAD_API_KEY = "totally-wrong-key"


def _make_token(sub: str = "athlete_user", *, expired: bool = False, tampered: bool = False) -> str:
    """Create a test JWT token (valid, expired, or tampered).

    Tamper strategy: replace the entire signature segment (third JWT part) with
    a fixed garbage base64url string. A JWT has three dot-separated parts:
      header.payload.signature
    Replacing the signature with a different string of the same apparent length
    guarantees that signature verification fails — regardless of which characters
    happen to be valid base64url.
    """
    if expired:
        exp = datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    else:
        exp = datetime.now(tz=timezone.utc) + timedelta(minutes=60)
    payload = {"sub": sub, "exp": exp}
    token = jwt.encode(payload, TEST_SECRET, algorithm=TEST_ALGORITHM)
    if tampered:
        # Split into header.payload.signature and replace signature entirely
        parts = token.split(".")
        if len(parts) == 3:
            # Use a fixed garbage signature of identical length
            original_sig = parts[2]
            # XOR every character's ordinal with 1 to guarantee mutation
            garbage_sig = "".join(
                chr(ord(c) ^ 1) if c.isalpha() else ("9" if c.isdigit() else "_")
                for c in original_sig
            )
            # Ensure at least one character actually changed (defensive)
            if garbage_sig == original_sig:
                garbage_sig = original_sig[:-3] + "AAA"
            token = ".".join([parts[0], parts[1], garbage_sig])
    return token


def _make_client() -> TestClient:
    """Build a fresh TestClient wired with require_auth each call.

    Building per-call avoids module-level state pollution when other test
    files trigger importlib.reload(api.security) during the same session.
    """
    from api.security import require_auth  # fresh import every call
    from fastapi import Depends

    app = FastAPI()

    @app.get("/protected")
    async def protected_endpoint(auth=Depends(require_auth)):  # noqa: B008
        return {"status": "ok"}

    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# sc-10: valid Bearer JWT → 200
# ---------------------------------------------------------------------------

class TestValidBearerJwt:
    """sc-10: A request with a valid Bearer JWT must receive HTTP 200."""

    def test_valid_bearer_returns_200(self) -> None:
        client = _make_client()
        token = _make_token()
        response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200

    def test_valid_bearer_returns_ok_body(self) -> None:
        client = _make_client()
        token = _make_token()
        response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert response.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# sc-11: expired JWT → 401
# ---------------------------------------------------------------------------

class TestExpiredJwt:
    """sc-11: An expired Bearer token must return HTTP 401."""

    def test_expired_token_returns_401(self) -> None:
        client = _make_client()
        token = _make_token(expired=True)
        response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 401

    def test_expired_token_does_not_fall_through_to_api_key(self) -> None:
        """ADR-4: expired Bearer MUST NOT fall through to X-API-Key — still 401."""
        client = _make_client()
        token = _make_token(expired=True)
        response = client.get(
            "/protected",
            headers={
                "Authorization": f"Bearer {token}",
                "X-API-Key": _VALID_API_KEY,
            },
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# sc-12: tampered JWT → 401
# ---------------------------------------------------------------------------

class TestTamperedJwt:
    """sc-12: A tampered JWT signature must return HTTP 401."""

    def test_tampered_token_returns_401(self) -> None:
        client = _make_client()
        token = _make_token(tampered=True)
        response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 401

    def test_tampered_token_does_not_fall_through_to_api_key(self) -> None:
        """ADR-4: tampered Bearer MUST NOT fall through to X-API-Key — still 401."""
        client = _make_client()
        token = _make_token(tampered=True)
        response = client.get(
            "/protected",
            headers={
                "Authorization": f"Bearer {token}",
                "X-API-Key": _VALID_API_KEY,
            },
        )
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# sc-13 / W1: malformed or empty-string Authorization header → 401
# ---------------------------------------------------------------------------

class TestMalformedAuthorizationHeader:
    """sc-13/W1: Any non-Bearer (or empty) Authorization header → 401.

    W1 critical: Authorization: "" (empty string) → 401 (NOT X-API-Key fallback).
    The trigger in require_auth MUST be `authorization is not None`, NOT `if authorization`.
    An empty string is not None, so it must be treated as a present-but-invalid header.
    """

    def test_empty_string_authorization_returns_401(self) -> None:
        """W1: empty-string Authorization header → 401, never falls through."""
        client = _make_client()
        response = client.get(
            "/protected",
            headers={"Authorization": ""},
        )
        assert response.status_code == 401

    def test_empty_string_authorization_with_valid_api_key_still_401(self) -> None:
        """W1 + ADR-4: empty Authorization present → 401 even with valid X-API-Key."""
        client = _make_client()
        response = client.get(
            "/protected",
            headers={
                "Authorization": "",
                "X-API-Key": _VALID_API_KEY,
            },
        )
        assert response.status_code == 401

    def test_wrong_scheme_returns_401(self) -> None:
        """sc-13: 'Token abc' is not 'Bearer <token>' → 401."""
        client = _make_client()
        response = client.get("/protected", headers={"Authorization": "Token abc123"})
        assert response.status_code == 401

    def test_basic_scheme_returns_401(self) -> None:
        """sc-13: 'Basic ...' is not a Bearer token → 401."""
        client = _make_client()
        response = client.get("/protected", headers={"Authorization": "Basic dXNlcjpwYXNz"})
        assert response.status_code == 401

    def test_bearer_without_token_returns_401(self) -> None:
        """sc-13: 'Bearer' alone (empty token part) → 401."""
        client = _make_client()
        response = client.get("/protected", headers={"Authorization": "Bearer"})
        assert response.status_code == 401

    def test_bearer_with_only_whitespace_returns_401(self) -> None:
        """sc-13: 'Bearer   ' (whitespace token) → 401."""
        client = _make_client()
        response = client.get("/protected", headers={"Authorization": "Bearer   "})
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# sc-14: no Authorization + valid X-API-Key → 200
# ---------------------------------------------------------------------------

class TestXApiKeyFallback:
    """sc-14/sc-15: When no Authorization header is present, fall back to X-API-Key."""

    def test_no_auth_header_with_valid_api_key_returns_200(self) -> None:
        """sc-14: X-API-Key coexistence — no Bearer header → fall through to key check."""
        client = _make_client()
        response = client.get("/protected", headers={"X-API-Key": _VALID_API_KEY})
        assert response.status_code == 200

    def test_no_auth_header_with_invalid_api_key_returns_401(self) -> None:
        """sc-15: invalid X-API-Key with no Bearer header → 401."""
        client = _make_client()
        response = client.get("/protected", headers={"X-API-Key": _BAD_API_KEY})
        assert response.status_code == 401

    def test_no_credentials_at_all_returns_401(self) -> None:
        """sc-15 edge: no Authorization, no X-API-Key → 401."""
        client = _make_client()
        response = client.get("/protected")
        assert response.status_code == 401


# ---------------------------------------------------------------------------
# sc-16: valid JWT + no X-API-Key → 200
# ---------------------------------------------------------------------------

class TestJwtOnlyNoApiKey:
    """sc-16: A valid Bearer JWT is sufficient even with no X-API-Key header."""

    def test_valid_jwt_without_api_key_returns_200(self) -> None:
        """sc-16: Bearer JWT-only authentication path works independently."""
        client = _make_client()
        token = _make_token()
        response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})
        # No X-API-Key header supplied
        assert response.status_code == 200
