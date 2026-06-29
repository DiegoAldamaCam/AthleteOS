"""Unit tests for api/security.py — require_api_key dependency.

Spec source: obs #314 (sdd/athleteos-api-auth/spec), scenarios sc-1..sc-3.
Design source: obs #315 (sdd/athleteos-api-auth/design), ADR-A1, ADR-A3.

Tests run WITHOUT Docker; require_api_key is wired to a minimal FastAPI app.

Scenarios covered:
  sc-1  Missing X-API-Key header → 401, NOT 422 (ADR-A3, design-gate S1/S2)
  sc-2  Wrong key same length → 401
  sc-3  Wrong key different length → 401
  sc-4  Correct key → dependency passes (200 from stub route)
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Depends
from starlette.testclient import TestClient

from api.security import require_api_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_security_client() -> TestClient:
    """Build a TestClient with a stub route that uses the real require_api_key dep."""
    app = FastAPI()

    @app.get("/protected")
    def _protected_route(_=Depends(require_api_key)):
        return {"ok": True}

    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# sc-1 — Missing X-API-Key header → 401, NOT 422 (ADR-A3)
# Design-gate S1: assert JSON body has 'detail' key.
# Design-gate S2: explicitly assert status != 422.
# ---------------------------------------------------------------------------

class TestMissingApiKey:
    """Missing header must yield 401 (not 422) with a JSON detail body."""

    def test_missing_key_returns_401(self):
        """sc-1 + ADR-A3: No X-API-Key header → HTTP 401."""
        client = _make_security_client()
        resp = client.get("/protected")
        assert resp.status_code == 401, (
            f"Expected 401 when X-API-Key is absent, got {resp.status_code}"
        )

    def test_missing_key_not_422(self):
        """S2 guard: missing key must NOT return 422 (FastAPI validation error)."""
        client = _make_security_client()
        resp = client.get("/protected")
        assert resp.status_code != 422, (
            "Missing X-API-Key returned 422 — Header must use default=None, not ..."
        )

    def test_missing_key_response_body_has_detail_key(self):
        """S1 guard: 401 body must be JSON with a 'detail' key."""
        client = _make_security_client()
        resp = client.get("/protected")
        body = resp.json()
        assert "detail" in body, (
            f"Expected JSON body with 'detail' key on 401, got: {body}"
        )

    def test_missing_key_detail_value_is_string(self):
        """S1 triangulation: 'detail' must be a non-empty string."""
        client = _make_security_client()
        resp = client.get("/protected")
        body = resp.json()
        assert isinstance(body.get("detail"), str), (
            f"detail must be a string, got: {body}"
        )
        assert body["detail"], "detail must be non-empty"


# ---------------------------------------------------------------------------
# sc-2 — Wrong key same length → 401 (ADR-A1 timing-safe compare)
# ---------------------------------------------------------------------------

class TestWrongKeyReturns401:
    """Wrong key in both same-length and different-length variants → 401."""

    def test_wrong_key_same_length_returns_401(self):
        """sc-2: Same-length wrong key → 401."""
        client = _make_security_client()
        # Conftest provisions 'test-api-key-fixture' (21 chars).
        # Use a same-length but different string to prove sc-2 coverage.
        wrong_same_len = "XXXX-api-key-fixture"  # different value, same length as conftest key minus 1 char — use exact 20 chars padded
        # Actually 'test-api-key-fixture' is 20 chars. Use same-length wrong key.
        resp = client.get("/protected", headers={"X-API-Key": "XXXX-api-key-fixture"})
        assert resp.status_code == 401, (
            f"Expected 401 for wrong key (same length), got {resp.status_code}"
        )

    def test_wrong_key_different_length_returns_401(self):
        """sc-3: Different-length wrong key → 401."""
        client = _make_security_client()
        resp = client.get("/protected", headers={"X-API-Key": "short"})
        assert resp.status_code == 401, (
            f"Expected 401 for wrong key (different length), got {resp.status_code}"
        )

    def test_wrong_key_body_has_detail(self):
        """S1 triangulation: 401 for wrong key also returns detail body."""
        client = _make_security_client()
        resp = client.get("/protected", headers={"X-API-Key": "wrong-key"})
        body = resp.json()
        assert "detail" in body, f"Expected 'detail' in 401 body, got: {body}"

    def test_wrong_key_not_403_or_500(self):
        """ADR-A3: wrong key must not return 403 or 500 — only 401."""
        client = _make_security_client()
        resp = client.get("/protected", headers={"X-API-Key": "definitely-wrong"})
        assert resp.status_code not in (403, 500), (
            f"Expected 401, not 403/500. Got: {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# sc-4 — Correct key → 200 (dependency passes)
# ---------------------------------------------------------------------------

class TestCorrectKeyPasses:
    """Correct key must let the request through."""

    def test_correct_key_returns_200(self):
        """sc-4 subset: correct key on stub route → 200."""
        client = _make_security_client()
        # Conftest provisioned 'test-api-key-fixture' as the API_KEY
        resp = client.get("/protected", headers={"X-API-Key": "test-api-key-fixture"})
        assert resp.status_code == 200, (
            f"Expected 200 for correct key, got {resp.status_code}"
        )

    def test_correct_key_response_body(self):
        """Triangulation: correct key returns the expected JSON body."""
        client = _make_security_client()
        resp = client.get("/protected", headers={"X-API-Key": "test-api-key-fixture"})
        body = resp.json()
        assert body == {"ok": True}, f"Expected {{ok: True}}, got: {body}"
