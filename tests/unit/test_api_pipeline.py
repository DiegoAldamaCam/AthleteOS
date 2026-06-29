"""Unit tests for GET /pipeline/dlq-depth — authentication scenarios (sc-1, sc-6).

Spec source: obs #314 (sdd/athleteos-api-auth/spec), sc-1, sc-6.
Design source: obs #315 (sdd/athleteos-api-auth/design), ADR-A3.

Tests run WITHOUT Docker, real Kafka, or real DB. The require_api_key dep is
wired as-is (NOT overridden) for missing-key tests; overridden for happy-path.

Scenarios covered:
  sc-1 (api-auth) — missing X-API-Key on /pipeline/dlq-depth → 401, NOT 422 (S1, S2)
  sc-6 — correct X-API-Key → GET /pipeline/dlq-depth returns 200
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI, Depends
from starlette.testclient import TestClient

from api.routers import pipeline
from api.security import require_api_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_DLQ_RESULT = {
    "broker_reachable": True,
    "topics": [
        {"topic": "dlq.canonical.training_event", "depth": 0, "status": "ok"},
    ],
}


def _make_pipeline_client_with_auth() -> TestClient:
    """TestClient with REAL require_api_key wired on the pipeline router."""
    app = FastAPI()
    app.include_router(pipeline.router)
    return TestClient(app, raise_server_exceptions=False)


def _make_pipeline_client_auth_overridden() -> TestClient:
    """TestClient with require_api_key overridden to always pass (happy-path)."""
    app = FastAPI()
    app.include_router(pipeline.router)
    app.dependency_overrides[require_api_key] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# sc-1 (api-auth) — Missing X-API-Key header → 401, NOT 422 (ADR-A3)
# Design-gate S1/S2: assert JSON body + explicit != 422
# ---------------------------------------------------------------------------

class TestDlqDepthMissingKey:
    """Missing header on /pipeline/dlq-depth must yield 401 (not 422)."""

    def test_dlq_depth_missing_key_returns_401(self):
        """sc-1: GET /pipeline/dlq-depth without X-API-Key → 401."""
        client = _make_pipeline_client_with_auth()
        with patch("api.routers.pipeline.get_dlq_depths", return_value=_FAKE_DLQ_RESULT):
            resp = client.get("/pipeline/dlq-depth")
        assert resp.status_code == 401, (
            f"Expected 401 when X-API-Key is absent, got {resp.status_code}"
        )

    def test_dlq_depth_missing_key_not_422(self):
        """S2 guard: missing key must NOT return 422."""
        client = _make_pipeline_client_with_auth()
        with patch("api.routers.pipeline.get_dlq_depths", return_value=_FAKE_DLQ_RESULT):
            resp = client.get("/pipeline/dlq-depth")
        assert resp.status_code != 422, (
            "GET /pipeline/dlq-depth returned 422 for missing key — must be 401"
        )

    def test_dlq_depth_missing_key_body_has_detail(self):
        """S1 guard: 401 body must be JSON with a 'detail' key."""
        client = _make_pipeline_client_with_auth()
        with patch("api.routers.pipeline.get_dlq_depths", return_value=_FAKE_DLQ_RESULT):
            resp = client.get("/pipeline/dlq-depth")
        body = resp.json()
        assert "detail" in body, f"Expected 'detail' in 401 body, got: {body}"


# ---------------------------------------------------------------------------
# sc-6 — Correct key → /pipeline/dlq-depth returns 200
# ---------------------------------------------------------------------------

class TestDlqDepthCorrectKey:
    """Correct key on /pipeline/dlq-depth → 200."""

    def test_dlq_depth_correct_key_returns_200(self):
        """sc-6: correct X-API-Key → 200 from /pipeline/dlq-depth."""
        client = _make_pipeline_client_auth_overridden()
        with patch("api.routers.pipeline.get_dlq_depths", return_value=_FAKE_DLQ_RESULT):
            resp = client.get("/pipeline/dlq-depth")
        assert resp.status_code == 200, (
            f"Expected 200 with auth overridden, got {resp.status_code}"
        )

    def test_dlq_depth_correct_key_real_dep(self):
        """sc-6 triangulation: real dep with correct key → 200."""
        client = _make_pipeline_client_with_auth()
        with patch("api.routers.pipeline.get_dlq_depths", return_value=_FAKE_DLQ_RESULT):
            resp = client.get(
                "/pipeline/dlq-depth",
                headers={"X-API-Key": "test-api-key-fixture"},
            )
        assert resp.status_code == 200, (
            f"Expected 200 with correct key, got {resp.status_code}"
        )
