"""Unit tests for api/config.py — api_key required field (sc-9, ADR-A2).

Spec source: obs #314 (sdd/athleteos-api-auth/spec), sc-9.
Design source: obs #315 (sdd/athleteos-api-auth/design), ADR-A2 (fail-closed).

sc-9: When API_KEY env var is absent, Settings() must raise pydantic ValidationError.

This test PROVES fail-closed semantics DESPITE the conftest setdefault provisioning:
  monkeypatch.delenv("API_KEY", raising=False) removes the provisioned value inside
  this test's scope only; monkeypatch auto-reverts after the test.
  With API_KEY genuinely absent at construction time, ValidationError fires.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError


class TestApiKeyRequiredField:
    """api_key must be a REQUIRED field with no default — fail-closed (sc-9, ADR-A2)."""

    def test_missing_api_key_raises_validation_error(self, monkeypatch):
        """sc-9: Settings() with API_KEY absent → ValidationError (fail-closed).

        monkeypatch.delenv removes the conftest-provisioned API_KEY for this
        test's scope only, proving the field is genuinely required at construction
        and that the conftest provision does NOT weaken the fail-closed guarantee.
        """
        monkeypatch.delenv("API_KEY", raising=False)

        from api.config import Settings

        with pytest.raises(ValidationError) as exc_info:
            Settings()

        # The error must reference the api_key field
        error_str = str(exc_info.value)
        assert "api_key" in error_str, (
            f"ValidationError must mention 'api_key', got: {error_str}"
        )

    def test_api_key_present_constructs_successfully(self):
        """Triangulation: Settings() with API_KEY set must not raise."""
        from api.config import Settings

        # conftest setdefault has provisioned API_KEY — this must succeed
        s = Settings()
        assert s.api_key == "test-api-key-fixture", (
            f"Expected api_key 'test-api-key-fixture', got {s.api_key!r}"
        )

    def test_api_key_no_default(self, monkeypatch):
        """ADR-A2: api_key field must have NO default value.

        A field with a default would silently allow startup with no key configured
        (fail-open). Absence of a default means pydantic raises on missing env.
        """
        monkeypatch.delenv("API_KEY", raising=False)

        from api.config import Settings

        # Must raise — no default means absent env = ValidationError
        with pytest.raises(ValidationError):
            Settings()

    def test_api_key_not_none_not_empty(self):
        """api_key from env must be the real string, not None or empty."""
        from api.config import Settings

        s = Settings()
        assert s.api_key is not None, "api_key must not be None"
        assert isinstance(s.api_key, str), f"api_key must be str, got {type(s.api_key)}"
        assert s.api_key, "api_key must not be empty string"
