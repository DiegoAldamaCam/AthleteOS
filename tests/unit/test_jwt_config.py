"""Unit tests for JWT configuration fields in api/config.py.

Spec: obs #385 (sdd/athleteos-jwt-auth/spec)
  sc-17: JWT_SECRET absent → ValidationError at startup (fail-closed)
  sc-18: JWT_SECRET present → Settings loads successfully

Design: obs #386 (sdd/athleteos-jwt-auth/design)
  ADR-5: jwt_secret REQUIRED, no default; mirrors api_key fail-closed pattern.

Test design note: mirrors TestApiKeyRequiredField pattern from test_config.py.
  Uses importlib.reload(api.config) inside pytest.raises to prove module-level
  Settings() raises deterministically, regardless of sys.modules cache state.
  The restore_config fixture (autouse, function-scoped) reloads api.config after
  each test to undo any reload side-effects.
"""

from __future__ import annotations

import importlib

import pytest
from pydantic import ValidationError


@pytest.fixture(autouse=True)
def restore_config():
    """Reload api.config after every test to undo any reload side-effects."""
    import api.config  # ensure module is in sys.modules before yield

    yield
    importlib.reload(api.config)


class TestJwtSecretRequiredField:
    """jwt_secret must be a REQUIRED field with no default — fail-closed (sc-17, ADR-5)."""

    def test_missing_jwt_secret_raises_validation_error(self, monkeypatch):
        """sc-17: Settings() with JWT_SECRET absent → ValidationError (fail-closed).

        Uses importlib.reload(api.config) inside pytest.raises to prove the
        module-level Settings() raises regardless of whether api.config was
        already cached in sys.modules.
        """
        import api.config

        monkeypatch.delenv("JWT_SECRET", raising=False)

        with pytest.raises(ValidationError) as exc_info:
            importlib.reload(api.config)

        error_str = str(exc_info.value)
        assert "jwt_secret" in error_str, (
            f"ValidationError must mention 'jwt_secret', got: {error_str}"
        )

    def test_jwt_secret_present_constructs_successfully(self):
        """sc-18: Settings() with JWT_SECRET set must not raise."""
        from api.config import Settings

        # conftest setdefault has provisioned JWT_SECRET — this must succeed
        s = Settings()
        assert s.jwt_secret == "test-jwt-secret-fixture", (
            f"Expected jwt_secret 'test-jwt-secret-fixture', got {s.jwt_secret!r}"
        )

    def test_jwt_secret_no_default(self, monkeypatch):
        """ADR-5: jwt_secret field must have NO default value.

        A field with a default would silently allow startup with no secret.
        Absence of a default means pydantic raises on missing env.
        """
        import api.config

        monkeypatch.delenv("JWT_SECRET", raising=False)

        with pytest.raises(ValidationError):
            importlib.reload(api.config)

    def test_jwt_algorithm_default(self):
        """jwt_algorithm defaults to 'HS256' when not overridden."""
        from api.config import Settings

        s = Settings()
        assert s.jwt_algorithm == "HS256", (
            f"Expected jwt_algorithm 'HS256', got {s.jwt_algorithm!r}"
        )

    def test_jwt_expiry_minutes_default(self):
        """jwt_expiry_minutes defaults to 60 when not overridden."""
        from api.config import Settings

        s = Settings()
        assert s.jwt_expiry_minutes == 60, (
            f"Expected jwt_expiry_minutes 60, got {s.jwt_expiry_minutes!r}"
        )
