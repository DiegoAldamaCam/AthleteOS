"""Unit tests for the CORS wildcard guard in api/config.py.

Spec source: obs #98 (sdd/athleteos-hardening/spec), Slice A — CORS Wildcard Guard.
Design source: obs #99, ADR H1: field_validator on cors_origins raises ValueError
    if '*' in parsed origins list AND cors_allow_credentials is True.

TDD cycle:
  RED  — tests reference Settings constructor behaviour that does not exist yet.
  GREEN — minimal validator implementation makes both tests pass.
  REFACTOR — no structural changes needed; code is already clean after GREEN.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Scenario A1: wildcard-with-credentials rejected at startup
# ---------------------------------------------------------------------------


class TestCORSWildcardGuard:
    """Settings must reject '*' in CORS origins when credentials are enabled."""

    def test_wildcard_with_credentials_raises_value_error(self):
        """GIVEN cors_origins='*' AND cors_allow_credentials=True
        WHEN Settings is constructed
        THEN ValueError is raised containing 'wildcard' and 'credentials'.
        """
        from api.config import Settings

        with pytest.raises(ValueError) as exc_info:
            Settings(
                cors_origins="*",
                cors_allow_credentials=True,
            )

        message = str(exc_info.value).lower()
        assert "wildcard" in message, f"Expected 'wildcard' in error, got: {exc_info.value}"
        assert "credentials" in message, f"Expected 'credentials' in error, got: {exc_info.value}"

    def test_wildcard_message_contains_both_keywords_case_insensitive(self):
        """Error message must contain BOTH 'wildcard' AND 'credentials' (case-insensitive).

        Triangulation: verifies the message contract independently from the
        ValueError type check.
        """
        from api.config import Settings

        with pytest.raises(ValueError) as exc_info:
            Settings(
                cors_origins="*",
                cors_allow_credentials=True,
            )

        raw = str(exc_info.value)
        lower = raw.lower()
        assert "wildcard" in lower, f"Message missing 'wildcard': {raw}"
        assert "credentials" in lower, f"Message missing 'credentials': {raw}"


# ---------------------------------------------------------------------------
# Scenario A2: explicit origins accepted at startup
# ---------------------------------------------------------------------------


class TestCORSExplicitOriginsAccepted:
    """Settings must accept an explicit origin list without raising."""

    def test_explicit_origin_with_credentials_constructs_successfully(self):
        """GIVEN cors_origins='http://localhost:5173' AND cors_allow_credentials=True
        WHEN Settings is constructed
        THEN no exception is raised.
        """
        from api.config import Settings

        # Should not raise — explicit origin is safe
        settings = Settings(
            cors_origins="http://localhost:5173",
            cors_allow_credentials=True,
        )
        assert settings.cors_allow_credentials is True
        assert "http://localhost:5173" in settings.cors_origins_list

    def test_multiple_explicit_origins_with_credentials_constructs_successfully(self):
        """Triangulation: comma-separated explicit list also accepted.

        More than one origin with credentials is safe as long as no wildcard
        is present.
        """
        from api.config import Settings

        settings = Settings(
            cors_origins="http://localhost:5173,https://app.example.com",
            cors_allow_credentials=True,
        )
        assert len(settings.cors_origins_list) == 2
        assert "http://localhost:5173" in settings.cors_origins_list
        assert "https://app.example.com" in settings.cors_origins_list

    def test_wildcard_without_credentials_does_not_raise(self):
        """Wildcard is acceptable when credentials are disabled (not our spec scenario).

        This triangulates the AND condition: both wildcard AND credentials must
        be true to trigger the guard.
        """
        from api.config import Settings

        # cors_allow_credentials=False → guard should NOT fire
        settings = Settings(
            cors_origins="*",
            cors_allow_credentials=False,
        )
        assert settings.cors_allow_credentials is False
