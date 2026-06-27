"""Application settings loaded from environment variables.

Uses pydantic-settings for env-driven configuration with sensible defaults
for local development. All production overrides are supplied via docker-compose
environment blocks or CI secrets.
"""

from __future__ import annotations

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """FastAPI application settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://athleteos:athleteos@localhost:5432/athleteos"
    kafka_bootstrap_servers: str = "localhost:9092"
    cors_origins: str = "http://localhost:5173"
    cors_allow_credentials: bool = True
    kafka_admin_request_timeout: float = 5.0  # seconds; env var: KAFKA_ADMIN_REQUEST_TIMEOUT
    db_connect_timeout: float = 5.0  # seconds; env var: DB_CONNECT_TIMEOUT

    @property
    def cors_origins_list(self) -> list[str]:
        """Return CORS origins as a list (comma-separated string → list)."""
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]

    @property
    def db_connect_timeout_seconds(self) -> int:
        """psycopg2-safe connect timeout in whole seconds (minimum 1).

        libpq requires an integer and treats 0 as "wait indefinitely". A naive
        int(0.5) == 0 would silently re-open the unbounded-wait hole this timeout
        exists to close, so we clamp to a minimum of 1 second.
        """
        return max(1, round(self.db_connect_timeout))

    @model_validator(mode="after")
    def _reject_wildcard_with_credentials(self) -> "Settings":
        """Reject '*' in CORS origins when credentials are enabled.

        A wildcard origin combined with allow_credentials=True violates the
        CORS spec (browsers refuse such responses) and is a security risk.
        Fail fast at startup so misconfiguration is caught immediately.
        """
        origins = self.cors_origins_list
        if "*" in origins and self.cors_allow_credentials:
            raise ValueError(
                "CORS wildcard origin ('*') cannot be used with credentials enabled. "
                "Set CORS_ORIGINS to explicit origin(s) or disable credentials."
            )
        return self


# Module-level singleton — imported everywhere via `from api.config import settings`
settings = Settings()
