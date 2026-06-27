"""Application settings loaded from environment variables.

Uses pydantic-settings for env-driven configuration with sensible defaults
for local development. All production overrides are supplied via docker-compose
environment blocks or CI secrets.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """FastAPI application settings."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://athleteos:athleteos@localhost:5432/athleteos"
    kafka_bootstrap_servers: str = "localhost:9092"
    cors_origins: str = "http://localhost:5173"
    kafka_admin_request_timeout: float = 5.0  # seconds; env var: KAFKA_ADMIN_REQUEST_TIMEOUT

    @property
    def cors_origins_list(self) -> list[str]:
        """Return CORS origins as a list (comma-separated string → list)."""
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


# Module-level singleton — imported everywhere via `from api.config import settings`
settings = Settings()
