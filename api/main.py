"""FastAPI application entry point for the AthleteOS API.

Exposes:
  GET /health                    — readiness probe (DB + Kafka)
  GET /athletes                  — sorted distinct athlete IDs (selector feed)
  GET /athletes/{id}/metrics     — metrics date-range time-series (Domain A)
  GET /pipeline/dlq-depth        — DLQ topic depth health panel (Domain B)
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings
from api.routers import athletes, login, metrics, pipeline
from api.observability import REGISTRY, instrument_app

logger = logging.getLogger("api")

app = FastAPI(
    title="AthleteOS API",
    description="Real-time athlete training-load metrics and pipeline health.",
    version="0.1.0",
)

# ---------------------------------------------------------------------------
# CORS — allow the React SPA origin(s) configured via CORS_ORIGINS env var
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=settings.cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(login.router)
app.include_router(metrics.router)
app.include_router(pipeline.router)
app.include_router(athletes.router)

# ---------------------------------------------------------------------------
# Observability — mount /metrics ASGI app + wire PrometheusMiddleware
# Must be added AFTER routers so middleware wraps all API routes.
# ---------------------------------------------------------------------------
instrument_app(app, REGISTRY)


# ---------------------------------------------------------------------------
# Global exception handler — catches UNHANDLED exceptions only.
#
# Design ADR H2: FastAPI registers its built-in HTTPException and
# RequestValidationError handlers before app code; Starlette dispatches by
# most-specific type first, so a broad Exception handler here does NOT shadow
# HTTPException (404) or RequestValidationError (422). Do NOT register an
# HTTPException handler — it would intercept FastAPI's own 404/422 responses.
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Log unhandled exceptions and return a generic 500 JSON response.

    The response body is intentionally generic — no stack trace, no internal
    error message — to avoid leaking implementation details to callers.
    """
    logger.error(
        "Unhandled exception on %s %s: %s",
        request.method,
        request.url.path,
        exc,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


# ---------------------------------------------------------------------------
# Readiness probe functions (module-level so tests can monkeypatch them)
# ---------------------------------------------------------------------------

def _probe_db() -> None:
    """Verify the PostgreSQL DB is reachable via a lightweight SELECT 1.

    Raises any exception on failure. Module-level function so it can be
    replaced by tests via monkeypatch (or app.dependency_overrides if wired
    as a dependency in the future).
    """
    import psycopg2

    conn = psycopg2.connect(
        settings.database_url,
        connect_timeout=settings.db_connect_timeout_seconds,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    finally:
        conn.close()


def _probe_kafka() -> None:
    """Verify the Kafka broker is reachable via AdminClient.list_topics.

    Reuses the cached AdminClient singleton from kafka_admin (ADR-16 + H5).
    Raises any exception on failure.
    """
    from api.kafka_admin import _get_or_create_admin_client

    admin = _get_or_create_admin_client(
        settings.kafka_bootstrap_servers,
        settings.kafka_admin_request_timeout,
    )
    admin.list_topics(timeout=settings.kafka_admin_request_timeout)


# ---------------------------------------------------------------------------
# Readiness health endpoint (REPLACE the static liveness probe)
#
# Design ADR H4: KEEP single /health; add DB + Kafka readiness.
# 200 → both deps healthy; 503 → at least one dep unreachable.
# ---------------------------------------------------------------------------
@app.get("/health", tags=["ops"])
def health() -> JSONResponse:
    """Readiness probe — returns 200 when both DB and Kafka are reachable.

    Calls _probe_db and _probe_kafka; each is a module-level function that
    tests can monkeypatch for fast unit tests without real containers.
    """
    failed: list[str] = []

    try:
        _probe_db()
    except Exception as exc:
        logger.warning("DB readiness probe failed: %s", exc)
        failed.append("db")

    try:
        _probe_kafka()
    except Exception as exc:
        logger.warning("Kafka readiness probe failed: %s", exc)
        failed.append("kafka")

    if failed:
        body: dict = {"status": "degraded"}
        for dep in failed:
            body[dep] = "unreachable"
        return JSONResponse(status_code=503, content=body)

    return JSONResponse(status_code=200, content={"status": "ok"})
