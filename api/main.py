"""FastAPI application entry point for the AthleteOS API.

Exposes:
  GET /health                    — liveness probe
  GET /athletes/{id}/metrics     — metrics date-range time-series (Domain A)
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import settings
from api.routers import metrics

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
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(metrics.router)


# ---------------------------------------------------------------------------
# Health probe
# ---------------------------------------------------------------------------
@app.get("/health", tags=["ops"])
def health() -> dict:
    """Liveness probe — returns 200 when the process is up."""
    return {"status": "ok"}
