"""API key guard dependency for protected FastAPI routes.

Spec: obs #314 (sdd/athleteos-api-auth/spec), sc-1..sc-6.
Design: obs #315 (sdd/athleteos-api-auth/design), ADR-A1, ADR-A2, ADR-A3.

Security properties:
  - ADR-A1: Single secrets.compare_digest call — NO == comparison, no length
             pre-check, no early-return branch that leaks timing information.
  - ADR-A2: Reads api_key from api.config.settings (REQUIRED field, no default).
  - ADR-A3: X-API-Key declared Optional with default=None so a missing header
             reaches THIS code and raises 401 — NOT FastAPI's 422 validation path.
"""

from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from api.config import settings


async def require_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Validate the X-API-Key request header against the configured API key.

    ADR-A3: header declared Optional with default=None → missing header reaches
    this function and raises 401 explicitly (NOT FastAPI's 422 validation path).

    ADR-A1: single timing-safe compare_digest call — no == operator, no length
    pre-check, no short-circuit branch that could leak timing information.
    """
    # ADR-A3: explicit None check raises 401, never 422
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
        )
    # ADR-A1: single constant-time comparison — NO == fallback
    if not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
