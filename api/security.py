"""API key guard and JWT coexistence dependency for protected FastAPI routes.

Spec: obs #314 (sdd/athleteos-api-auth/spec), sc-1..sc-6.
      obs #385 (sdd/athleteos-jwt-auth/spec), sc-10..sc-16.
Design: obs #315 (sdd/athleteos-api-auth/design), ADR-A1, ADR-A2, ADR-A3.
        obs #386 (sdd/athleteos-jwt-auth/design), ADR-4.

Security properties:
  - ADR-A1: Single secrets.compare_digest call — NO == comparison, no length
             pre-check, no early-return branch that leaks timing information.
  - ADR-A2: Reads api_key from api.config.settings (REQUIRED field, no default).
  - ADR-A3: X-API-Key declared Optional with default=None so a missing header
             reaches THIS code and raises 401 — NOT FastAPI's 422 validation path.
  - ADR-4:  Bearer is authoritative — if Authorization header is present (even
             empty string), JWT MUST verify or → 401. No silent fallback to
             X-API-Key on any invalid Bearer (sc-11/12/13, W1).
  - W1:     Trigger is `authorization is not None` (NOT truthy) so an empty-string
             Authorization header is treated as present and yields 401 (sc-13).
"""

from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from api.config import settings
from api.jwt_utils import decode_access_token


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


async def require_auth(
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Coexistence auth dependency: prefers Bearer JWT, falls back to X-API-Key.

    Algorithm (ADR-4, W1):

    Step 1 — Bearer path (authoritative):
        Trigger: `authorization is not None`  ← W1: NOT truthy; catches empty string.
        Parse: split into [scheme, token]; if scheme != "bearer" or token empty → 401.
        Verify: decode_access_token(token) → HTTPException(401) on any error.
        Return: success. NEVER fall through to X-API-Key (ADR-4).

    Step 2 — X-API-Key fallback (only when Authorization is absent):
        Reuse require_api_key logic with secrets.compare_digest (ADR-A1).
        None or mismatch → 401; match → return.

    sc-10: valid Bearer → 200
    sc-11: expired Bearer → 401 (no fallback)
    sc-12: tampered Bearer → 401 (no fallback)
    sc-13/W1: malformed or empty-string Authorization → 401 (no fallback)
    sc-14: no Authorization + valid X-API-Key → 200
    sc-15: no Authorization + invalid X-API-Key → 401
    sc-16: valid Bearer + no X-API-Key → 200
    """
    # Step 1: Authorization header present (even empty string) → Bearer path only
    if authorization is not None:
        # Parse scheme and token — split on first space only
        parts = authorization.split(" ", 1)
        scheme = parts[0].lower() if parts else ""
        token = parts[1].strip() if len(parts) > 1 else ""

        if scheme != "bearer" or not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid Authorization header",
            )
        # decode_access_token raises HTTPException(401) on any jwt.InvalidTokenError
        decode_access_token(token)
        return  # ADR-4: authoritative — never fall through

    # Step 2: No Authorization header — fall back to X-API-Key (ADR-A1, ADR-A3)
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
        )
    if not secrets.compare_digest(x_api_key, settings.api_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
