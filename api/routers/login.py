"""POST /login endpoint — JWT-based authentication.

Spec: obs #385 (sdd/athleteos-jwt-auth/spec)
  sc-6: valid credentials → 200 + {access_token, token_type: "bearer"}
  sc-7: wrong password → 401 generic "Invalid credentials" (no specifics)
  sc-8: unknown username → 401 same generic message (timing-safe dummy verify)
  sc-9: missing body / missing fields → 422 (FastAPI/Pydantic validation)

Design: obs #386 (sdd/athleteos-jwt-auth/design)
  Login flow:
    parse LoginRequest (sc-9: missing fields → 422)
    SELECT password_hash FROM users WHERE username=%s
    if no row → verify_password(password, _DUMMY_HASH) (discard result) → 401 (sc-8, S2 timing-safe)
    if row → verify_password; fail → 401 generic (sc-7)
    pass → create_access_token(sub=username) → 200 (sc-6)

Security properties:
  - Timing-safe enumeration: unknown username path always calls verify_password against
    _DUMMY_HASH so response time matches the found-but-wrong-password path (sc-8, S2).
  - Generic 401: both wrong-password and unknown-user return the SAME "Invalid credentials"
    message, preventing user enumeration (sc-7, sc-8).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from api.db import get_db
from api.jwt_utils import _DUMMY_HASH, create_access_token, verify_password

router = APIRouter()

_INVALID_CREDENTIALS = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Invalid credentials",
)

_SQL_GET_USER = "SELECT password_hash FROM users WHERE username = %s"


class LoginRequest(BaseModel):
    """Request body for POST /login."""

    username: str
    password: str


@router.post("/login", summary="Authenticate and receive a JWT access token")
def login(body: LoginRequest, db=Depends(get_db)) -> dict:
    """Authenticate a user and issue a signed JWT.

    On any failure (wrong password, unknown user), raise a generic 401 that
    does not distinguish the failure type to prevent user enumeration (sc-7, sc-8).

    sc-9: FastAPI/Pydantic validates the request body; missing fields return 422
    automatically — no explicit check needed here.
    """
    with db.cursor() as cur:
        cur.execute(_SQL_GET_USER, (body.username,))
        row = cur.fetchone()

    if row is None:
        # sc-8: run bcrypt verify against _DUMMY_HASH so timing matches the
        # found-but-wrong-password path; discard the (always-False) result.
        verify_password(body.password, _DUMMY_HASH)
        raise _INVALID_CREDENTIALS

    password_hash: str = row[0]
    if not verify_password(body.password, password_hash):
        # sc-7: wrong password — same generic error as sc-8 (anti-enumeration)
        raise _INVALID_CREDENTIALS

    # sc-6: valid credentials — issue a signed JWT
    token = create_access_token(sub=body.username)
    return {"access_token": token, "token_type": "bearer"}
