"""JWT and password utilities for the AthleteOS authentication system.

Design: obs #386 (sdd/athleteos-jwt-auth/design), ADR-2, ADR-3, ADR-6, S1, S2.

Security properties:
  - ADR-2: Uses PyJWT >=2.8,<3; algorithms= is pinned on decode() — no alg=none bypass.
  - ADR-3: passlib CryptContext(schemes=["bcrypt"]) for password hashing/verification.
  - ADR-6: JWT helpers isolated in this module (not api/security.py).
  - S1: decode_access_token catches the broad jwt.InvalidTokenError (covers expired,
         bad-signature, malformed) and re-raises as HTTPException 401.
  - S2: _DUMMY_HASH is a precomputed constant (not hashed at runtime) for genuine
         timing parity in the unknown-user path (sc-8).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
from fastapi import HTTPException, status
from passlib.context import CryptContext

from api.config import settings

# ---------------------------------------------------------------------------
# Password context (bcrypt, 12 rounds by default)
# ---------------------------------------------------------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ---------------------------------------------------------------------------
# Precomputed bcrypt dummy hash — S2 timing-safe enumeration (sc-8).
# This constant is the hash of a random internal string; it is NEVER the hash
# of any real user password. Its purpose is to run bcrypt.verify() against it
# when a username is not found, so the response time matches the found-but-wrong-
# password path and timing analysis cannot distinguish the two cases.
#
# Generated once: CryptContext(schemes=["bcrypt"]).hash("dummy-user-not-found-bcrypt-timing-parity")
# DO NOT replace with a hash_password() call — that would compute at import time
# and add startup latency; it also changes on every deploy, breaking S2 guarantee.
# ---------------------------------------------------------------------------
_DUMMY_HASH: str = "$2b$12$9AvvT5WK1b9lMtX8d441qe/Ed9bC9GNW4dzPSj/GgsOaah.KgG/Zi"


def hash_password(plain: str) -> str:
    """Hash a plaintext password using bcrypt via passlib CryptContext.

    sc-3: returned hash starts with '$2b$' (bcrypt) and is not equal to plain.
    """
    return pwd_context.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    """Verify a plaintext password against a bcrypt hash.

    sc-4: returns True when plain matches the hash.
    sc-5: returns False when plain does not match.
    """
    return pwd_context.verify(plain, hashed)


def create_access_token(sub: str) -> str:
    """Create a signed JWT access token for the given subject (username).

    The token contains:
      - sub: the provided username
      - exp: now + jwt_expiry_minutes (from Settings)

    Signed with settings.jwt_secret using settings.jwt_algorithm (default HS256).
    """
    expiry = datetime.now(tz=timezone.utc) + timedelta(minutes=settings.jwt_expiry_minutes)
    payload = {"sub": sub, "exp": expiry}
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def decode_access_token(token: str) -> dict:
    """Decode and verify a JWT access token.

    S1: catches the broad jwt.InvalidTokenError (which covers ExpiredSignatureError,
    InvalidSignatureError, DecodeError, etc.) and re-raises as HTTPException 401.
    This ensures sc-11 (expired), sc-12 (tampered), sc-13 (malformed) all reach
    the same 401 path without enumerating individual exception subclasses.

    algorithms= is pinned to [settings.jwt_algorithm] — no alg=none bypass (ADR-2).
    """
    try:
        return jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
    except jwt.InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        ) from exc
