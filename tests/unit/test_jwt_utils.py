"""Unit tests for api/jwt_utils.py — bcrypt helpers + JWT creation/verification.

Spec: obs #385 (sdd/athleteos-jwt-auth/spec)
  sc-3: Stored hash starts with '$2b$' and is not equal to plaintext
  sc-4: Correct password verifies → True
  sc-5: Wrong password fails verify → False
  + create_access_token returns decodable JWT with sub and future exp
  + decode_access_token with expired token raises
  + decode_access_token with tampered token raises

Design: obs #386 (sdd/athleteos-jwt-auth/design)
  ADR-2: PyJWT >=2.8,<3 for JWT creation/verification
  ADR-3: passlib CryptContext(schemes=["bcrypt"])
  ADR-6: jwt helpers live in api/jwt_utils.py
  S1: catch broad jwt.InvalidTokenError
  S2: _DUMMY_HASH is a precomputed constant (not runtime-computed)
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import jwt
import pytest


class TestPasswordHashing:
    """bcrypt hash/verify round-trip (sc-3, sc-4, sc-5)."""

    def test_hash_is_not_plaintext(self):
        """sc-3: stored hash starts with '$2b$' and is not equal to plaintext."""
        from api.jwt_utils import hash_password

        hashed = hash_password("secret")
        assert hashed.startswith("$2b$"), f"Expected bcrypt prefix '$2b$', got: {hashed[:10]!r}"
        assert hashed != "secret", "Stored hash must not equal plaintext"

    def test_correct_password_verifies(self):
        """sc-4: correct password verifies against its hash → True."""
        from api.jwt_utils import hash_password, verify_password

        hashed = hash_password("secret")
        assert verify_password("secret", hashed) is True

    def test_wrong_password_fails_verify(self):
        """sc-5: wrong password against a bcrypt hash → False."""
        from api.jwt_utils import hash_password, verify_password

        hashed = hash_password("secret")
        assert verify_password("wrong", hashed) is False

    def test_dummy_hash_is_string_constant(self):
        """S2: _DUMMY_HASH is a precomputed constant, not computed at runtime."""
        from api.jwt_utils import _DUMMY_HASH

        assert isinstance(_DUMMY_HASH, str), "_DUMMY_HASH must be a str constant"
        assert _DUMMY_HASH.startswith("$2b$"), (
            f"_DUMMY_HASH must be a valid bcrypt hash starting with '$2b$', got: {_DUMMY_HASH[:10]!r}"
        )

    def test_dummy_hash_verifies_correctly(self):
        """_DUMMY_HASH must be verifiable (real bcrypt, not a stub)."""
        from api.jwt_utils import verify_password, _DUMMY_HASH

        # Any wrong password should return False, proving it is a real hash
        result = verify_password("not-the-dummy-password", _DUMMY_HASH)
        assert result is False, "_DUMMY_HASH must behave like a real bcrypt hash"


class TestJwtTokenCreation:
    """create_access_token returns a valid JWT with sub and future exp."""

    def test_create_token_returns_string(self):
        """create_access_token must return a non-empty string."""
        from api.jwt_utils import create_access_token

        token = create_access_token("alice")
        assert isinstance(token, str), f"Expected str, got {type(token)}"
        assert token, "Token must not be empty"

    def test_create_token_contains_sub_claim(self):
        """Token must decode to a payload containing sub=username."""
        import os
        import jwt as pyjwt
        from api.jwt_utils import create_access_token

        secret = os.environ["JWT_SECRET"]
        token = create_access_token("alice")
        payload = pyjwt.decode(token, secret, algorithms=["HS256"])
        assert payload["sub"] == "alice", (
            f"Expected sub='alice', got {payload.get('sub')!r}"
        )

    def test_create_token_contains_future_exp(self):
        """Token must contain an exp claim set in the future."""
        import os
        import jwt as pyjwt
        from api.jwt_utils import create_access_token

        secret = os.environ["JWT_SECRET"]
        token = create_access_token("bob")
        payload = pyjwt.decode(token, secret, algorithms=["HS256"])
        exp = payload.get("exp")
        assert exp is not None, "Token must have an exp claim"
        now_ts = datetime.now(tz=timezone.utc).timestamp()
        assert exp > now_ts, f"exp={exp} must be in the future (now={now_ts:.0f})"

    def test_different_users_get_different_tokens(self):
        """Triangulation: tokens for different users must differ."""
        from api.jwt_utils import create_access_token

        token_alice = create_access_token("alice")
        token_bob = create_access_token("bob")
        assert token_alice != token_bob, "Different usernames must produce different tokens"


class TestJwtTokenVerification:
    """decode_access_token validates signature and expiry."""

    def test_valid_token_decodes_successfully(self):
        """decode_access_token returns payload for a freshly-created token."""
        from api.jwt_utils import create_access_token, decode_access_token

        token = create_access_token("charlie")
        payload = decode_access_token(token)
        assert payload["sub"] == "charlie"

    def test_expired_token_raises(self):
        """S1: expired JWT raises (maps to jwt.InvalidTokenError) → 401 path."""
        import os
        import jwt as pyjwt
        from datetime import timedelta
        from fastapi import HTTPException
        from api.jwt_utils import decode_access_token
        from api.config import settings

        past_exp = datetime.now(tz=timezone.utc) - timedelta(seconds=10)
        expired_token = pyjwt.encode(
            {"sub": "alice", "exp": past_exp},
            settings.jwt_secret,
            algorithm=settings.jwt_algorithm,
        )
        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(expired_token)
        assert exc_info.value.status_code == 401

    def test_tampered_token_raises(self):
        """S1: token with bad signature raises HTTPException 401."""
        from fastapi import HTTPException
        from api.jwt_utils import create_access_token, decode_access_token

        token = create_access_token("alice")
        # corrupt the signature section
        parts = token.split(".")
        tampered = parts[0] + "." + parts[1] + "." + "invalidsignatureXXX"
        with pytest.raises(HTTPException) as exc_info:
            decode_access_token(tampered)
        assert exc_info.value.status_code == 401
