"""CLI tool to seed the first login user into the AthleteOS users table.

Uses the same bcrypt hashing scheme as api/jwt_utils.py (passlib CryptContext,
bcrypt, 12 rounds). Importing api.jwt_utils directly would trigger the FastAPI
Settings() constructor which requires DATABASE_URL, API_KEY, and JWT_SECRET env
vars — unnecessary overhead for a standalone CLI. This script replicates the
single hash_password() call inline using the same library and scheme.

Usage
-----
Set DATABASE_URL (preferred):

    DATABASE_URL=postgresql://athleteos:secret@localhost:5432/athleteos \\
        python -m tools.seed_user --username admin --password changeme

Or rely on the POSTGRES_PASSWORD fallback (connects to localhost:5432/athleteos):

    POSTGRES_PASSWORD=secret python -m tools.seed_user --username admin --password changeme

Idempotency
-----------
The INSERT uses ON CONFLICT (username) DO NOTHING. Re-running with the same
username is a no-op (no error, no password update). To change a password,
connect directly with psql and UPDATE the row.

Dependencies
------------
psycopg2-binary is a declared runtime dependency in pyproject.toml.
"""

from __future__ import annotations

import argparse
import os
import sys

import psycopg2
from passlib.context import CryptContext

# Mirror the exact configuration in api/jwt_utils.py so hashes are compatible.
_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _hash_password(plain: str) -> str:
    """Hash a plaintext password using bcrypt (mirrors api/jwt_utils.hash_password)."""
    return _pwd_context.hash(plain)


def _build_dsn() -> str:
    """Resolve the database connection string.

    Priority:
    1. DATABASE_URL env var (full DSN — preferred, matches FastAPI/compose convention)
    2. Construct from POSTGRES_PASSWORD targeting localhost:5432 db/user athleteos
    """
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        return dsn

    pg_password = os.environ.get("POSTGRES_PASSWORD")
    if pg_password:
        return f"postgresql://athleteos:{pg_password}@localhost:5432/athleteos"

    sys.exit(
        "Error: set DATABASE_URL or POSTGRES_PASSWORD env var before running this script.\n"
        "Example:\n"
        "  DATABASE_URL=postgresql://athleteos:secret@localhost:5432/athleteos \\\n"
        "      python -m tools.seed_user --username admin --password changeme"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Seed a first login user into the AthleteOS users table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--username", required=True, help="Username for the new user.")
    parser.add_argument("--password", required=True, help="Plaintext password (hashed before storage).")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    dsn = _build_dsn()

    password_hash = _hash_password(args.password)

    try:
        conn = psycopg2.connect(dsn, connect_timeout=10)
    except psycopg2.OperationalError as exc:
        sys.exit(f"Error: could not connect to database.\n{exc}")

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash)
                    VALUES (%s, %s)
                    ON CONFLICT (username) DO NOTHING
                    """,
                    (args.username, password_hash),
                )
                if cur.rowcount == 0:
                    print(
                        f"User '{args.username}' already exists — no changes made.\n"
                        "To update the password, connect with psql and UPDATE the row directly."
                    )
                else:
                    print(f"User '{args.username}' created successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
