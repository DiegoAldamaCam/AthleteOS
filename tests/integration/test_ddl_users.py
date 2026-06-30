"""Integration tests for users table DDL idempotency and schema contract.

Spec: obs #385 (sdd/athleteos-jwt-auth/spec)
  sc-1: users_ddl.sql applied twice → no error (IF NOT EXISTS semantics)
  sc-2: users table has exactly the correct columns, types, and constraints

Design: obs #386 (sdd/athleteos-jwt-auth/design)
  File: storage/postgres/users_ddl.sql

Docker-gated: skipped automatically when Docker daemon is not reachable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.conftest import requires_docker

requires_docker()

try:
    import psycopg2
except ImportError:
    pytest.skip("psycopg2 not installed; users DDL tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DDL path
# ---------------------------------------------------------------------------

_USERS_DDL_PATH = (
    Path(__file__).resolve().parents[2] / "storage" / "postgres" / "users_ddl.sql"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_container(docker_ok):
    """A fresh PostgresContainer scoped to this test module."""
    if not docker_ok:
        pytest.skip("Docker not available")
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:16")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def pg_dsn(pg_container) -> str:
    return pg_container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
def pg_conn(pg_dsn):
    conn = psycopg2.connect(pg_dsn)
    conn.autocommit = True
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply_users_ddl(conn) -> None:
    """Execute the users DDL script against conn."""
    ddl_text = _USERS_DDL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(ddl_text)


# ---------------------------------------------------------------------------
# sc-1: DDL idempotency
# ---------------------------------------------------------------------------


class TestUsersDdlIdempotency:
    """sc-1: Applying users_ddl.sql twice must not raise an error."""

    def test_ddl_runs_twice_without_error(self, pg_conn):
        """sc-1: First and second application both succeed (IF NOT EXISTS)."""
        _apply_users_ddl(pg_conn)
        _apply_users_ddl(pg_conn)  # second run — must not raise


# ---------------------------------------------------------------------------
# sc-2: Schema contract
# ---------------------------------------------------------------------------


class TestUsersDdlSchema:
    """sc-2: users table columns, types, and NOT NULL constraints match spec."""

    def test_users_table_exists_after_ddl(self, pg_conn):
        """Users table must exist after DDL is applied."""
        # DDL may already have been applied by the idempotency test (module scope);
        # apply again to be safe — idempotent.
        _apply_users_ddl(pg_conn)
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.users')"
            )
            row = cur.fetchone()
        assert row is not None and row[0] is not None, (
            "users table must exist after applying users_ddl.sql"
        )

    def test_users_columns_and_not_null_constraints(self, pg_conn):
        """sc-2: all required columns exist with correct not-null constraints."""
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'users'
                ORDER BY ordinal_position
                """
            )
            rows = cur.fetchall()

        columns = {row[0]: {"is_nullable": row[1], "default": row[2]} for row in rows}

        # id: NOT NULL (primary key)
        assert "id" in columns, "Column 'id' must exist"
        assert columns["id"]["is_nullable"] == "NO", "id must be NOT NULL"

        # username: NOT NULL
        assert "username" in columns, "Column 'username' must exist"
        assert columns["username"]["is_nullable"] == "NO", "username must be NOT NULL"

        # password_hash: NOT NULL
        assert "password_hash" in columns, "Column 'password_hash' must exist"
        assert columns["password_hash"]["is_nullable"] == "NO", "password_hash must be NOT NULL"

        # created_at: NOT NULL with default
        assert "created_at" in columns, "Column 'created_at' must exist"
        assert columns["created_at"]["is_nullable"] == "NO", "created_at must be NOT NULL"
        assert columns["created_at"]["default"] is not None, (
            "created_at must have a default (NOW())"
        )

    def test_username_unique_constraint(self, pg_conn):
        """sc-2: username column has a UNIQUE constraint."""
        # Insert a row, then attempt to insert the same username again → the
        # second insert must raise a UNIQUE violation. Both inserts run on the
        # fixture connection; the duplicate insert is wrapped so we can roll the
        # transaction back afterward and leave pg_conn usable. (A second
        # psycopg2.connect() is avoided on purpose: psycopg2 redacts the password
        # from conn.dsn, so reconnecting from it fails authentication rather than
        # exercising the constraint.)
        import psycopg2.errors

        from api.jwt_utils import hash_password

        _apply_users_ddl(pg_conn)

        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s) "
                "ON CONFLICT DO NOTHING",
                ("sc2_test_user", hash_password("pwd")),
            )
        pg_conn.commit()

        with pytest.raises(psycopg2.errors.UniqueViolation) as exc_info:
            with pg_conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
                    ("sc2_test_user", hash_password("other")),
                )
        pg_conn.rollback()  # clear the aborted transaction so the connection stays usable

        assert "unique" in str(exc_info.value).lower() or "duplicate" in str(exc_info.value).lower(), (
            f"Expected UNIQUE violation error, got: {exc_info.value}"
        )
