"""psycopg2 database connection dependency for FastAPI.

Provides a per-request connection via FastAPI's dependency injection.
Connections are opened and closed per request (no persistent pool) — acceptable
for the current single-instance usage pattern. Upgrade to a connection pool
(e.g., psycopg2.pool.ThreadedConnectionPool or asyncpg) if throughput requires it.
"""

from __future__ import annotations

from typing import Generator

import psycopg2
import psycopg2.extras

from api.config import settings


def get_db() -> Generator:
    """FastAPI dependency: yield a psycopg2 connection, close on exit.

    Usage in a route:
        @router.get(...)
        def my_route(db=Depends(get_db)):
            with db.cursor() as cur:
                cur.execute(...)
    """
    conn = psycopg2.connect(settings.database_url, connect_timeout=settings.db_connect_timeout_seconds)
    try:
        yield conn
    finally:
        conn.close()
