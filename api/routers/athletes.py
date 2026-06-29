"""Athletes list endpoint: GET /athletes.

Spec: obs #255, athletes-list-api specification (sc-1.1..sc-1.5).
Design: obs #256.
Auth: obs #314 (sdd/athleteos-api-auth/spec), sc-1, sc-4.

Business rules (LOCKED):
  - Returns sorted, distinct athlete IDs from athlete_metrics.
  - Empty table → HTTP 200 + {"athletes": []}.
  - Requires X-API-Key header (sc-1, sc-4); missing key → 401; wrong key → 401.
  - DB failure → HTTP 500 via global exception handler (mirrors metrics.py — no per-route try/except).
  - Full path /athletes in @router.get; bare app.include_router(athletes.router) — no prefix.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from api.db import get_db
from api.security import require_api_key

router = APIRouter()

# ---------------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------------

_SQL_LIST_ATHLETES = """
    SELECT DISTINCT athlete_id
    FROM athlete_metrics
    ORDER BY athlete_id
"""


@router.get("/athletes", summary="List all distinct athlete IDs", dependencies=[Depends(require_api_key)])
def list_athletes(db=Depends(get_db)) -> dict:
    """Return sorted distinct athlete IDs known to the system.

    Derives athlete identity from ``athlete_metrics`` rows — no separate
    athletes table exists.  Returns ``{"athletes": []}`` (HTTP 200) when
    the table is empty (sc-1.3).  DB failures propagate as HTTP 500 via
    the global exception handler (sc-1.5).
    """
    with db.cursor() as cur:
        cur.execute(_SQL_LIST_ATHLETES)
        rows = cur.fetchall()
    return {"athletes": [row[0] for row in rows]}
