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
from api.security import require_auth

router = APIRouter()

# ---------------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------------

_SQL_LIST_ATHLETES = """
    SELECT DISTINCT athlete_id
    FROM athlete_metrics
    ORDER BY athlete_id
"""

# Directory: join distinct metric athletes with the athletes metadata table.
# LEFT JOIN so athletes present only in athlete_metrics (e.g. the two pipeline
# athletes with no directory row) still appear, with null name/sport.
_SQL_ATHLETE_DIRECTORY = """
    SELECT m.athlete_id, a.name, a.sport
    FROM (SELECT DISTINCT athlete_id FROM athlete_metrics) AS m
    LEFT JOIN athletes AS a ON a.athlete_id = m.athlete_id
    ORDER BY a.sport NULLS LAST, m.athlete_id
"""

_SQL_LIST_SPORTS = """
    SELECT a.sport, COUNT(*) AS athlete_count
    FROM athletes AS a
    GROUP BY a.sport
    ORDER BY athlete_count DESC, a.sport
"""


@router.get("/athletes", summary="List all distinct athlete IDs", dependencies=[Depends(require_auth)])
def list_athletes(db=Depends(get_db)) -> dict:
    """Return sorted distinct athlete IDs known to the system.

    Derives athlete identity from ``athlete_metrics`` rows.  Returns
    ``{"athletes": []}`` (HTTP 200) when the table is empty (sc-1.3).  DB
    failures propagate as HTTP 500 via the global exception handler (sc-1.5).

    NOTE: response shape is LOCKED to {"athletes": list[str]} (sc-1.4). Richer
    per-athlete metadata (name/sport) is served by GET /athletes/directory.
    """
    with db.cursor() as cur:
        cur.execute(_SQL_LIST_ATHLETES)
        rows = cur.fetchall()
    return {"athletes": [row[0] for row in rows]}


@router.get(
    "/athletes/directory",
    summary="List athletes with name and sport metadata",
    dependencies=[Depends(require_auth)],
)
def list_athlete_directory(db=Depends(get_db)) -> dict:
    """Return each athlete with its display name and sport.

    Additive endpoint (does not alter /athletes). Athletes present only in
    athlete_metrics but not in the athletes table get null name/sport.
    """
    with db.cursor() as cur:
        cur.execute(_SQL_ATHLETE_DIRECTORY)
        rows = cur.fetchall()
    return {
        "athletes": [
            {"athlete_id": r[0], "name": r[1], "sport": r[2]} for r in rows
        ]
    }


@router.get(
    "/sports",
    summary="List sports with athlete counts",
    dependencies=[Depends(require_auth)],
)
def list_sports(db=Depends(get_db)) -> dict:
    """Return the distinct sports and how many athletes each has."""
    with db.cursor() as cur:
        cur.execute(_SQL_LIST_SPORTS)
        rows = cur.fetchall()
    return {"sports": [{"sport": r[0], "athlete_count": r[1]} for r in rows]}
