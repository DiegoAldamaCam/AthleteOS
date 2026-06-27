"""Metrics date-range endpoint: GET /athletes/{id}/metrics.

Spec: obs #65 Domain A — Metrics Date-Range Query.
Design: obs #66 Backend Design — Metrics endpoint section.

Business rules (LOCKED):
  - to defaults to today (UTC); from defaults to to-90d.
  - from > to → 422 (validator).
  - Bad date format → 422 (FastAPI native via `date` type annotation).
  - Unknown athlete (zero rows in ANY range) → 404.
  - Athlete exists but no rows in requested range → 200 [].
  - Rows returned in ascending metric_date order.
  - No date interpolation/fill — the API returns sparse series as-is.
  - Parameterized SQL only (no f-string injection).
"""

from __future__ import annotations

from datetime import date, timedelta, timezone
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status

from api.db import get_db
from api.models import MetricRow

router = APIRouter()

# ---------------------------------------------------------------------------
# SQL constants (extracted per REFACTOR task 2.9)
# ---------------------------------------------------------------------------

_SQL_ATHLETE_EXISTS = """
    SELECT 1 FROM athlete_metrics
    WHERE athlete_id = %s
    LIMIT 1
"""

_SQL_METRICS_RANGE = """
    SELECT
        athlete_id,
        metric_date,
        acute_load,
        chronic_load_28d,
        chronic_load_42d,
        acute_chronic_ratio,
        deload_flag
    FROM athlete_metrics
    WHERE athlete_id = %s
      AND metric_date BETWEEN %s AND %s
    ORDER BY metric_date ASC
"""


def _today_utc() -> date:
    """Return the current date in UTC."""
    return date.today()


@router.get(
    "/athletes/{athlete_id}/metrics",
    response_model=list[MetricRow],
    summary="Get athlete training-load metrics for a date range",
)
def get_athlete_metrics(
    athlete_id: str,
    from_date: Annotated[
        Optional[date],
        Query(alias="from", description="Start date (ISO-8601). Defaults to to-90d."),
    ] = None,
    to_date: Annotated[
        Optional[date],
        Query(alias="to", description="End date (ISO-8601). Defaults to today UTC."),
    ] = None,
    db=Depends(get_db),
) -> list[MetricRow]:
    """Return per-day training-load metrics for ``athlete_id`` within the date range.

    - If ``to`` is omitted, defaults to today (UTC).
    - If ``from`` is omitted, defaults to ``to - 90 days``.
    - ``from > to`` raises HTTP 422.
    - Unknown athlete raises HTTP 404.
    - Athlete with no rows in range returns HTTP 200 with ``[]``.
    """
    # Resolve defaults
    resolved_to: date = to_date if to_date is not None else _today_utc()
    resolved_from: date = from_date if from_date is not None else resolved_to - timedelta(days=90)

    # Validate date ordering
    if resolved_from > resolved_to:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=[
                {
                    "type": "value_error",
                    "loc": ["query", "from"],
                    "msg": "'from' must not be after 'to'",
                    "input": str(resolved_from),
                }
            ],
        )

    with db.cursor() as cur:
        # Check athlete existence (any row in the table for this id)
        cur.execute(_SQL_ATHLETE_EXISTS, (athlete_id,))
        if cur.fetchone() is None:
            raise HTTPException(status_code=http_status.HTTP_404_NOT_FOUND, detail="Athlete not found")

        # Fetch rows in the requested range
        cur.execute(_SQL_METRICS_RANGE, (athlete_id, resolved_from, resolved_to))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    return [MetricRow(**row) for row in rows]
