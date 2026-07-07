"""Cross-athlete analytics endpoints (per-sport aggregation).

Additive to the per-athlete metrics API. Powers the comparative dashboard:
  - GET /analytics/by-sport                     — mean metrics per sport
  - GET /analytics/risk-distribution            — athlete counts per ACR zone/sport
  - GET /analytics/sport/{sport}/daily-average  — the sport's mean daily curve

All queries are parameterized. Sport metadata lives in the ``athletes`` table;
training rows live in ``athlete_metrics``; they are joined on athlete_id.

Business rules:
  - "Latest" per athlete = that athlete's most recent metric_date row.
  - ACR zones: safe (<1.3), caution (1.3–1.5), danger (>1.5); NULL ACR is
    excluded from zone counts (uncomputable, e.g. day-1 rows).
  - Requires X-API-Key (all routes depend on require_auth).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi import status as http_status

from api.db import get_db
from api.security import require_auth

router = APIRouter()

# ACR zone thresholds (mirror the front TrendChart zones).
ACR_SAFE_MAX = 1.3
ACR_CAUTION_MAX = 1.5

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# Mean metrics per sport over each athlete's LATEST row (one row per athlete),
# so a sport's averages are not skewed by athletes with more history.
_SQL_BY_SPORT = """
    WITH latest AS (
        SELECT DISTINCT ON (m.athlete_id)
            m.athlete_id,
            m.acute_load,
            m.chronic_load_28d,
            m.acute_chronic_ratio,
            m.fatigue_score,
            m.readiness_score
        FROM athlete_metrics m
        ORDER BY m.athlete_id, m.metric_date DESC
    )
    SELECT
        a.sport,
        COUNT(*)                         AS athlete_count,
        AVG(l.acute_load)                AS avg_acute_load,
        AVG(l.chronic_load_28d)          AS avg_chronic_load,
        AVG(l.acute_chronic_ratio)       AS avg_acr,
        AVG(l.fatigue_score)             AS avg_fatigue,
        AVG(l.readiness_score)           AS avg_readiness
    FROM latest l
    JOIN athletes a ON a.athlete_id = l.athlete_id
    GROUP BY a.sport
    ORDER BY a.sport
"""

# Per-sport count of athletes in each ACR risk zone, using each athlete's latest row.
_SQL_RISK_DISTRIBUTION = """
    WITH latest AS (
        SELECT DISTINCT ON (m.athlete_id)
            m.athlete_id,
            m.acute_chronic_ratio
        FROM athlete_metrics m
        ORDER BY m.athlete_id, m.metric_date DESC
    )
    SELECT
        a.sport,
        COUNT(*) FILTER (WHERE l.acute_chronic_ratio < %s)                          AS safe,
        COUNT(*) FILTER (WHERE l.acute_chronic_ratio >= %s AND l.acute_chronic_ratio <= %s) AS caution,
        COUNT(*) FILTER (WHERE l.acute_chronic_ratio > %s)                          AS danger,
        COUNT(*) FILTER (WHERE l.acute_chronic_ratio IS NULL)                       AS unknown
    FROM latest l
    JOIN athletes a ON a.athlete_id = l.athlete_id
    GROUP BY a.sport
    ORDER BY a.sport
"""

_SQL_SPORT_EXISTS = "SELECT 1 FROM athletes WHERE sport = %s LIMIT 1"

# Mean daily curve for a sport across all its athletes, within a date window.
_SQL_SPORT_DAILY_AVERAGE = """
    SELECT
        m.metric_date,
        AVG(m.acute_load)          AS avg_acute_load,
        AVG(m.chronic_load_28d)    AS avg_chronic_load,
        AVG(m.acute_chronic_ratio) AS avg_acr,
        COUNT(DISTINCT m.athlete_id) AS athlete_count
    FROM athlete_metrics m
    JOIN athletes a ON a.athlete_id = m.athlete_id
    WHERE a.sport = %s
      AND m.metric_date BETWEEN %s AND %s
    GROUP BY m.metric_date
    ORDER BY m.metric_date ASC
"""


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def _f(v) -> float | None:
    """Round an aggregate to 1 decimal, preserving None."""
    return None if v is None else round(float(v), 1)


@router.get(
    "/analytics/by-sport",
    summary="Mean training metrics per sport (latest row per athlete)",
    dependencies=[Depends(require_auth)],
)
def analytics_by_sport(db=Depends(get_db)) -> dict:
    with db.cursor() as cur:
        cur.execute(_SQL_BY_SPORT)
        rows = cur.fetchall()
    sports = [
        {
            "sport": r[0],
            "athlete_count": int(r[1]),
            "avg_acute_load": _f(r[2]),
            "avg_chronic_load": _f(r[3]),
            "avg_acr": None if r[4] is None else round(float(r[4]), 3),
            "avg_fatigue": _f(r[5]),
            "avg_readiness": _f(r[6]),
        }
        for r in rows
    ]
    return {"sports": sports}


@router.get(
    "/analytics/risk-distribution",
    summary="Athlete counts per ACR risk zone, grouped by sport",
    dependencies=[Depends(require_auth)],
)
def analytics_risk_distribution(db=Depends(get_db)) -> dict:
    with db.cursor() as cur:
        cur.execute(
            _SQL_RISK_DISTRIBUTION,
            (ACR_SAFE_MAX, ACR_SAFE_MAX, ACR_CAUTION_MAX, ACR_CAUTION_MAX),
        )
        rows = cur.fetchall()
    sports = [
        {
            "sport": r[0],
            "safe": int(r[1]),
            "caution": int(r[2]),
            "danger": int(r[3]),
            "unknown": int(r[4]),
        }
        for r in rows
    ]
    return {"sports": sports}


@router.get(
    "/analytics/sport/{sport}/daily-average",
    summary="Mean daily load curve for a sport",
    dependencies=[Depends(require_auth)],
)
def analytics_sport_daily_average(sport: str, db=Depends(get_db)) -> dict:
    resolved_to = _today_utc()
    resolved_from = resolved_to - timedelta(days=90)

    with db.cursor() as cur:
        cur.execute(_SQL_SPORT_EXISTS, (sport,))
        if cur.fetchone() is None:
            raise HTTPException(
                status_code=http_status.HTTP_404_NOT_FOUND,
                detail="Sport not found",
            )
        cur.execute(_SQL_SPORT_DAILY_AVERAGE, (sport, resolved_from, resolved_to))
        rows = cur.fetchall()

    points = [
        {
            "metric_date": r[0].isoformat(),
            "avg_acute_load": _f(r[1]),
            "avg_chronic_load": _f(r[2]),
            "avg_acr": None if r[3] is None else round(float(r[3]), 3),
            "athlete_count": int(r[4]),
        }
        for r in rows
    ]
    return {"sport": sport, "points": points}
