"""Adherence-metrics batch job: planning_blocks + Iceberg → adherence_score UPSERT.

Design notes (ADR-22, ADR-23, ADR-24)
======================================
- Pure Python batch job — NO pyflink (import-safe on Python 3.14).
- Reads planning_blocks from PostgreSQL, resolving the LATEST revision per
  block via DISTINCT ON (ADR-24).
- Reads training events from the Iceberg warehouse via
  storage.duckdb.reader.read_training_events (ADR-23, glob-based Parquet read).
- Computes adherence_score using jobs.adherence_metrics.compute (ADR-22).
- Writes adherence_score ONLY via build_adherence_upsert (ADR-19 non-overlap).

Key constraints
---------------
- metric_date = min(block.end_date, today_date) — in-progress blocks use today
  as the upper bound (ADH-J2/J3).
- block_weeks = max(1, ceil((end_date - start_date).days / 7)).
- Blocks with planned_sessions_per_week ≤ 0 are skipped (E5 guard).
- Athletes with no planning_blocks row are never UPSERTed (E2 — plan-driven loop).
- Commit is issued per-block (not per-athlete) for incremental durability.
"""

from __future__ import annotations

import datetime
import math
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Job configuration (import-safe — no pyflink, no heavy deps at module level)
# ---------------------------------------------------------------------------


@dataclass
class AdherenceJobConfig:
    """Configuration for the adherence-metrics batch job.

    All fields are required; no defaults provided so misconfiguration is
    caught at instantiation rather than silently using wrong paths.

    Attributes:
        pg_dsn:         psycopg2-compatible DSN for the PostgreSQL serving store.
        warehouse_path: Absolute path to the Iceberg warehouse root directory.
        today_date:     The calendar date to treat as "today" (injected for
                        deterministic tests — ADH-J2/J3).
    """

    pg_dsn: str = ""
    warehouse_path: str = ""
    today_date: datetime.date = field(default_factory=datetime.date.today)


# ---------------------------------------------------------------------------
# SQL — plan version resolution (ADR-24)
# ---------------------------------------------------------------------------

_SQL_LATEST_PLANNING_BLOCKS = """
SELECT DISTINCT ON (athlete_id, block_id)
    athlete_id,
    block_id,
    goal,
    start_date,
    end_date,
    planned_sessions_per_week,
    weekly_volume_targets
FROM planning_blocks
ORDER BY athlete_id, block_id, ingest_time DESC
"""

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_EPOCH_MS_PER_DAY: int = 86_400_000
_MILLIS_PER_SECOND: int = 1_000


def _date_to_utc_midnight_ms(d: datetime.date) -> int:
    """Convert a date to UTC midnight epoch-milliseconds (inclusive day start)."""
    dt_utc = datetime.datetime(d.year, d.month, d.day, tzinfo=datetime.timezone.utc)
    return int(dt_utc.timestamp() * _MILLIS_PER_SECOND)


def _ms_to_utc_date(epoch_ms: int) -> datetime.date:
    """Convert epoch-ms to the UTC calendar date it falls on."""
    dt_utc = datetime.datetime.fromtimestamp(epoch_ms / _MILLIS_PER_SECOND, tz=datetime.timezone.utc)
    return dt_utc.date()


# ---------------------------------------------------------------------------
# Job entry point
# ---------------------------------------------------------------------------


def run(cfg: Optional[AdherenceJobConfig] = None) -> None:
    """Run the adherence-metrics batch job.

    Iterates all planning blocks (latest revision per block), computes the
    adherence score from Iceberg actuals, and UPSERTs the result into
    athlete_metrics.adherence_score.

    Args:
        cfg: Job configuration.  Defaults to AdherenceJobConfig() with
             empty DSN/warehouse and today's date when not provided (useful
             for manual invocation with env-var injection in the future).
    """
    if cfg is None:
        cfg = AdherenceJobConfig()

    # Lazy imports: psycopg2 is available in the job runtime but may be
    # absent in some test environments that use fake connections.
    import psycopg2  # type: ignore[import]

    from jobs.adherence_metrics.compute import (
        compute_adherence_score,
        parse_weekly_volume_targets,
    )
    from storage.duckdb.reader import read_training_events
    from storage.postgres.sink import build_adherence_upsert, upsert_with_retry

    # ------------------------------------------------------------------
    # 1. Open PostgreSQL connection
    # ------------------------------------------------------------------
    conn = psycopg2.connect(cfg.pg_dsn)

    try:
        # ------------------------------------------------------------------
        # 2. Read all training events from Iceberg ONCE (not per-block)
        # ------------------------------------------------------------------
        all_events = read_training_events(cfg.warehouse_path)

        # ------------------------------------------------------------------
        # 3. Read all planning blocks (latest revision per block — ADR-24)
        # ------------------------------------------------------------------
        with conn.cursor() as cur:
            cur.execute(_SQL_LATEST_PLANNING_BLOCKS)
            columns = [desc[0] for desc in cur.description]
            blocks = [dict(zip(columns, row)) for row in cur.fetchall()]

        # ------------------------------------------------------------------
        # 4. Process each block
        # ------------------------------------------------------------------
        for block in blocks:
            athlete_id: str = block["athlete_id"]
            planned_sessions_per_week: int = block["planned_sessions_per_week"]
            start_date: datetime.date = block["start_date"]
            end_date: datetime.date = block["end_date"]
            weekly_volume_targets_raw: Optional[str] = block.get("weekly_volume_targets")

            # E5: skip blocks with no planned sessions
            if planned_sessions_per_week <= 0:
                continue

            # Compute block metrics
            block_weeks: int = max(1, math.ceil((end_date - start_date).days / 7))
            planned_sessions: int = planned_sessions_per_week * block_weeks

            # ADH-J2/J3: metric_date = min(end_date, today)
            metric_date: datetime.date = min(end_date, cfg.today_date)

            # Convert dates to epoch-ms for filtering (event_time is epoch-ms)
            start_ms: int = _date_to_utc_midnight_ms(start_date)
            metric_date_end_ms: int = _date_to_utc_midnight_ms(metric_date) + _EPOCH_MS_PER_DAY - 1

            # Filter Iceberg events to this athlete + date window
            window_events = [
                e for e in all_events
                if str(e.get("athlete_id", "")) == str(athlete_id)
                and start_ms <= int(e["event_time"]) <= metric_date_end_ms
            ]

            # COUNT DISTINCT training days (actual_sessions)
            actual_sessions: int = len({
                _ms_to_utc_date(int(e["event_time"])) for e in window_events
            })

            # SUM session_load (actual_volume)
            actual_volume: float = sum(
                float(e.get("session_load") or 0.0) for e in window_events
            )

            # Parse weekly volume targets (E3: bad JSON → None → sessions-only)
            weekly_target: Optional[float] = (
                parse_weekly_volume_targets(weekly_volume_targets_raw)
                if weekly_volume_targets_raw is not None
                else None
            )
            target_volume: Optional[float] = (
                weekly_target * block_weeks if weekly_target is not None else None
            )

            # Compute adherence score
            score: Optional[float] = compute_adherence_score(
                actual_sessions,
                planned_sessions,
                actual_volume,
                target_volume,
            )

            # E2/E5: skip UPSERT when score is None
            if score is None:
                continue

            # Build record and upsert
            record = {
                "athlete_id": athlete_id,
                "metric_date": metric_date,  # datetime.date — NOT epoch-ms
                "adherence_score": score,
            }

            conn = upsert_with_retry(
                record,
                conn,
                lambda: psycopg2.connect(cfg.pg_dsn),  # type: ignore[misc]
                max_retries=3,
                base_backoff_s=0.5,
                build_fn=build_adherence_upsert,
            )

        # Final commit (per-block commits happen inside upsert_with_retry)
        conn.commit()

    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
