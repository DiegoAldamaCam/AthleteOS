-- AthleteOS serving store DDL (task 6.1, PR5 Phase 6).
--
-- Creates the athlete_metrics table with:
--   - metric_date as DATE (converted from epoch-ms by the PG sink).
--   - PRIMARY KEY (athlete_id, metric_date) — one row per athlete per calendar day.
--   - All MUST metric columns per the serving-store spec.
--   - An index on athlete_id to speed up per-athlete queries.
--
-- All statements use IF NOT EXISTS so this script is idempotent and safe to
-- re-run against an existing database (bootstrap, CI, local dev).

CREATE TABLE IF NOT EXISTS athlete_metrics (
    athlete_id          TEXT        NOT NULL,
    metric_date         DATE        NOT NULL,
    acute_load          FLOAT       NOT NULL,
    chronic_load_28d    FLOAT       NOT NULL,
    chronic_load_42d    FLOAT       NOT NULL,
    acute_chronic_ratio FLOAT       NULL,
    deload_flag         INT         NOT NULL,
    PRIMARY KEY (athlete_id, metric_date)
);

CREATE INDEX IF NOT EXISTS idx_athlete_metrics_athlete
    ON athlete_metrics (athlete_id);
