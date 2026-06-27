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

-- metrics-v2: load-based scores and coaching flags (additive columns, idempotent).
-- DDL MUST run before deploying the new job binary (ADR-D1, WARNING-3).
-- IF NOT EXISTS guarantees idempotency (Scenario 18).
ALTER TABLE athlete_metrics
    ADD COLUMN IF NOT EXISTS fatigue_score   FLOAT NULL,
    ADD COLUMN IF NOT EXISTS readiness_score FLOAT NULL,
    ADD COLUMN IF NOT EXISTS coaching_flags  TEXT  NULL;

-- wellness-source: recovery_score column (additive, idempotent) — W3-9
ALTER TABLE athlete_metrics ADD COLUMN IF NOT EXISTS recovery_score FLOAT NULL;
-- ADR-19: enable recovery-only partial-row INSERT (omitted load cols -> NULL = not computed).
-- ALTER COLUMN ... DROP NOT NULL is idempotent: re-running on an already-nullable column succeeds.
ALTER TABLE athlete_metrics ALTER COLUMN acute_load DROP NOT NULL;
ALTER TABLE athlete_metrics ALTER COLUMN chronic_load_28d DROP NOT NULL;
ALTER TABLE athlete_metrics ALTER COLUMN chronic_load_42d DROP NOT NULL;
ALTER TABLE athlete_metrics ALTER COLUMN deload_flag DROP NOT NULL;
