-- AthleteOS planning store DDL (PR-PL2a — ADR-20, ADR-21).
--
-- Creates the planning_blocks table with:
--   - PRIMARY KEY (athlete_id, block_id, ingest_time) — versioning PK per ADR-20.
--     Multiple revisions of the same (athlete_id, block_id) coexist in the table;
--     ingest_time is the version axis. Downstream adherence queries resolve
--     "plan version in effect on date X" via ORDER BY ingest_time DESC LIMIT 1.
--   - start_date / end_date as DATE (converted from epoch-ms by the PG sink).
--   - weekly_volume_targets as TEXT (compact JSON string, not JSONB) — matches
--     the Avro STRING wire type and simplifies schema evolution.
--
-- All statements use IF NOT EXISTS so this script is idempotent and safe to
-- re-run against an existing database (bootstrap, CI, local dev) — PL2-10.
--
-- This script is ADDITIVE: it does NOT modify athlete_metrics or any other
-- table. Rollback: DROP TABLE planning_blocks (athlete_metrics is untouched).

CREATE TABLE IF NOT EXISTS planning_blocks (
    athlete_id                  TEXT            NOT NULL,
    block_id                    TEXT            NOT NULL,
    ingest_time                 TIMESTAMPTZ     NOT NULL,
    goal                        TEXT            NOT NULL,
    start_date                  DATE            NOT NULL,
    end_date                    DATE            NOT NULL,
    planned_sessions_per_week   INT             NOT NULL,
    weekly_volume_targets       TEXT            NOT NULL,
    PRIMARY KEY (athlete_id, block_id, ingest_time)
);

CREATE INDEX IF NOT EXISTS idx_planning_blocks_athlete
    ON planning_blocks (athlete_id);

CREATE INDEX IF NOT EXISTS idx_planning_blocks_athlete_block
    ON planning_blocks (athlete_id, block_id);
