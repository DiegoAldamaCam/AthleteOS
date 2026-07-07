-- athletes table DDL for AthleteOS athlete directory (demo/multi-sport).
--
-- Holds per-athlete metadata (display name + sport/discipline) so the dashboard
-- can label, search and filter athletes by sport. The athlete_metrics table
-- carries only training-load rows keyed by athlete_id and has no notion of
-- sport; this table adds that dimension without altering athlete_metrics.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS + IF NOT EXISTS index means re-applying
-- this script to an instance that already has the table succeeds without error.
-- Applied at first-init via docker-entrypoint-initdb.d, or manually via:
--   psql -U athleteos -d athleteos -f storage/postgres/athletes_ddl.sql
--
-- Columns:
--   athlete_id  TEXT PRIMARY KEY  — matches athlete_metrics.athlete_id
--   name        TEXT NOT NULL     — display name
--   sport       TEXT NOT NULL     — discipline (e.g. running, powerlifting)
--   created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()

CREATE TABLE IF NOT EXISTS athletes (
    athlete_id  TEXT        NOT NULL PRIMARY KEY,
    name        TEXT        NOT NULL,
    sport       TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Speeds up "list athletes by sport" and sport-facet queries.
CREATE INDEX IF NOT EXISTS idx_athletes_sport ON athletes (sport);
