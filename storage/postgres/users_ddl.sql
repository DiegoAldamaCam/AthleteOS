-- users table DDL for AthleteOS JWT authentication.
--
-- Spec: obs #385 (sdd/athleteos-jwt-auth/spec), sc-1, sc-2.
-- Design: obs #386 (sdd/athleteos-jwt-auth/design).
--
-- Idempotent: CREATE TABLE IF NOT EXISTS semantics mean re-applying this script
-- to an instance that already has the users table succeeds without error (sc-1).
-- This script is applied manually via: psql -f storage/postgres/users_ddl.sql
-- or inline in integration tests using PostgresContainer.
--
-- Columns (sc-2):
--   id            SERIAL PRIMARY KEY
--   username      TEXT NOT NULL UNIQUE  — enforces uniqueness for login lookups
--   password_hash TEXT NOT NULL         — bcrypt hash; plaintext is NEVER stored
--   created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()

CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      TEXT        NOT NULL UNIQUE,
    password_hash TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
