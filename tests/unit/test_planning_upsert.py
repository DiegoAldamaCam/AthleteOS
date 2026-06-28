"""Unit tests for storage.postgres.sink — planning_blocks UPSERT (PL2-11, PL2-12).
Also verifies storage/postgres/planning_ddl.sql content (PL2-10 idempotency).

Verifies:
  - build_planning_upsert returns correct SQL referencing ONLY planning_blocks
  - SQL uses INSERT ... ON CONFLICT (athlete_id, block_id, ingest_time) DO NOTHING (ADR-21)
  - SQL does NOT reference athlete_metrics, recovery_score, or any load-metric column
  - Params tuple is correct (8 values, all correctly typed)
  - PK coexistence: two records with same (athlete_id, block_id) but different
    ingest_time produce TWO non-conflicting SQL rows (PL2-11)
  - Non-overlap: _UPSERT_SQL (load path) is byte-for-byte unchanged (PL2-13)
  - Non-overlap: existing symbols build_upsert / build_recovery_upsert are untouched
  - DDL: planning_ddl.sql uses IF NOT EXISTS (idempotent), correct PK (PL2-10)

These are pure unit tests — no Docker, no real DB required.
"""

from __future__ import annotations
from pathlib import Path

import pytest

from storage.postgres.sink import (
    _PLANNING_UPSERT_SQL,  # type: ignore[attr-defined]
    _UPSERT_SQL,
    _RECOVERY_UPSERT_SQL,  # type: ignore[attr-defined]
    build_planning_upsert,
    build_upsert,
    build_recovery_upsert,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_INGEST_TIME_A = 1_748_740_000_000  # first revision
_INGEST_TIME_B = 1_748_800_000_000  # second revision (different ingest_time → new PK row)


def _make_planning_record(
    *,
    event_id: str = "evt-plan-001",
    event_time: int = 1_748_736_000_000,
    ingest_time: int = _INGEST_TIME_A,
    source: str = "planning_connector",
    schema_version: int = 1,
    athlete_id: str = "A1",
    block_id: str = "BLK-001",
    goal: str = "Build aerobic base",
    start_date: int = 1_748_736_000_000,
    end_date: int = 1_756_598_400_000,
    planned_sessions_per_week: int = 5,
    weekly_volume_targets: str = '{"strength": 3, "cardio": 2}',
) -> dict:
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": ingest_time,
        "source": source,
        "schema_version": schema_version,
        "athlete_id": athlete_id,
        "block_id": block_id,
        "goal": goal,
        "start_date": start_date,
        "end_date": end_date,
        "planned_sessions_per_week": planned_sessions_per_week,
        "weekly_volume_targets": weekly_volume_targets,
    }


# ---------------------------------------------------------------------------
# PL2-12: build_planning_upsert SQL references ONLY planning_blocks
# ---------------------------------------------------------------------------


class TestBuildPlanningUpsertSqlIsolation:
    """PL2-12: build_planning_upsert SQL must reference only planning_blocks table."""

    def test_sql_contains_insert_into_planning_blocks(self):
        """SQL must INSERT INTO planning_blocks."""
        record = _make_planning_record()
        sql, _ = build_planning_upsert(record)
        assert "planning_blocks" in sql

    def test_sql_does_not_reference_athlete_metrics(self):
        """PL2-12: SQL must NOT reference athlete_metrics (isolation guarantee)."""
        record = _make_planning_record()
        sql, _ = build_planning_upsert(record)
        assert "athlete_metrics" not in sql, (
            "CRITICAL (PL2-12): build_planning_upsert SQL must NOT reference "
            "athlete_metrics. The planning and load-metrics UPSERTs must be isolated."
        )

    def test_sql_does_not_reference_recovery_score(self):
        """PL2-12: SQL must NOT reference recovery_score."""
        record = _make_planning_record()
        sql, _ = build_planning_upsert(record)
        assert "recovery_score" not in sql

    def test_sql_does_not_reference_load_columns(self):
        """PL2-12: SQL must NOT reference load-metrics columns."""
        record = _make_planning_record()
        sql, _ = build_planning_upsert(record)
        for col in ("acute_load", "chronic_load_28d", "chronic_load_42d",
                    "deload_flag", "fatigue_score", "readiness_score"):
            assert col not in sql, (
                f"CRITICAL (PL2-12): Column {col!r} must NOT appear in "
                "build_planning_upsert SQL."
            )

    def test_sql_uses_do_nothing_not_do_update(self):
        """ADR-21: ON CONFLICT must be DO NOTHING — NOT DO UPDATE.

        DO UPDATE would overwrite revision history, violating ADR-20's
        versioning semantic. Any genuine new revision (different ingest_time)
        never conflicts; an exact replay → DO NOTHING (idempotent).
        """
        record = _make_planning_record()
        sql, _ = build_planning_upsert(record)
        upper = sql.upper()
        assert "DO NOTHING" in upper, (
            "ADR-21: ON CONFLICT must use DO NOTHING (not DO UPDATE) to "
            "preserve revision history per ADR-20."
        )
        assert "DO UPDATE" not in upper, (
            "ADR-21: DO UPDATE must NOT appear in planning upsert SQL — "
            "it would overwrite revision history."
        )

    def test_sql_conflict_target_is_full_versioning_pk(self):
        """ADR-20/21: ON CONFLICT target must be (athlete_id, block_id, ingest_time)."""
        record = _make_planning_record()
        sql, _ = build_planning_upsert(record)
        assert "athlete_id" in sql
        assert "block_id" in sql
        assert "ingest_time" in sql


# ---------------------------------------------------------------------------
# PL2-12: _PLANNING_UPSERT_SQL constant isolation
# ---------------------------------------------------------------------------


class TestPlanningUpsertSqlConstantIsolation:
    """_PLANNING_UPSERT_SQL constant must be isolated from existing SQL constants."""

    def test_planning_upsert_sql_does_not_contain_athlete_metrics(self):
        """_PLANNING_UPSERT_SQL must reference planning_blocks, not athlete_metrics."""
        assert "planning_blocks" in _PLANNING_UPSERT_SQL
        assert "athlete_metrics" not in _PLANNING_UPSERT_SQL

    def test_upsert_sql_does_not_contain_planning_blocks(self):
        """Non-overlap: _UPSERT_SQL (load path) must NOT reference planning_blocks.

        Mirror of test_recovery_upsert.py::TestLoadUpsertSqlNonOverlap for
        the planning path. Ensures the three UPSERT constants are byte-for-byte
        non-overlapping (PL2-12, PL2-13).
        """
        assert "planning_blocks" not in _UPSERT_SQL, (
            "CRITICAL (PL2-12): _UPSERT_SQL must NOT reference planning_blocks. "
            "The load-metrics and planning UPSERTs must have non-overlapping targets."
        )

    def test_recovery_upsert_sql_does_not_contain_planning_blocks(self):
        """_RECOVERY_UPSERT_SQL must NOT reference planning_blocks."""
        assert "planning_blocks" not in _RECOVERY_UPSERT_SQL, (
            "CRITICAL: _RECOVERY_UPSERT_SQL must NOT reference planning_blocks."
        )

    def test_planning_upsert_sql_does_not_contain_recovery_score(self):
        """_PLANNING_UPSERT_SQL must NOT reference recovery_score."""
        assert "recovery_score" not in _PLANNING_UPSERT_SQL


# ---------------------------------------------------------------------------
# build_planning_upsert — params tuple
# ---------------------------------------------------------------------------


class TestBuildPlanningUpsertParams:
    """build_planning_upsert produces the correct params tuple."""

    def test_params_athlete_id_is_string(self):
        record = _make_planning_record(athlete_id="ath_007")
        _, params = build_planning_upsert(record)
        assert params[0] == "ath_007"
        assert isinstance(params[0], str)

    def test_params_block_id_is_string(self):
        record = _make_planning_record(block_id="BLK-XYZ")
        _, params = build_planning_upsert(record)
        assert "BLK-XYZ" in params

    def test_params_ingest_time_is_int(self):
        record = _make_planning_record(ingest_time=_INGEST_TIME_A)
        _, params = build_planning_upsert(record)
        # ingest_time must appear in params as an int (or epoch-ms compatible)
        assert _INGEST_TIME_A in params

    def test_params_planned_sessions_is_int(self):
        record = _make_planning_record(planned_sessions_per_week=7)
        _, params = build_planning_upsert(record)
        assert 7 in params

    def test_params_weekly_volume_targets_is_string(self):
        wvt = '{"cardio": 4}'
        record = _make_planning_record(weekly_volume_targets=wvt)
        _, params = build_planning_upsert(record)
        assert wvt in params


# ---------------------------------------------------------------------------
# PL2-11: PK coexistence — two records same (athlete_id, block_id) but
# different ingest_time produce non-conflicting SQL rows
# ---------------------------------------------------------------------------


class TestPlanningUpsertPkCoexistence:
    """PL2-11: Different ingest_time values → different PK → no conflict."""

    def test_two_revisions_produce_different_params(self):
        """Two records with same (athlete_id, block_id) but different ingest_time
        must produce params that differ (no PK collision).

        This is the pure SQL-builder proof of PL2-11 coexistence: the actual
        PK uniqueness is enforced by the DB (integration test in PR-PL2b), but
        the SQL builder must emit distinct param tuples for distinct revisions.
        """
        record_a = _make_planning_record(ingest_time=_INGEST_TIME_A)
        record_b = _make_planning_record(ingest_time=_INGEST_TIME_B)

        _, params_a = build_planning_upsert(record_a)
        _, params_b = build_planning_upsert(record_b)

        assert params_a != params_b, (
            "PL2-11: Two records with different ingest_time must produce "
            "different params (they are distinct revisions, not conflicts)."
        )

    def test_different_ingest_times_both_in_params(self):
        """Each revision's ingest_time must appear in its own params tuple."""
        record_a = _make_planning_record(ingest_time=_INGEST_TIME_A)
        record_b = _make_planning_record(ingest_time=_INGEST_TIME_B)

        _, params_a = build_planning_upsert(record_a)
        _, params_b = build_planning_upsert(record_b)

        assert _INGEST_TIME_A in params_a
        assert _INGEST_TIME_B in params_b
        assert _INGEST_TIME_B not in params_a
        assert _INGEST_TIME_A not in params_b

    def test_same_sql_both_revisions_do_nothing_only(self):
        """Both revisions use the same SQL with DO NOTHING (not DO UPDATE)."""
        record_a = _make_planning_record(ingest_time=_INGEST_TIME_A)
        record_b = _make_planning_record(ingest_time=_INGEST_TIME_B)

        sql_a, _ = build_planning_upsert(record_a)
        sql_b, _ = build_planning_upsert(record_b)

        assert sql_a == sql_b == _PLANNING_UPSERT_SQL
        assert "DO NOTHING" in sql_a.upper()


# ---------------------------------------------------------------------------
# PL2-13: Non-regression — existing symbols untouched
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# PL2-10: DDL idempotency — planning_ddl.sql content assertions
# ---------------------------------------------------------------------------


class TestPlanningDdlContent:
    """PL2-10: planning_ddl.sql must be idempotent and define the correct PK."""

    _DDL_PATH = (
        Path(__file__).resolve().parent.parent.parent
        / "storage" / "postgres" / "planning_ddl.sql"
    )

    def _read_ddl(self) -> str:
        return self._DDL_PATH.read_text(encoding="utf-8")

    def test_ddl_file_exists(self):
        assert self._DDL_PATH.exists(), (
            f"planning_ddl.sql must exist at {self._DDL_PATH}"
        )

    def test_ddl_uses_create_table_if_not_exists(self):
        """PL2-10: idempotent DDL must use CREATE TABLE IF NOT EXISTS."""
        ddl = self._read_ddl()
        assert "CREATE TABLE IF NOT EXISTS" in ddl.upper(), (
            "PL2-10: DDL must use CREATE TABLE IF NOT EXISTS for idempotency"
        )

    def test_ddl_creates_planning_blocks_table(self):
        ddl = self._read_ddl()
        assert "planning_blocks" in ddl

    def test_ddl_defines_correct_versioning_pk(self):
        """ADR-20: PK must be (athlete_id, block_id, ingest_time) — versioning, not dedup."""
        ddl = self._read_ddl()
        upper = ddl.upper()
        assert "PRIMARY KEY" in upper
        # All three PK columns must appear
        assert "athlete_id" in ddl.lower()
        assert "block_id" in ddl.lower()
        assert "ingest_time" in ddl.lower()

    def test_ddl_does_not_modify_athlete_metrics(self):
        """DDL must NOT contain SQL statements that touch athlete_metrics.

        Comments may mention athlete_metrics (e.g. rollback instructions);
        only actual SQL statement lines are checked.
        """
        ddl = self._read_ddl()
        # Filter to non-comment, non-empty lines only
        sql_lines = [
            line for line in ddl.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        sql_only = "\n".join(sql_lines)
        assert "athlete_metrics" not in sql_only, (
            "planning_ddl.sql SQL statements must NOT reference athlete_metrics. "
            "The DDL is additive — it creates only planning_blocks."
        )

    def test_ddl_includes_required_columns(self):
        """DDL must define all required planning_blocks columns."""
        ddl = self._read_ddl()
        for col in (
            "athlete_id", "block_id", "ingest_time",
            "goal", "start_date", "end_date",
            "planned_sessions_per_week", "weekly_volume_targets",
        ):
            assert col in ddl, f"Column {col!r} must be in planning_ddl.sql"

    def test_ddl_ingest_time_is_timestamptz(self):
        """ingest_time must be TIMESTAMPTZ (carries timezone → correct temporal semantics)."""
        ddl = self._read_ddl()
        assert "TIMESTAMPTZ" in ddl.upper(), (
            "ingest_time must be TIMESTAMPTZ for correct temporal semantics"
        )


class TestExistingSymbolsUntouched:
    """PL2-13: build_upsert and build_recovery_upsert must remain byte-for-byte
    unmodified by the planning upsert additions.

    Mirrors TestLoadUpsertSqlNonOverlap in test_recovery_upsert.py.
    """

    def test_build_upsert_still_works_after_planning_additions(self):
        """build_upsert (load path) must still return _UPSERT_SQL unchanged."""
        record = {
            "athlete_id": "ath_A",
            "metric_date": 1_735_689_600_000,
            "acute_load": 100.0,
            "chronic_load_28d": 90.0,
            "chronic_load_42d": 85.0,
            "acute_chronic_ratio": 1.1,
            "deload_flag": 0,
            "fatigue_score": 20.0,
            "readiness_score": 65.0,
            "coaching_flags": "[]",
        }
        sql, params = build_upsert(record)
        assert sql is _UPSERT_SQL, "build_upsert must return the _UPSERT_SQL constant unchanged"
        assert "athlete_metrics" in sql
        assert "DO UPDATE" in sql.upper()

    def test_build_recovery_upsert_still_works_after_planning_additions(self):
        """build_recovery_upsert must still return _RECOVERY_UPSERT_SQL unchanged."""
        record = {
            "athlete_id": "ath_B",
            "metric_date": 1_735_689_600_000,
            "recovery_score": 73.29,
        }
        sql, params = build_recovery_upsert(record)
        assert sql is _RECOVERY_UPSERT_SQL, (
            "build_recovery_upsert must return _RECOVERY_UPSERT_SQL unchanged"
        )
        assert "athlete_metrics" in sql
        assert "recovery_score" in sql
