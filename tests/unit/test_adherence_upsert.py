"""Unit tests for storage.postgres.sink — adherence_score UPSERT (ADH-D2, ADH-D3, ADH-D4).

Also verifies ddl.sql includes adherence_score (ADH-D1 smoke), that
existing SQL constants are byte-for-byte unchanged, and that the metrics
router SELECT query includes adherence_score (ADH-A1/A2 contract).

These are pure unit tests — no Docker, no real DB required.
"""

from __future__ import annotations

import datetime

import pytest

from api.routers.metrics import _SQL_METRICS_RANGE
from storage.postgres.sink import (
    _ADHERENCE_UPSERT_SQL,  # type: ignore[attr-defined]
    _PLANNING_UPSERT_SQL,  # type: ignore[attr-defined]
    _RECOVERY_UPSERT_SQL,  # type: ignore[attr-defined]
    _UPSERT_SQL,
    build_adherence_upsert,
    build_planning_upsert,
    build_recovery_upsert,
    build_upsert,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JUN_1_2025 = datetime.date(2025, 6, 1)
_JAN_1_2025_EPOCH_MS = 1_735_689_600_000  # 2025-01-01 00:00:00 UTC


def _make_adherence_record(
    *,
    athlete_id: str = "A1",
    metric_date: datetime.date = _JUN_1_2025,
    adherence_score: "float | None" = 0.75,
) -> dict:
    return {
        "athlete_id": athlete_id,
        "metric_date": metric_date,
        "adherence_score": adherence_score,
    }


# ---------------------------------------------------------------------------
# ADH-D1 smoke: ddl.sql mentions adherence_score
# ---------------------------------------------------------------------------


class TestDdlContainsAdherenceScore:
    """ADH-D1: ddl.sql must contain adherence_score ADD COLUMN statement."""

    def test_ddl_contains_adherence_score(self):
        from pathlib import Path

        ddl_path = (
            Path(__file__).resolve().parent.parent.parent
            / "storage" / "postgres" / "ddl.sql"
        )
        ddl = ddl_path.read_text(encoding="utf-8")
        assert "adherence_score" in ddl, (
            "ADH-D1: ddl.sql must contain an adherence_score ADD COLUMN statement"
        )

    def test_ddl_adherence_uses_if_not_exists(self):
        from pathlib import Path

        ddl_path = (
            Path(__file__).resolve().parent.parent.parent
            / "storage" / "postgres" / "ddl.sql"
        )
        ddl = ddl_path.read_text(encoding="utf-8")
        assert "IF NOT EXISTS" in ddl.upper(), (
            "ADH-D1: adherence_score DDL must use IF NOT EXISTS for idempotency"
        )


# ---------------------------------------------------------------------------
# ADH-D2: build_adherence_upsert — SQL only touches adherence_score
# ---------------------------------------------------------------------------


class TestBuildAdherenceUpsertSqlIsolation:
    """ADH-D2: build_adherence_upsert SQL must only touch adherence_score."""

    def test_sql_inserts_into_athlete_metrics(self):
        record = _make_adherence_record()
        sql, _ = build_adherence_upsert(record)
        assert "INSERT INTO athlete_metrics" in sql

    def test_sql_lists_only_adherence_columns(self):
        """INSERT must list ONLY (athlete_id, metric_date, adherence_score)."""
        record = _make_adherence_record()
        sql, _ = build_adherence_upsert(record)
        assert "adherence_score" in sql
        assert "athlete_id" in sql
        assert "metric_date" in sql
        # Load and recovery columns must NOT appear
        for forbidden in (
            "acute_load", "chronic_load_28d", "chronic_load_42d",
            "deload_flag", "fatigue_score", "readiness_score",
            "coaching_flags", "recovery_score",
        ):
            assert forbidden not in sql, (
                f"ADH-D2: Column {forbidden!r} must NOT appear in _ADHERENCE_UPSERT_SQL"
            )

    def test_sql_has_on_conflict_do_update_adherence_score_only(self):
        """DO UPDATE SET must reference ONLY adherence_score (non-overlap)."""
        record = _make_adherence_record()
        sql, _ = build_adherence_upsert(record)
        assert "ON CONFLICT" in sql.upper()
        assert "DO UPDATE" in sql.upper()
        assert "adherence_score" in sql
        # Other columns must not be in the UPDATE clause
        for forbidden in (
            "acute_load", "recovery_score", "fatigue_score",
            "readiness_score", "coaching_flags",
        ):
            assert forbidden not in sql, (
                f"ADH-D2: {forbidden!r} must NOT appear in adherence UPSERT SQL"
            )

    def test_sql_conflict_target_is_athlete_id_metric_date(self):
        record = _make_adherence_record()
        sql, _ = build_adherence_upsert(record)
        assert "athlete_id" in sql
        assert "metric_date" in sql


# ---------------------------------------------------------------------------
# ADH-D2: build_adherence_upsert — params tuple
# ---------------------------------------------------------------------------


class TestBuildAdherenceUpsertParams:
    """build_adherence_upsert returns correct (sql, params) 3-tuple."""

    def test_params_is_three_tuple(self):
        record = _make_adherence_record()
        _, params = build_adherence_upsert(record)
        assert len(params) == 3, f"Expected 3-tuple, got {len(params)}: {params}"

    def test_params_athlete_id_is_string(self):
        record = _make_adherence_record(athlete_id="ath_007")
        _, params = build_adherence_upsert(record)
        assert params[0] == "ath_007"
        assert isinstance(params[0], str)

    def test_params_metric_date_is_date_object(self):
        """metric_date must be a datetime.date — NOT epoch-ms (ADH key constraint)."""
        record = _make_adherence_record(metric_date=_JUN_1_2025)
        _, params = build_adherence_upsert(record)
        assert isinstance(params[1], datetime.date), (
            f"params[1] must be datetime.date, got {type(params[1]).__name__}: {params[1]!r}"
        )
        assert params[1] == _JUN_1_2025, (
            f"Expected {_JUN_1_2025}, got {params[1]!r}"
        )

    def test_params_metric_date_is_not_int(self):
        """metric_date must NEVER be passed as epoch-ms int (not recovery's path)."""
        record = _make_adherence_record(metric_date=_JUN_1_2025)
        _, params = build_adherence_upsert(record)
        assert not isinstance(params[1], int), (
            "CRITICAL: build_adherence_upsert must NOT call epoch_ms_to_date. "
            "metric_date is already a datetime.date in the adherence path."
        )

    def test_params_adherence_score_is_float(self):
        record = _make_adherence_record(adherence_score=0.75)
        _, params = build_adherence_upsert(record)
        assert isinstance(params[2], float), (
            f"params[2] must be float, got {type(params[2]).__name__}"
        )
        assert abs(params[2] - 0.75) < 0.001

    def test_params_adherence_score_none_passthrough(self):
        """adherence_score=None must bind as SQL NULL (None)."""
        record = _make_adherence_record(adherence_score=None)
        _, params = build_adherence_upsert(record)
        assert params[2] is None, (
            f"adherence_score=None must bind as None (SQL NULL), got {params[2]!r}"
        )

    def test_params_different_athlete_and_score(self):
        """Triangulation: different inputs produce different params."""
        record = _make_adherence_record(athlete_id="bob", adherence_score=0.5)
        _, params = build_adherence_upsert(record)
        assert params[0] == "bob"
        assert abs(params[2] - 0.5) < 0.001


# ---------------------------------------------------------------------------
# Non-overlap: existing SQL constants are byte-for-byte unchanged
# ---------------------------------------------------------------------------


class TestExistingSymbolsNonOverlap:
    """ADH-D3/D4: Existing UPSERT constants must be unchanged; no cross-contamination."""

    def test_upsert_sql_does_not_contain_adherence_score(self):
        """Load-metrics _UPSERT_SQL must NOT reference adherence_score."""
        assert "adherence_score" not in _UPSERT_SQL, (
            "CRITICAL (ADH-D3): _UPSERT_SQL must NOT contain 'adherence_score'. "
            "Load path and adherence path must be non-overlapping."
        )

    def test_recovery_upsert_sql_does_not_contain_adherence_score(self):
        """_RECOVERY_UPSERT_SQL must NOT reference adherence_score."""
        assert "adherence_score" not in _RECOVERY_UPSERT_SQL, (
            "CRITICAL (ADH-D4): _RECOVERY_UPSERT_SQL must NOT contain 'adherence_score'."
        )

    def test_adherence_upsert_sql_does_not_contain_recovery_score(self):
        """_ADHERENCE_UPSERT_SQL must NOT touch recovery_score."""
        assert "recovery_score" not in _ADHERENCE_UPSERT_SQL

    def test_adherence_upsert_sql_does_not_contain_load_columns(self):
        """_ADHERENCE_UPSERT_SQL must NOT touch load columns."""
        for col in ("acute_load", "chronic_load_28d", "chronic_load_42d", "deload_flag"):
            assert col not in _ADHERENCE_UPSERT_SQL, (
                f"Column {col!r} must NOT appear in _ADHERENCE_UPSERT_SQL"
            )

    def test_build_upsert_still_returns_load_sql(self):
        """Non-regression: build_upsert (load path) must be byte-for-byte unchanged."""
        record = {
            "athlete_id": "ath_A",
            "metric_date": _JAN_1_2025_EPOCH_MS,
            "acute_load": 100.0,
            "chronic_load_28d": 90.0,
            "chronic_load_42d": 85.0,
            "acute_chronic_ratio": 1.1,
            "deload_flag": 0,
            "fatigue_score": 20.0,
            "readiness_score": 65.0,
            "coaching_flags": "[]",
        }
        sql, _ = build_upsert(record)
        assert sql is _UPSERT_SQL, "build_upsert must return _UPSERT_SQL (unchanged)"
        assert "adherence_score" not in sql

    def test_build_recovery_upsert_still_returns_recovery_sql(self):
        """Non-regression: build_recovery_upsert must be byte-for-byte unchanged."""
        record = {
            "athlete_id": "ath_B",
            "metric_date": _JAN_1_2025_EPOCH_MS,
            "recovery_score": 73.29,
        }
        sql, _ = build_recovery_upsert(record)
        assert sql is _RECOVERY_UPSERT_SQL, (
            "build_recovery_upsert must return _RECOVERY_UPSERT_SQL (unchanged)"
        )
        assert "adherence_score" not in sql

    def test_build_planning_upsert_still_returns_planning_sql(self):
        """Non-regression: build_planning_upsert must be byte-for-byte unchanged."""
        record = {
            "athlete_id": "ath_C",
            "block_id": "BLK-001",
            "ingest_time": 1_748_740_000_000,
            "goal": "test",
            "start_date": 1_748_736_000_000,
            "end_date": 1_756_598_400_000,
            "planned_sessions_per_week": 5,
            "weekly_volume_targets": "{}",
        }
        sql, _ = build_planning_upsert(record)
        assert sql is _PLANNING_UPSERT_SQL, (
            "build_planning_upsert must return _PLANNING_UPSERT_SQL (unchanged)"
        )
        assert "adherence_score" not in sql


# ---------------------------------------------------------------------------
# ADH-A1/A2: _SQL_METRICS_RANGE must SELECT adherence_score (API contract)
# ---------------------------------------------------------------------------


class TestMetricsRouterSqlIncludesAdherenceScore:
    """The metrics router SELECT must include adherence_score (ADH-A1/A2 contract)."""

    def test_adh_a1_sql_metrics_range_selects_adherence_score(self):
        """ADH-A1: _SQL_METRICS_RANGE must SELECT adherence_score from athlete_metrics."""
        assert "adherence_score" in _SQL_METRICS_RANGE, (
            "ADH-A1: _SQL_METRICS_RANGE must include 'adherence_score' in the SELECT list. "
            f"Current SQL:\n{_SQL_METRICS_RANGE}"
        )

    def test_adh_a2_sql_not_null_placeholder(self):
        """adherence_score must be a real column reference, not NULL AS adherence_score."""
        import re
        null_pattern = re.search(r'NULL\s+AS\s+adherence_score', _SQL_METRICS_RANGE, re.IGNORECASE)
        assert null_pattern is None, (
            "adherence_score must be a real column SELECT, not NULL AS adherence_score"
        )
