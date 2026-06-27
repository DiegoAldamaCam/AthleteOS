"""Unit tests for storage.postgres.sink — recovery_score UPSERT (W3-7 unit, W3-8 unit).

Verifies:
  - build_recovery_upsert returns correct SQL and 3-tuple (athlete_id, metric_date, recovery_score)
  - _RECOVERY_UPSERT_SQL is a separate constant from _UPSERT_SQL
  - _UPSERT_SQL does NOT contain recovery_score (non-overlap guarantee W3-8)
  - ON CONFLICT touches ONLY recovery_score (no load column cross-contamination)

These are pure unit tests — no Docker, no real DB required.
"""

from __future__ import annotations

import datetime

import pytest

from storage.postgres.sink import (
    _RECOVERY_UPSERT_SQL,  # type: ignore[attr-defined]
    _UPSERT_SQL,
    build_recovery_upsert,
    epoch_ms_to_date,
    upsert_with_retry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JAN_1_2025_EPOCH_MS = 1_735_689_600_000  # 2025-01-01 00:00:00 UTC


def _make_recovery_record(
    athlete_id: str = "athlete_1",
    metric_date: int = _JAN_1_2025_EPOCH_MS,
    recovery_score: float = 73.29,
) -> dict:
    return {
        "athlete_id": athlete_id,
        "metric_date": metric_date,
        "recovery_score": recovery_score,
    }


# ---------------------------------------------------------------------------
# build_recovery_upsert — SQL structure
# ---------------------------------------------------------------------------


class TestBuildRecoveryUpsertSql:
    """build_recovery_upsert returns (sql, params) with the correct SQL structure."""

    def test_sql_contains_insert_into_athlete_metrics(self):
        record = _make_recovery_record()
        sql, _ = build_recovery_upsert(record)
        assert "INSERT INTO athlete_metrics" in sql

    def test_sql_lists_only_three_columns(self):
        """INSERT must list ONLY (athlete_id, metric_date, recovery_score)."""
        record = _make_recovery_record()
        sql, _ = build_recovery_upsert(record)
        # The column list should contain exactly these three — and NOT the load cols
        assert "athlete_id" in sql
        assert "metric_date" in sql
        assert "recovery_score" in sql
        # Load columns must be absent from the INSERT list
        for load_col in ("acute_load", "chronic_load_28d", "chronic_load_42d", "deload_flag"):
            assert load_col not in sql, (
                f"Load column {load_col!r} must NOT appear in _RECOVERY_UPSERT_SQL"
            )

    def test_sql_has_on_conflict_do_update_recovery_score_only(self):
        """DO UPDATE SET must reference ONLY recovery_score (non-overlap ADR-17)."""
        record = _make_recovery_record()
        sql, _ = build_recovery_upsert(record)
        assert "ON CONFLICT" in sql.upper()
        assert "DO UPDATE" in sql.upper()
        # DO UPDATE SET recovery_score must be present
        assert "recovery_score" in sql
        # fatigue_score, readiness_score, acute_load must NOT be in UPDATE clause
        for forbidden_col in (
            "fatigue_score", "readiness_score", "coaching_flags",
            "acute_load", "chronic_load_28d", "chronic_load_42d", "deload_flag",
        ):
            assert forbidden_col not in sql, (
                f"Column {forbidden_col!r} must NOT appear in recovery UPSERT SQL"
            )

    def test_sql_conflict_target_is_athlete_id_and_metric_date(self):
        record = _make_recovery_record()
        sql, _ = build_recovery_upsert(record)
        # ON CONFLICT (athlete_id, metric_date)
        assert "athlete_id" in sql
        assert "metric_date" in sql


# ---------------------------------------------------------------------------
# build_recovery_upsert — params tuple
# ---------------------------------------------------------------------------


class TestBuildRecoveryUpsertParams:
    """build_recovery_upsert produces the correct 3-tuple of bound parameters."""

    def test_params_is_three_tuple(self):
        record = _make_recovery_record()
        _, params = build_recovery_upsert(record)
        assert len(params) == 3, f"Expected 3-tuple, got {len(params)}: {params}"

    def test_params_athlete_id_is_string(self):
        record = _make_recovery_record(athlete_id="athlete_42")
        _, params = build_recovery_upsert(record)
        assert params[0] == "athlete_42", f"params[0] must be athlete_id str, got {params[0]!r}"

    def test_params_metric_date_is_date_object(self):
        record = _make_recovery_record(metric_date=_JAN_1_2025_EPOCH_MS)
        _, params = build_recovery_upsert(record)
        expected_date = datetime.date(2025, 1, 1)
        assert params[1] == expected_date, (
            f"params[1] must be datetime.date(2025,1,1), got {params[1]!r}"
        )

    def test_params_recovery_score_is_float(self):
        record = _make_recovery_record(recovery_score=73.29)
        _, params = build_recovery_upsert(record)
        assert isinstance(params[2], float), f"params[2] must be float, got {type(params[2])}"
        assert abs(params[2] - 73.29) < 0.01, (
            f"params[2] must be ≈73.29, got {params[2]!r}"
        )

    def test_params_recovery_score_none_passes_through(self):
        """recovery_score=None must be passed through as None (SQL NULL)."""
        record = {
            "athlete_id": "ath_x",
            "metric_date": _JAN_1_2025_EPOCH_MS,
            "recovery_score": None,
        }
        _, params = build_recovery_upsert(record)
        assert params[2] is None, (
            f"recovery_score=None must bind as SQL NULL (None), got {params[2]!r}"
        )

    def test_params_different_athlete_and_score(self):
        """Triangulation: different inputs produce different params."""
        record = _make_recovery_record(athlete_id="bob", recovery_score=55.0)
        _, params = build_recovery_upsert(record)
        assert params[0] == "bob"
        assert abs(params[2] - 55.0) < 0.01


# ---------------------------------------------------------------------------
# W3-8: _UPSERT_SQL does NOT contain recovery_score
# ---------------------------------------------------------------------------


class TestLoadUpsertSqlNonOverlap:
    """W3-8 unit: the load-metrics _UPSERT_SQL must NOT reference recovery_score."""

    def test_upsert_sql_does_not_contain_recovery_score(self):
        """W3-8: _UPSERT_SQL (load path) must NOT name recovery_score anywhere.

        This is the byte-level non-overlap guarantee: even if the DB row has
        a recovery_score column, the load UPSERT never touches it.
        """
        assert "recovery_score" not in _UPSERT_SQL, (
            "CRITICAL (W3-8): _UPSERT_SQL must NOT contain 'recovery_score'. "
            "The load and recovery UPSERTs must have non-overlapping column sets."
        )

    def test_recovery_upsert_sql_does_not_contain_load_columns(self):
        """_RECOVERY_UPSERT_SQL must not touch load-only columns."""
        for col in ("acute_load", "fatigue_score", "readiness_score", "coaching_flags"):
            assert col not in _RECOVERY_UPSERT_SQL, (
                f"_RECOVERY_UPSERT_SQL must NOT contain load column {col!r}"
            )


# ---------------------------------------------------------------------------
# FIX 1 proof: upsert_with_retry with build_fn=build_recovery_upsert uses
# _RECOVERY_UPSERT_SQL (not the load SQL)
# ---------------------------------------------------------------------------


class TestUpsertWithRetryBuildFnInjection:
    """Proves build_fn is plumbed through upsert_with_retry → execute_upsert.

    Uses a fake cursor that captures (sql, params) so we can assert which SQL
    constant was actually executed — without a real DB.
    """

    def test_upsert_with_retry_uses_recovery_sql_when_build_fn_injected(self):
        """upsert_with_retry(..., build_fn=build_recovery_upsert) must call
        cursor.execute with _RECOVERY_UPSERT_SQL, not _UPSERT_SQL.

        This is the FIX-1 proof: before the fix, the hardcoded build_upsert call
        inside execute_upsert would trigger KeyError: 'acute_load' on a recovery
        record and the wrong SQL would be used.
        """
        from unittest.mock import MagicMock

        executed_sqls: list[str] = []

        fake_cursor = MagicMock()
        fake_cursor.__enter__ = lambda s: s
        fake_cursor.__exit__ = MagicMock(return_value=False)
        fake_cursor.execute.side_effect = lambda sql, params: executed_sqls.append(sql)

        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        fake_conn.closed = 0

        record = {
            "athlete_id": "ath_fix1",
            "metric_date": _JAN_1_2025_EPOCH_MS,
            "recovery_score": 73.29,
        }

        upsert_with_retry(
            record,
            fake_conn,
            lambda: fake_conn,
            max_retries=1,
            base_backoff_s=0.0,
            build_fn=build_recovery_upsert,
        )

        assert len(executed_sqls) == 1, f"Expected exactly 1 execute call, got {len(executed_sqls)}"
        used_sql = executed_sqls[0]
        assert used_sql == _RECOVERY_UPSERT_SQL, (
            "FIX-1: upsert_with_retry with build_fn=build_recovery_upsert must use "
            "_RECOVERY_UPSERT_SQL, not the load SQL. "
            f"Got:\n{used_sql!r}\nExpected:\n{_RECOVERY_UPSERT_SQL!r}"
        )
        assert _UPSERT_SQL not in executed_sqls, (
            "FIX-1: The load _UPSERT_SQL must NOT be called when build_fn=build_recovery_upsert"
        )

    def test_upsert_with_retry_default_still_uses_load_sql(self):
        """upsert_with_retry with default build_fn must still use _UPSERT_SQL.

        Regression guard: FIX-1 must not break the existing load path.
        """
        from unittest.mock import MagicMock

        executed_sqls: list[str] = []

        fake_cursor = MagicMock()
        fake_cursor.__enter__ = lambda s: s
        fake_cursor.__exit__ = MagicMock(return_value=False)
        fake_cursor.execute.side_effect = lambda sql, params: executed_sqls.append(sql)

        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        fake_conn.closed = 0

        record = {
            "athlete_id": "ath_load",
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

        upsert_with_retry(
            record,
            fake_conn,
            lambda: fake_conn,
            max_retries=1,
            base_backoff_s=0.0,
            # No build_fn → defaults to build_upsert
        )

        assert len(executed_sqls) == 1
        assert executed_sqls[0] == _UPSERT_SQL, (
            "Default build_fn must produce _UPSERT_SQL (load path regression guard)"
        )
