"""Unit tests for storage.postgres.sink (task 6.1, PR5 Phase 6).

Pure, pyflink-free tests. No Docker, no real PostgreSQL connection.
All DB interaction is tested via a fake cursor that records execute() calls.

Scenarios covered (strict TDD):
  - epoch_ms_to_date: epoch-ms day-start -> correct UTC date
  - epoch_ms_to_date: boundary at exactly midnight UTC
  - build_upsert: SQL contains ON CONFLICT (athlete_id, metric_date) DO UPDATE
  - build_upsert: SQL updates all metric columns
  - build_upsert: acute_chronic_ratio None -> bound param is None
  - build_upsert: acute_chronic_ratio float('nan') -> bound param is None
  - build_upsert: normal record binds all 7 fields correctly
  - execute_upsert: batch of N records executes N upserts (or executemany)
"""

from __future__ import annotations

import datetime
import math
from unittest.mock import MagicMock, call

import pytest

from storage.postgres.sink import (
    build_upsert,
    epoch_ms_to_date,
    execute_upsert,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    athlete_id: str = "athlete_1",
    metric_date: int = 1_700_000_000_000,
    acute_load: float = 100.0,
    chronic_load_28d: float = 80.0,
    chronic_load_42d: float = 75.0,
    acute_chronic_ratio: "float | None" = 1.25,
    deload_flag: int = 0,
) -> dict:
    return {
        "athlete_id": athlete_id,
        "metric_date": metric_date,
        "acute_load": acute_load,
        "chronic_load_28d": chronic_load_28d,
        "chronic_load_42d": chronic_load_42d,
        "acute_chronic_ratio": acute_chronic_ratio,
        "deload_flag": deload_flag,
    }


# ---------------------------------------------------------------------------
# epoch_ms_to_date tests
# ---------------------------------------------------------------------------

class TestEpochMsToDate:
    """epoch_ms_to_date converts day-start epoch-ms to the exact UTC calendar date."""

    def test_known_epoch_ms_maps_to_correct_date(self):
        # 2023-11-14 00:00:00 UTC = 1699920000000 ms
        epoch_ms = 1_699_920_000_000
        result = epoch_ms_to_date(epoch_ms)
        assert result == datetime.date(2023, 11, 14)

    def test_boundary_exactly_midnight_utc(self):
        # 2024-01-01 00:00:00 UTC = 1704067200000 ms
        epoch_ms = 1_704_067_200_000
        result = epoch_ms_to_date(epoch_ms)
        assert result == datetime.date(2024, 1, 1)

    def test_different_epoch_ms_produces_different_date(self):
        # 2024-03-15 00:00:00 UTC = 1710460800000 ms
        epoch_ms = 1_710_460_800_000
        result = epoch_ms_to_date(epoch_ms)
        assert result == datetime.date(2024, 3, 15)

    def test_epoch_ms_zero_is_1970_01_01(self):
        # epoch 0 = 1970-01-01 00:00:00 UTC
        result = epoch_ms_to_date(0)
        assert result == datetime.date(1970, 1, 1)


# ---------------------------------------------------------------------------
# build_upsert tests
# ---------------------------------------------------------------------------

class TestBuildUpsert:
    """build_upsert returns (sql, params) for a single metrics record."""

    def test_sql_contains_on_conflict_do_update(self):
        record = _make_record()
        sql, _ = build_upsert(record)
        assert "ON CONFLICT" in sql.upper()
        assert "DO UPDATE" in sql.upper()

    def test_sql_conflict_target_is_athlete_id_and_metric_date(self):
        record = _make_record()
        sql, _ = build_upsert(record)
        # Must conflict on exactly (athlete_id, metric_date)
        assert "athlete_id" in sql
        assert "metric_date" in sql

    def test_sql_updates_all_metric_columns(self):
        record = _make_record()
        sql, _ = build_upsert(record)
        sql_upper = sql.upper()
        for col in ("ACUTE_LOAD", "CHRONIC_LOAD_28D", "CHRONIC_LOAD_42D",
                    "ACUTE_CHRONIC_RATIO", "DELOAD_FLAG"):
            assert col in sql_upper, f"Column {col!r} missing from UPDATE clause"

    def test_acute_chronic_ratio_none_becomes_sql_null(self):
        record = _make_record(acute_chronic_ratio=None)
        _, params = build_upsert(record)
        # Find the acr value in params — it must be Python None (-> SQL NULL)
        # params is a tuple/dict; we look for the None value at the right position
        assert None in params, "acute_chronic_ratio=None must bind as None (SQL NULL)"

    def test_acute_chronic_ratio_nan_becomes_sql_null(self):
        record = _make_record(acute_chronic_ratio=float("nan"))
        _, params = build_upsert(record)
        assert None in params, "acute_chronic_ratio=nan must bind as None (SQL NULL)"

    def test_normal_record_binds_all_7_fields(self):
        record = _make_record(
            athlete_id="ath_42",
            metric_date=1_704_067_200_000,   # 2024-01-01
            acute_load=110.5,
            chronic_load_28d=82.3,
            chronic_load_42d=79.1,
            acute_chronic_ratio=1.34,
            deload_flag=1,
        )
        sql, params = build_upsert(record)
        # All 7 fields must appear in params (as a tuple or list)
        params_list = list(params)
        assert "ath_42" in params_list, "athlete_id missing from params"
        assert datetime.date(2024, 1, 1) in params_list, "metric_date (as DATE) missing from params"
        assert 110.5 in params_list, "acute_load missing from params"
        assert 82.3 in params_list, "chronic_load_28d missing from params"
        assert 79.1 in params_list, "chronic_load_42d missing from params"
        # 1.34 is a real float — must be present (not None)
        assert any(
            isinstance(p, float) and math.isclose(p, 1.34)
            for p in params_list
        ), "acute_chronic_ratio missing from params"
        assert 1 in params_list, "deload_flag missing from params"

    def test_normal_record_acr_is_not_none_when_finite(self):
        record = _make_record(acute_chronic_ratio=0.95)
        _, params = build_upsert(record)
        params_list = list(params)
        assert None not in params_list, "acr=0.95 must NOT bind as None"
        assert any(
            isinstance(p, float) and math.isclose(p, 0.95)
            for p in params_list
        ), "acr value 0.95 missing from params"


# ---------------------------------------------------------------------------
# execute_upsert tests (fake cursor)
# ---------------------------------------------------------------------------

class TestExecuteUpsert:
    """execute_upsert drives a psycopg2 cursor with the parameterized UPSERT."""

    def test_single_record_calls_execute_once(self):
        fake_cursor = MagicMock()
        record = _make_record()
        execute_upsert(fake_cursor, record)
        assert fake_cursor.execute.call_count == 1

    def test_single_record_execute_called_with_sql_and_params(self):
        fake_cursor = MagicMock()
        record = _make_record()
        execute_upsert(fake_cursor, record)
        # execute must have been called with exactly (sql, params)
        assert fake_cursor.execute.called
        args = fake_cursor.execute.call_args
        sql_arg, params_arg = args[0][0], args[0][1]
        assert isinstance(sql_arg, str), "first arg to execute must be a SQL string"
        assert params_arg is not None, "second arg to execute must be the params"

    def test_batch_of_records_executes_n_upserts(self):
        fake_cursor = MagicMock()
        records = [
            _make_record(athlete_id=f"ath_{i}", metric_date=1_704_067_200_000 + i * 86_400_000)
            for i in range(5)
        ]
        for rec in records:
            execute_upsert(fake_cursor, rec)
        assert fake_cursor.execute.call_count == 5, (
            f"expected 5 execute() calls for 5 records, got {fake_cursor.execute.call_count}"
        )

    def test_batch_execute_produces_correct_athlete_ids(self):
        """Each execute() call must bind the correct athlete_id for its record."""
        fake_cursor = MagicMock()
        athlete_ids = ["alice", "bob", "carol"]
        base_ms = 1_704_067_200_000
        records = [
            _make_record(athlete_id=aid, metric_date=base_ms + i * 86_400_000)
            for i, aid in enumerate(athlete_ids)
        ]
        for rec in records:
            execute_upsert(fake_cursor, rec)

        all_calls = fake_cursor.execute.call_args_list
        assert len(all_calls) == 3
        for i, aid in enumerate(athlete_ids):
            _, params = all_calls[i][0]
            assert aid in list(params), f"expected {aid!r} in params for call {i}"
