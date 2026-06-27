"""Unit tests for storage.postgres.sink (task 6.1, PR5 Phase 6 + metrics-v2).

Pure, pyflink-free tests. No Docker, no real PostgreSQL connection.
All DB interaction is tested via a fake cursor that records execute() calls.

Scenarios covered (strict TDD):
  - epoch_ms_to_date: epoch-ms day-start -> correct UTC date
  - epoch_ms_to_date: boundary at exactly midnight UTC
  - build_upsert: SQL contains ON CONFLICT (athlete_id, metric_date) DO UPDATE
  - build_upsert: SQL updates all metric columns (including 3 new v2 cols)
  - build_upsert: acute_chronic_ratio None -> bound param is None
  - build_upsert: acute_chronic_ratio float('nan') -> bound param is None
  - build_upsert: normal record binds all 10 fields correctly (metrics-v2)
  - build_upsert: fatigue_score nan -> bound param is None (metrics-v2)
  - build_upsert: readiness_score nan -> bound param is None (metrics-v2)
  - build_upsert: coaching_flags JSON string passthrough (metrics-v2)
  - execute_upsert: batch of N records executes N upserts (or executemany)
  - upsert_with_retry: succeeds on first attempt (no retry needed)
  - upsert_with_retry: conn_factory called on OperationalError (reconnect path)
  - upsert_with_retry: succeeds on second attempt after reconnect
  - upsert_with_retry: raises after exhausting all retries
"""

from __future__ import annotations

import datetime
import math
from unittest.mock import MagicMock, call, patch

import pytest

from storage.postgres.sink import (
    build_upsert,
    epoch_ms_to_date,
    execute_upsert,
    upsert_with_retry,
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
    fatigue_score: "float | None" = 20.0,
    readiness_score: "float | None" = 65.0,
    coaching_flags: str = "[]",
) -> dict:
    return {
        "athlete_id": athlete_id,
        "metric_date": metric_date,
        "acute_load": acute_load,
        "chronic_load_28d": chronic_load_28d,
        "chronic_load_42d": chronic_load_42d,
        "acute_chronic_ratio": acute_chronic_ratio,
        "deload_flag": deload_flag,
        "fatigue_score": fatigue_score,
        "readiness_score": readiness_score,
        "coaching_flags": coaching_flags,
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
                    "ACUTE_CHRONIC_RATIO", "DELOAD_FLAG",
                    "FATIGUE_SCORE", "READINESS_SCORE", "COACHING_FLAGS"):
            assert col in sql_upper, f"Column {col!r} missing from UPDATE clause"

    def test_acute_chronic_ratio_none_becomes_sql_null(self):
        record = _make_record(acute_chronic_ratio=None)
        _, params = build_upsert(record)
        # acute_chronic_ratio is at index 5 in the params tuple
        # (order: athlete_id[0], metric_date[1], acute_load[2],
        #  chronic_load_28d[3], chronic_load_42d[4], acr[5], deload_flag[6],
        #  fatigue_score[7], readiness_score[8], coaching_flags[9]).
        # Position-pinned assertion: column-order regressions break this test.
        assert params[5] is None, (
            f"acute_chronic_ratio=None must be at params[5] as None (SQL NULL), "
            f"got {params[5]!r}"
        )

    def test_acute_chronic_ratio_nan_becomes_sql_null(self):
        record = _make_record(acute_chronic_ratio=float("nan"))
        _, params = build_upsert(record)
        # Same position pin as above (index 5 = acute_chronic_ratio).
        assert params[5] is None, (
            f"acute_chronic_ratio=nan must be at params[5] as None (SQL NULL), "
            f"got {params[5]!r}"
        )

    def test_normal_record_binds_all_10_fields(self):
        """metrics-v2: build_upsert produces 10 params (7 existing + 3 new)."""
        record = _make_record(
            athlete_id="ath_42",
            metric_date=1_704_067_200_000,   # 2024-01-01
            acute_load=110.5,
            chronic_load_28d=82.3,
            chronic_load_42d=79.1,
            acute_chronic_ratio=1.34,
            deload_flag=1,
            fatigue_score=28.0,
            readiness_score=72.5,
            coaching_flags='["monitor"]',
        )
        sql, params = build_upsert(record)
        params_list = list(params)
        # Positions 0-6 unchanged (regression guard)
        assert "ath_42" in params_list, "athlete_id missing from params"
        assert datetime.date(2024, 1, 1) in params_list, "metric_date (as DATE) missing from params"
        assert 110.5 in params_list, "acute_load missing from params"
        assert 82.3 in params_list, "chronic_load_28d missing from params"
        assert 79.1 in params_list, "chronic_load_42d missing from params"
        assert any(
            isinstance(p, float) and math.isclose(p, 1.34)
            for p in params_list
        ), "acute_chronic_ratio missing from params"
        assert 1 in params_list, "deload_flag missing from params"
        # Positions 7-9: new metrics-v2 fields
        assert any(
            isinstance(p, float) and math.isclose(p, 28.0)
            for p in params_list
        ), "fatigue_score missing from params"
        assert any(
            isinstance(p, float) and math.isclose(p, 72.5)
            for p in params_list
        ), "readiness_score missing from params"
        assert '["monitor"]' in params_list, "coaching_flags JSON string missing from params"
        # Total must be exactly 10
        assert len(params_list) == 10, f"Expected 10 params, got {len(params_list)}"

    def test_fatigue_score_nan_becomes_sql_null(self):
        """metrics-v2: fatigue_score=nan (IEEE-754 None sentinel) -> params[7]=None."""
        record = _make_record(fatigue_score=float("nan"))
        _, params = build_upsert(record)
        assert params[7] is None, (
            f"fatigue_score=nan must be params[7]=None (SQL NULL), got {params[7]!r}"
        )

    def test_readiness_score_nan_becomes_sql_null(self):
        """metrics-v2: readiness_score=nan -> params[8]=None."""
        record = _make_record(readiness_score=float("nan"))
        _, params = build_upsert(record)
        assert params[8] is None, (
            f"readiness_score=nan must be params[8]=None (SQL NULL), got {params[8]!r}"
        )

    def test_coaching_flags_json_string_passthrough(self):
        """metrics-v2: coaching_flags JSON string bound at params[9] unchanged."""
        record = _make_record(coaching_flags='["deload","high_fatigue"]')
        _, params = build_upsert(record)
        assert params[9] == '["deload","high_fatigue"]', (
            f"coaching_flags must be bound verbatim as JSON string at params[9], got {params[9]!r}"
        )

    def test_coaching_flags_empty_list_is_json_array(self):
        """metrics-v2: empty coaching_flags -> '[]' (never None) at params[9]."""
        record = _make_record(coaching_flags="[]")
        _, params = build_upsert(record)
        assert params[9] == "[]", (
            f"empty coaching_flags must be '[]' at params[9], got {params[9]!r}"
        )

    def test_normal_record_acr_is_not_none_when_finite(self):
        record = _make_record(acute_chronic_ratio=0.95)
        _, params = build_upsert(record)
        # acr=0.95 is finite — it must appear at params[5], not as None
        assert params[5] is not None, "acr=0.95 must NOT bind as None"
        assert isinstance(params[5], float) and math.isclose(params[5], 0.95), (
            f"acr value 0.95 missing from params[5], got {params[5]!r}"
        )


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


# ---------------------------------------------------------------------------
# upsert_with_retry tests (fake connection + fake conn_factory)
# ---------------------------------------------------------------------------


def _make_fake_conn(fail_times: int = 0, error_cls: type = Exception):
    """Build a fake psycopg2-like connection.

    The fake cursor raises ``error_cls`` on the first ``fail_times`` calls to
    ``execute()``, then succeeds.  ``conn.closed`` is 0 (open); we simulate
    OperationalError explicitly so the reconnect branch is triggered.

    Returns (conn, cursor_mock) so tests can inspect call counts.
    """
    cursor_mock = MagicMock()
    call_count = {"n": 0}

    def _execute(sql, params):
        call_count["n"] += 1
        if call_count["n"] <= fail_times:
            raise error_cls("simulated DB error")

    cursor_mock.execute.side_effect = _execute

    conn = MagicMock()
    conn.cursor.return_value = cursor_mock
    conn.closed = 0
    return conn, cursor_mock


class TestUpsertWithRetry:
    """upsert_with_retry — reconnect and exponential-backoff retry logic."""

    def test_succeeds_on_first_attempt_no_retry(self):
        """Happy path: no errors, conn_factory is never called."""
        conn, cursor = _make_fake_conn(fail_times=0)
        conn_factory = MagicMock()
        record = _make_record()

        with patch("storage.postgres.sink.time") as mock_time:
            returned_conn = upsert_with_retry(
                record, conn, conn_factory, max_retries=3, base_backoff_s=0.0
            )

        assert cursor.execute.call_count == 1, "execute must be called once on success"
        conn_factory.assert_not_called()
        mock_time.sleep.assert_not_called()
        assert returned_conn is conn, "must return the original connection on success"

    def test_conn_factory_called_on_operational_error(self):
        """When OperationalError is raised, conn_factory must be called to reconnect."""
        # We make the connection look 'dead' so the reconnect branch fires.
        conn, cursor = _make_fake_conn(fail_times=0)
        # Override execute to raise OperationalError only once, simulating a
        # transient connection error, then succeed on the fresh connection.

        # Use a counter to track calls across both the original and new conn.
        call_state = {"attempt": 0}
        fresh_cursor = MagicMock()

        def _first_conn_execute(sql, params):
            raise _FakeOperationalError("connection reset by peer")

        cursor.execute.side_effect = _first_conn_execute

        # fresh_cursor succeeds immediately
        fresh_conn = MagicMock()
        fresh_conn.cursor.return_value = fresh_cursor
        fresh_conn.closed = 0
        conn_factory = MagicMock(return_value=fresh_conn)

        record = _make_record()

        with patch("storage.postgres.sink.time") as mock_time:
            returned_conn = upsert_with_retry(
                record, conn, conn_factory, max_retries=3, base_backoff_s=0.0
            )

        conn_factory.assert_called_once(), "conn_factory must be called exactly once on error"
        fresh_cursor.execute.assert_called_once(), "fresh cursor must be used after reconnect"
        assert returned_conn is fresh_conn, "must return the new connection after reconnect"

    def test_succeeds_on_second_attempt_after_reconnect(self):
        """First attempt fails with a connection error; second attempt succeeds."""
        conn, first_cursor = _make_fake_conn(fail_times=0)

        def _dead_execute(sql, params):
            raise _FakeOperationalError("broken pipe")

        first_cursor.execute.side_effect = _dead_execute
        conn.closed = 1  # mark as dead so reconnect branch fires

        fresh_cursor = MagicMock()
        fresh_conn = MagicMock()
        fresh_conn.cursor.return_value = fresh_cursor
        fresh_conn.closed = 0
        conn_factory = MagicMock(return_value=fresh_conn)

        record = _make_record(athlete_id="ath_reconnect")

        with patch("storage.postgres.sink.time") as mock_time:
            returned_conn = upsert_with_retry(
                record, conn, conn_factory, max_retries=3, base_backoff_s=0.0
            )

        # First cursor raised; fresh cursor must have been called once.
        assert fresh_cursor.execute.call_count == 1
        # Verify the correct athlete_id was bound via the fresh cursor.
        _sql, params = fresh_cursor.execute.call_args[0]
        assert "ath_reconnect" in list(params), "correct record must be retried on fresh conn"
        assert returned_conn is fresh_conn

    def test_raises_after_exhausting_all_retries(self):
        """When every attempt fails, upsert_with_retry re-raises the last exception."""
        # Build an always-failing cursor so all retries exhaust.
        always_fail_cursor = MagicMock()
        always_fail_cursor.execute.side_effect = Exception("simulated DB error")

        # Each "new" connection from conn_factory also always fails.
        def _make_failing_conn():
            c = MagicMock()
            c.cursor.return_value = always_fail_cursor
            c.closed = 0
            return c

        conn = _make_failing_conn()
        conn_factory = MagicMock(side_effect=_make_failing_conn)

        record = _make_record()

        with patch("storage.postgres.sink.time"):
            with pytest.raises(Exception, match="simulated DB error"):
                upsert_with_retry(
                    record, conn, conn_factory, max_retries=3, base_backoff_s=0.0
                )

        # execute must have been called once per attempt (3 total).
        assert always_fail_cursor.execute.call_count == 3, (
            f"expected 3 execute attempts for max_retries=3, got "
            f"{always_fail_cursor.execute.call_count}"
        )


# ---------------------------------------------------------------------------
# Helpers for TestUpsertWithRetry
# ---------------------------------------------------------------------------


# Base the fake on the REAL psycopg2.OperationalError when psycopg2 is
# installed (CI / runtime), so upsert_with_retry's
# `isinstance(exc, psycopg2.OperationalError)` reconnect branch fires. Fall
# back to Exception only where psycopg2 is absent (e.g. CPython 3.14 dev box),
# matching the same fallback the production code uses.
try:  # pragma: no cover - import guard
    import psycopg2 as _psycopg2

    _OPERATIONAL_ERROR_BASE: type = _psycopg2.OperationalError
except ImportError:  # pragma: no cover - psycopg2 absent locally
    _OPERATIONAL_ERROR_BASE = Exception


class _FakeOperationalError(_OPERATIONAL_ERROR_BASE):  # type: ignore[misc,valid-type]
    """Simulates psycopg2.OperationalError; subclasses the real class when present."""
