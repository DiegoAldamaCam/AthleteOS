"""Unit tests for the PURE metrics-computation logic (PR4, task 5.1-5.4 pure half).

These tests run WITHOUT pyflink and WITHOUT Docker -- they exercise
``jobs.metrics.compute`` which is deliberately pyflink-free so the spec metric
formulas (daily_load, acute/chronic rolling windows, ACR, deload state machine)
have full unit coverage on any interpreter (CPython 3.14 included).

Source of truth: serving-store spec "Metric Formulas".
  daily_load(d)       = sum(session_load on day d)
  acute_load          = sum(daily_load for d in [t-6, t])           -- 7d rolling SUM
  chronic_load_28d    = sum(daily_load for d in [t-27, t]) / n      -- 28d rolling AVG, n=days present (ADR-16)
  chronic_load_42d    = sum(daily_load for d in [t-41, t]) / n      -- 42d rolling AVG, n=days present (ADR-16)
  acute_chronic_ratio = acute_load / chronic_load_28d               -- NULL if chronic=0
  deload_flag         = +1 if ACR>1.3 for >=3 consecutive days
                      | -1 if ACR<0.8 for >=3 consecutive days
                      | 0  otherwise
"""

from __future__ import annotations

import math

import pytest

import json

from jobs.metrics.compute import (
    ACUTE_WINDOW_DAYS,
    CHRONIC_28D_WINDOW_DAYS,
    CHRONIC_42D_WINDOW_DAYS,
    DELOAD_HIGH,
    DELOAD_LOW,
    DELOAD_NORMAL,
    LATE_DATA,
    METRICS_SOURCE_TOPIC,
    MILLIS_PER_DAY,
    VALIDATION_FAILURE,
    acute_chronic_ratio,
    acute_load,
    build_metrics_dlq_envelope,
    chronic_load,
    compute_coaching_flags,
    compute_deload_flags,
    compute_fatigue_score,
    compute_readiness_score,
    compute_rolling_metrics,
    is_finite_load,
    metrics_row_to_json,
    sum_loads,
    update_deload_state,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DAY_MS = MILLIS_PER_DAY  # alias for brevity in rolling-window tests

# A simple by_day fixture: N consecutive days starting from day 0.
# day_start_ms for day i = i * _DAY_MS (window_end is just above the last day).


def _by_day(n_days: int, load_per_day: float = 100.0) -> dict[int, float]:
    """Return {day_start_ms: load} for days 0..n_days-1."""
    return {i * _DAY_MS: load_per_day for i in range(n_days)}


def _window_end(n_days: int) -> int:
    """window_end = n_days * MILLIS_PER_DAY (exclusive, day-aligned)."""
    return n_days * _DAY_MS


# --- compute_rolling_metrics (C8 single source of truth) -------------------


class TestComputeRollingMetrics:
    """Prove that compute_rolling_metrics() is the canonical window formula.

    These tests directly test the pure function that the Flink window operator
    calls, guaranteeing the implementation running in prod is the same one that
    passed the unit tests. (C8 readability / single source of truth)
    """

    def test_7_days_full_window(self):
        # 7 days @100: acute=700, chronic_28d=100 (/7=100), ACR=7.0
        by_day = _by_day(7)
        w_end = _window_end(7)
        al, cl28, cl42, acr = compute_rolling_metrics(by_day, w_end)
        assert al == pytest.approx(700.0)
        assert cl28 == pytest.approx(100.0)
        assert cl42 == pytest.approx(100.0)
        assert acr == pytest.approx(7.0)

    def test_single_day_acr_is_one_dynamic_denominator(self):
        # Day 1 only: acute=100, chronic_28d=100/1=100 -> ACR=1.0
        # (with /n denominator, 1 day -> chronic=100 not 100/28)
        by_day = _by_day(1)
        w_end = _window_end(1)
        al, cl28, cl42, acr = compute_rolling_metrics(by_day, w_end)
        assert al == pytest.approx(100.0)
        assert cl28 == pytest.approx(100.0)  # /n: 1 day
        assert acr == pytest.approx(1.0)  # 100/100

    def test_empty_window_acr_none(self):
        # Empty by_day -> all zeros -> ACR=None (spec: NULL if chronic=0)
        al, cl28, cl42, acr = compute_rolling_metrics({}, _window_end(1))
        assert al == 0.0
        assert cl28 == 0.0
        assert acr is None

    def test_28_days_full_window(self):
        # 28 days @100: acute=700 (last 7), chronic_28d=100, ACR=7.0
        by_day = _by_day(28)
        w_end = _window_end(28)
        al, cl28, cl42, acr = compute_rolling_metrics(by_day, w_end)
        assert al == pytest.approx(700.0)
        assert cl28 == pytest.approx(100.0)
        assert acr == pytest.approx(7.0)

    def test_42_days_all_windows_populated(self):
        # 42 days @100: acute=700, chronic_28d=100, chronic_42d=100, ACR=7.0
        by_day = _by_day(42)
        w_end = _window_end(42)
        al, cl28, cl42, acr = compute_rolling_metrics(by_day, w_end)
        assert al == pytest.approx(700.0)
        assert cl28 == pytest.approx(100.0)
        assert cl42 == pytest.approx(100.0)
        assert acr == pytest.approx(7.0)

    def test_partial_window_dynamic_denominator_adr16(self):
        # ADR-16: 3 days @100 in a 42d window -> chronic_28d=100/3=100,
        # not 300/28=10.7. Proves the pure function is used (not inline /28).
        by_day = _by_day(3)
        w_end = _window_end(3)
        al, cl28, cl42, acr = compute_rolling_metrics(by_day, w_end)
        assert cl28 == pytest.approx(100.0), (
            f"chronic_28d must be 100.0 (300/3, /n denominator); got {cl28}"
        )


# --- daily_load = sum of session_load on a day -----------------------------


class TestSumLoads:
    def test_single_value(self):
        assert sum_loads([100.0]) == 100.0

    def test_multiple_values(self):
        # spec: daily_load(d) = sum(session_load on day d)
        assert sum_loads([100.0, 200.0, 300.0]) == 600.0

    def test_empty_is_zero(self):
        assert sum_loads([]) == 0.0

    def test_nan_raises(self):
        # NaN guard: a NaN session_load must not silently corrupt a daily sum;
        # the Flink layer routes NaN to the DLQ. The pure sum surfaces it loudly.
        with pytest.raises(ValueError):
            sum_loads([100.0, float("nan")])

    def test_inf_raises(self):
        with pytest.raises(ValueError):
            sum_loads([100.0, float("inf")])


# --- acute_load = 7-day rolling SUM ----------------------------------------


class TestAcuteLoad:
    def test_seven_days_of_100_is_700(self):
        # spec ACR scenario: acute_load=700
        daily = [100.0] * ACUTE_WINDOW_DAYS
        assert acute_load(daily) == 700.0

    def test_six_days_excluded(self):
        # acute is a SUM over exactly the 7 daily loads passed in
        daily = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
        assert acute_load(daily) == 280.0

    def test_empty_window_is_zero(self):
        assert acute_load([]) == 0.0


# --- chronic_load = rolling AVERAGE ----------------------------------------


class TestChronicLoad:
    def test_28d_average_of_100_is_100(self):
        daily = [100.0] * CHRONIC_28D_WINDOW_DAYS
        assert chronic_load(daily) == 100.0

    def test_42d_average_of_100_is_100(self):
        daily = [100.0] * CHRONIC_42D_WINDOW_DAYS
        assert chronic_load(daily) == 100.0

    def test_28d_average_mixed(self):
        # sum=2800 over 28 days -> avg=100
        daily = [100.0] * 28
        assert chronic_load(daily) == 100.0

    def test_28d_with_one_outlier(self):
        # 27 days of 100 + 1 day of 2800 -> sum=5500 -> avg=5500/28=196.43
        daily = [100.0] * 27 + [2800.0]
        assert chronic_load(daily) == pytest.approx(5500.0 / 28.0)

    def test_empty_average_is_zero(self):
        # avoid div-by-zero: empty window -> 0.0 (no chronic baseline yet)
        assert chronic_load([]) == 0.0

    # --- ADR-16 partial-window tests (dynamic /n denominator) ---------------
    # chronic_load uses a DYNAMIC denominator /n (days present), NOT fixed /28
    # or /42. Sports-science rationale: a new athlete with few training days
    # must not get an artificially depressed chronic baseline (fixed /28 would
    # produce chronic=300/28≈10.7 when only 3 days exist, inflating ACR to
    # implausible levels and triggering false deload alerts). The dynamic
    # denominator reflects the athlete's ACTUAL average load over the days
    # present, making ACR meaningful from day 1. See ADR-16 in design.md.

    def test_partial_28d_window_3_days_dynamic_denominator(self):
        # ADR-16: 3 days @100 -> chronic = 300/3 = 100 (NOT 300/28 ≈ 10.7).
        # Proves the formula is /n, not /28, for a new athlete's first days.
        result = chronic_load([100.0] * 3)
        assert result == pytest.approx(100.0), (
            f"chronic_load([100]*3) must be 100.0 (300/3, dynamic /n); "
            f"got {result} (if ≈10.7 the formula incorrectly uses /28)"
        )

    def test_partial_28d_window_27_days_dynamic_denominator(self):
        # ADR-16: 27 days @100 -> chronic = 2700/27 = 100 (NOT 2700/28 ≈ 96.4).
        result = chronic_load([100.0] * 27)
        assert result == pytest.approx(100.0), (
            f"chronic_load([100]*27) must be 100.0 (2700/27, dynamic /n); "
            f"got {result} (if ≈96.4 the formula incorrectly uses /28)"
        )

    def test_partial_42d_window_3_days_dynamic_denominator(self):
        # ADR-16: 3 days @100 -> chronic = 300/3 = 100 (same function, 42d caller).
        # The function is window-size agnostic; the caller passes n days from
        # whichever window (28d or 42d). /n holds for both.
        result = chronic_load([100.0] * 3)
        assert result == pytest.approx(100.0)

    def test_partial_42d_window_27_days_dynamic_denominator(self):
        # ADR-16: 27 days in a 42d window -> chronic = 2700/27 = 100 (NOT /42).
        result = chronic_load([100.0] * 27)
        assert result == pytest.approx(100.0)


# --- acute_chronic_ratio = acute / chronic_28d (NULL if chronic=0) ---------


class TestAcuteChronicRatio:
    def test_spec_scenario_700_over_500_is_1_4(self):
        # spec "Scenario: ACR computation": acute=700, chronic=500 -> ACR=1.4
        assert acute_chronic_ratio(700.0, 500.0) == pytest.approx(1.4)

    def test_chronic_zero_returns_none(self):
        # spec: NULL if chronic_load_28d = 0
        assert acute_chronic_ratio(700.0, 0.0) is None

    def test_exact_threshold_not_breach(self):
        # ACR exactly 1.3 is NOT > 1.3 (strict); ratio still computed
        assert acute_chronic_ratio(130.0, 100.0) == pytest.approx(1.3)


# --- ACR None propagation (chronic=0 -> deload resets, no false DELOAD_LOW) -


class TestAcrNoneDeloadReset:
    def test_chronic_zero_deload_state_resets(self):
        # CRITICAL C3+F2: When ACR is None (chronic==0, guaranteed on athlete
        # day 1), the deload state machine MUST reset (count=0, sign=NORMAL,
        # flag=NORMAL). It must NOT assert a DELOAD_LOW (-1) streak, which
        # would otherwise happen if None were coerced to 0.0 < 0.8.
        count, sign, flag = update_deload_state(0, DELOAD_NORMAL, None)
        assert flag == DELOAD_NORMAL, (
            f"ACR=None (chronic=0) must yield DELOAD_NORMAL, got flag={flag}. "
            "None must NOT be treated as 0.0 < 0.8 (false DELOAD_LOW)."
        )
        assert count == 0
        assert sign == DELOAD_NORMAL

    def test_three_days_chronic_zero_no_deload_low(self):
        # A new athlete's first 3 days all have chronic=0 -> ACR=None for each.
        # Running the state machine must NOT produce DELOAD_LOW on day 3.
        flags = compute_deload_flags([None, None, None])
        assert all(f == DELOAD_NORMAL for f in flags), (
            f"New athlete with 3 None-ACR days must have all NORMAL flags; "
            f"got {flags}. False DELOAD_LOW streak detected."
        )

    def test_none_acr_resets_existing_streak_then_high_restarts(self):
        # High streak of 2 days, then chronic=0 (None ACR), then high again.
        # The None day resets the streak; new streak starts from 1.
        # After the None: [high, high, None, high, high, high] -> trigger on day 6.
        flags = compute_deload_flags([1.4, 1.4, None, 1.4, 1.4, 1.4])
        assert flags[2] == DELOAD_NORMAL, "None-ACR day must be NORMAL"
        assert flags[5] == DELOAD_HIGH, "3rd consecutive high after reset must trigger"


# --- deload state machine (consecutive-day rule) ---------------------------


class TestUpdateDeloadState:
    def test_first_high_breach_no_flag(self):
        # 1 day of ACR>1.3 -> count=1, sign=+1, flag=0 (need 3)
        count, sign, flag = update_deload_state(0, 0, 1.4)
        assert (count, sign, flag) == (1, DELOAD_HIGH, DELOAD_NORMAL)

    def test_second_high_breach_no_flag(self):
        count, sign, flag = update_deload_state(1, DELOAD_HIGH, 1.4)
        assert (count, sign, flag) == (2, DELOAD_HIGH, DELOAD_NORMAL)

    def test_third_high_breach_triggers_plus_one(self):
        # spec: +1 if ACR>1.3 for >=3 consecutive days
        count, sign, flag = update_deload_state(2, DELOAD_HIGH, 1.4)
        assert (count, sign, flag) == (3, DELOAD_HIGH, DELOAD_HIGH)

    def test_third_low_breach_triggers_minus_one(self):
        # spec: -1 if ACR<0.8 for >=3 consecutive days
        count, sign, flag = update_deload_state(2, DELOAD_LOW, 0.7)
        assert (count, sign, flag) == (3, DELOAD_LOW, DELOAD_LOW)

    def test_normal_resets_streak(self):
        # a normal day between breaches resets the counter
        count, sign, flag = update_deload_state(2, DELOAD_HIGH, 1.0)
        assert (count, sign, flag) == (0, DELOAD_NORMAL, DELOAD_NORMAL)

    def test_sign_flip_resets_count(self):
        # going from a high streak to a low day starts a fresh low streak
        count, sign, flag = update_deload_state(2, DELOAD_HIGH, 0.7)
        assert (count, sign, flag) == (1, DELOAD_LOW, DELOAD_NORMAL)

    def test_none_acr_resets(self):
        # chronic=0 -> ACR None -> cannot assert a breach -> reset
        count, sign, flag = update_deload_state(2, DELOAD_HIGH, None)
        assert (count, sign, flag) == (0, DELOAD_NORMAL, DELOAD_NORMAL)

    def test_acr_exact_1_3_is_normal(self):
        # strict >: 1.3 is not a high breach
        count, sign, flag = update_deload_state(0, 0, 1.3)
        assert sign == DELOAD_NORMAL

    def test_acr_exact_0_8_is_normal(self):
        # strict <: 0.8 is not a low breach
        count, sign, flag = update_deload_state(0, 0, 0.8)
        assert sign == DELOAD_NORMAL

    def test_continuation_stays_plus_one(self):
        # 4th consecutive high day -> still +1 (streak continues)
        count, sign, flag = update_deload_state(3, DELOAD_HIGH, 1.4)
        assert (count, sign, flag) == (4, DELOAD_HIGH, DELOAD_HIGH)


class TestComputeDeloadFlags:
    def test_three_consecutive_high_triggers_on_day_three(self):
        # spec "Scenario: Deload flag triggered": ACR>1.3 on days 18,19,20
        # -> deload_flag=+1 on day 20
        flags = compute_deload_flags([1.4, 1.4, 1.4])
        assert flags == [0, 0, DELOAD_HIGH]

    def test_three_consecutive_low_triggers_minus_one(self):
        flags = compute_deload_flags([0.7, 0.7, 0.7])
        assert flags == [0, 0, DELOAD_LOW]

    def test_broken_streak_does_not_trigger(self):
        # high, high, normal, high, high, high -> triggers on the 6th (3 fresh)
        flags = compute_deload_flags([1.4, 1.4, 1.0, 1.4, 1.4, 1.4])
        assert flags == [0, 0, 0, 0, 0, DELOAD_HIGH]

    def test_all_normal(self):
        flags = compute_deload_flags([1.0, 1.1, 0.9, 1.2])
        assert flags == [0, 0, 0, 0]

    def test_none_acr_resets_streak(self):
        # high, high, None (chronic=0), high, high, high -> trigger on day 6
        flags = compute_deload_flags([1.4, 1.4, None, 1.4, 1.4, 1.4])
        assert flags == [0, 0, 0, 0, 0, DELOAD_HIGH]

    def test_continuation_emits_plus_one_each_day(self):
        flags = compute_deload_flags([1.4, 1.4, 1.4, 1.4, 1.4])
        assert flags == [0, 0, DELOAD_HIGH, DELOAD_HIGH, DELOAD_HIGH]

    def test_empty(self):
        assert compute_deload_flags([]) == []


# --- is_finite_load (NaN guard helper) -------------------------------------


class TestIsFiniteLoad:
    def test_finite_float(self):
        assert is_finite_load(100.0) is True

    def test_finite_int(self):
        assert is_finite_load(100) is True

    def test_nan(self):
        assert is_finite_load(float("nan")) is False

    def test_pos_inf(self):
        assert is_finite_load(float("inf")) is False

    def test_neg_inf(self):
        assert is_finite_load(float("-inf")) is False

    def test_none(self):
        assert is_finite_load(None) is False

    def test_bool_rejected(self):
        # bool is an int subclass; a session_load must be a real number, not a flag
        assert is_finite_load(True) is False


# --- constant sanity (guards against accidental spec drift) ----------------


class TestConstants:
    def test_millis_per_day(self):
        assert MILLIS_PER_DAY == 86_400_000

    def test_window_sizes(self):
        assert ACUTE_WINDOW_DAYS == 7
        assert CHRONIC_28D_WINDOW_DAYS == 28
        assert CHRONIC_42D_WINDOW_DAYS == 42

    def test_deload_flags(self):
        assert DELOAD_HIGH == 1
        assert DELOAD_LOW == -1
        assert DELOAD_NORMAL == 0


# --- DLQ envelope (spec shape) --------------------------------------------


class TestMetricsDlqEnvelope:
    def test_nan_guard_envelope(self):
        env = build_metrics_dlq_envelope(
            original_key="athlete-1",
            original_value=b'{"session_load": "NaN"}',
            error_type=VALIDATION_FAILURE,
            error_message="session_load is NaN",
            timestamp=1_700_000_000_000,
        )
        assert env["original_topic"] == METRICS_SOURCE_TOPIC
        assert env["original_key"] == "athlete-1"
        assert env["error_type"] == VALIDATION_FAILURE
        assert env["error_message"] == "session_load is NaN"
        assert env["timestamp"] == 1_700_000_000_000
        # original_value is base64 of the bytes
        import base64

        assert base64.b64decode(env["original_value"]) == b'{"session_load": "NaN"}'

    def test_late_data_envelope(self):
        env = build_metrics_dlq_envelope(
            original_key="athlete-1",
            original_value="late-row",
            error_type=LATE_DATA,
            error_message="event arrived past window-end + 24h allowed lateness",
            timestamp=1_700_000_000_001,
        )
        assert env["error_type"] == LATE_DATA

    def test_none_original_value_is_empty_string(self):
        env = build_metrics_dlq_envelope(
            original_key=None,
            original_value=None,
            error_type=VALIDATION_FAILURE,
            error_message="missing",
            timestamp=1,
        )
        assert env["original_value"] == ""
        assert env["original_key"] is None


# --- metrics_row_to_json (NF-2: RFC-8259-safe JSON serialization) ----------


class TestMetricsRowToJson:
    """Prove metrics_row_to_json() produces valid RFC 8259 JSON.

    NF-2 (CRITICAL): The previous implementation used json.dumps default
    allow_nan=True, which emits the non-standard tokens `NaN` and `Infinity`
    for non-finite float values -- invalid per RFC 8259. The PostgreSQL
    consumer in PR5 would crash on any such message.

    Fix: metrics_row_to_json uses allow_nan=False so any non-finite numeric
    value raises ValueError immediately (fail-fast -> DLQ), rather than
    emitting a poison message to the metrics stream.
    """

    def test_finite_values_produce_valid_json(self):
        # Happy path: all-finite inputs round-trip through json.loads cleanly.
        result = metrics_row_to_json(
            athlete_id="ath-1",
            metric_date=1_700_000_000_000,
            acute_load_val=700.0,
            chronic_load_28d_val=100.0,
            chronic_load_42d_val=100.0,
            acr_val=7.0,
            deload_flag=0,
        )
        parsed = json.loads(result)  # raises if invalid JSON
        assert parsed["athlete_id"] == "ath-1"
        assert parsed["acute_load"] == pytest.approx(700.0)
        assert parsed["chronic_load_28d"] == pytest.approx(100.0)
        assert parsed["chronic_load_42d"] == pytest.approx(100.0)
        assert parsed["acute_chronic_ratio"] == pytest.approx(7.0)
        assert parsed["deload_flag"] == 0

    def test_none_acr_serializes_as_json_null(self):
        # ACR=None (chronic=0) must become JSON null, not the string "None".
        result = metrics_row_to_json(
            athlete_id="ath-2",
            metric_date=1_700_000_000_000,
            acute_load_val=100.0,
            chronic_load_28d_val=0.0,
            chronic_load_42d_val=0.0,
            acr_val=None,
            deload_flag=0,
        )
        parsed = json.loads(result)
        assert parsed["acute_chronic_ratio"] is None

    def test_nan_acr_already_guarded_to_null(self):
        # NaN ACR (IEEE-754 sentinel from prior guard) must become JSON null.
        result = metrics_row_to_json(
            athlete_id="ath-3",
            metric_date=1_700_000_000_000,
            acute_load_val=100.0,
            chronic_load_28d_val=0.0,
            chronic_load_42d_val=0.0,
            acr_val=float("nan"),
            deload_flag=0,
        )
        parsed = json.loads(result)
        assert parsed["acute_chronic_ratio"] is None

    def test_nan_acute_load_raises_value_error(self):
        # NF-2 core: a non-finite load field must raise ValueError (fail-fast
        # -> DLQ) rather than emitting the non-standard `NaN` token.
        with pytest.raises(ValueError):
            metrics_row_to_json(
                athlete_id="ath-4",
                metric_date=1_700_000_000_000,
                acute_load_val=float("nan"),
                chronic_load_28d_val=100.0,
                chronic_load_42d_val=100.0,
                acr_val=None,
                deload_flag=0,
            )

    def test_nan_chronic_load_28d_raises_value_error(self):
        # Non-finite chronic_load_28d must fail fast, not emit `NaN`.
        with pytest.raises(ValueError):
            metrics_row_to_json(
                athlete_id="ath-5",
                metric_date=1_700_000_000_000,
                acute_load_val=100.0,
                chronic_load_28d_val=float("nan"),
                chronic_load_42d_val=100.0,
                acr_val=None,
                deload_flag=0,
            )

    def test_nan_chronic_load_42d_raises_value_error(self):
        # Non-finite chronic_load_42d must fail fast, not emit `NaN`.
        with pytest.raises(ValueError):
            metrics_row_to_json(
                athlete_id="ath-6",
                metric_date=1_700_000_000_000,
                acute_load_val=100.0,
                chronic_load_28d_val=100.0,
                chronic_load_42d_val=float("nan"),
                acr_val=None,
                deload_flag=0,
            )

    def test_inf_acute_load_raises_value_error(self):
        # +Inf (also non-standard JSON) must fail fast via allow_nan=False.
        with pytest.raises(ValueError):
            metrics_row_to_json(
                athlete_id="ath-7",
                metric_date=1_700_000_000_000,
                acute_load_val=float("inf"),
                chronic_load_28d_val=100.0,
                chronic_load_42d_val=100.0,
                acr_val=None,
                deload_flag=0,
            )

    def test_output_is_valid_rfc8259_json(self):
        # Triangulation: use different field values; json.loads is the RFC 8259
        # parser -- success proves no NaN/Infinity token was emitted.
        result = metrics_row_to_json(
            athlete_id="ath-8",
            metric_date=1_000_000_000_000,
            acute_load_val=350.5,
            chronic_load_28d_val=250.25,
            chronic_load_42d_val=275.0,
            acr_val=1.4,
            deload_flag=1,
        )
        parsed = json.loads(result)  # raises json.JSONDecodeError if invalid
        assert parsed["deload_flag"] == 1
        assert parsed["metric_date"] == 1_000_000_000_000

    # FIX 3: v2 fields (fatigue_score, readiness_score, coaching_flags) in JSON output.
    def test_v2_fields_included_in_json(self):
        """FIX 3: metrics_row_to_json must include fatigue_score, readiness_score,
        coaching_flags in the JSON output (Kafka staging topic matches PG schema)."""
        result = metrics_row_to_json(
            athlete_id="ath-v2",
            metric_date=1_700_000_000_000,
            acute_load_val=200.0,
            chronic_load_28d_val=100.0,
            chronic_load_42d_val=100.0,
            acr_val=2.0,
            deload_flag=1,
            fatigue_score_val=40.0,
            readiness_score_val=30.0,
            coaching_flags_val=["deload", "monitor"],
        )
        parsed = json.loads(result)
        assert "fatigue_score" in parsed, "fatigue_score missing from JSON output"
        assert "readiness_score" in parsed, "readiness_score missing from JSON output"
        assert "coaching_flags" in parsed, "coaching_flags missing from JSON output"
        assert parsed["fatigue_score"] == pytest.approx(40.0)
        assert parsed["readiness_score"] == pytest.approx(30.0)
        assert parsed["coaching_flags"] == ["deload", "monitor"]

    def test_v2_nan_fatigue_score_becomes_null(self):
        """FIX 3: NaN fatigue_score must serialize as null (not raise ValueError)."""
        result = metrics_row_to_json(
            athlete_id="ath-v2b",
            metric_date=1_700_000_000_000,
            acute_load_val=100.0,
            chronic_load_28d_val=100.0,
            chronic_load_42d_val=100.0,
            acr_val=1.0,
            deload_flag=0,
            fatigue_score_val=float("nan"),
            readiness_score_val=None,
            coaching_flags_val=[],
        )
        parsed = json.loads(result)
        assert parsed["fatigue_score"] is None, (
            f"NaN fatigue_score must become null, got {parsed['fatigue_score']!r}"
        )
        assert parsed["readiness_score"] is None
        assert parsed["coaching_flags"] == []

    def test_v2_none_scores_become_null(self):
        """FIX 3: None fatigue/readiness scores serialize as null."""
        result = metrics_row_to_json(
            athlete_id="ath-v2c",
            metric_date=1_700_000_000_000,
            acute_load_val=0.0,
            chronic_load_28d_val=0.0,
            chronic_load_42d_val=0.0,
            acr_val=None,
            deload_flag=0,
            fatigue_score_val=None,
            readiness_score_val=None,
            coaching_flags_val=[],
        )
        parsed = json.loads(result)
        assert parsed["fatigue_score"] is None
        assert parsed["readiness_score"] is None


# ---------------------------------------------------------------------------
# compute_fatigue_score (metrics-v2, Scenarios 1-4)
# ---------------------------------------------------------------------------


class TestComputeFatigueScore:
    """Scenario 1-4: fatigue_score = clamp(acute / max(chronic42,1), 0, 5) * 20."""

    def test_scenario_1_standard_computation(self):
        # Sc 1: acute=100, chronic_42d=50 -> ratio=2.0, clamped=2.0, *20=40.0
        assert compute_fatigue_score(100.0, 50.0) == pytest.approx(40.0)

    def test_scenario_2_chronic_zero_returns_none(self):
        # Sc 2: chronic_load_42d==0 -> NULL guard -> None
        assert compute_fatigue_score(100.0, 0.0) is None

    def test_scenario_3_saturation_ceiling(self):
        # Sc 3: acute=600, chronic_42d=100 -> ratio=6 > clamp ceiling of 5 -> 5*20=100.0
        assert compute_fatigue_score(600.0, 100.0) == pytest.approx(100.0)

    def test_scenario_4_zero_acute_load(self):
        # Sc 4: acute=0, chronic_42d=80 -> ratio=0 -> 0*20=0.0
        assert compute_fatigue_score(0.0, 80.0) == pytest.approx(0.0)

    def test_triangulation_moderate_ratio(self):
        # Triangulation: ratio=1.0 (100/100) -> 1.0*20=20.0
        assert compute_fatigue_score(100.0, 100.0) == pytest.approx(20.0)

    def test_triangulation_max_clamp_boundary(self):
        # ratio exactly 5 (at ceiling): 250/50 -> 5*20=100.0 (boundary, not exceeded)
        assert compute_fatigue_score(250.0, 50.0) == pytest.approx(100.0)

    # FIX 2: NaN guard — NaN inputs must return None, not propagate NaN.
    def test_chronic_load_42d_nan_returns_none(self):
        # NaN chronic_load_42d must return None (not nan) — FIX 2.
        result = compute_fatigue_score(100.0, float("nan"))
        assert result is None, f"Expected None for NaN chronic_load_42d, got {result!r}"

    def test_acute_load_nan_returns_none(self):
        # NaN acute_load must return None (not nan) — FIX 2.
        result = compute_fatigue_score(float("nan"), 50.0)
        assert result is None, f"Expected None for NaN acute_load, got {result!r}"


# ---------------------------------------------------------------------------
# compute_readiness_score (metrics-v2, Scenarios 5-11)
# ---------------------------------------------------------------------------


class TestComputeReadinessScore:
    """Scenario 5-11: readiness_score piecewise ACR zones, capped at 80.0."""

    def test_scenario_5_optimal_acr_1_0(self):
        # Sc 5: acr=1.0, chronic_28d=50 -> zone <=1.0: 60+(0.2/0.2)*20=80.0
        assert compute_readiness_score(1.0, 50.0) == pytest.approx(80.0)

    def test_scenario_6_acr_boundary_0_8(self):
        # Sc 6: acr=0.8, chronic_28d=50 -> zone <=0.8: 40+(0.8/0.8)*20=60.0
        assert compute_readiness_score(0.8, 50.0) == pytest.approx(60.0)

    def test_scenario_7_acr_boundary_1_3(self):
        # Sc 7: acr=1.3, chronic_28d=50 -> zone <=1.3: 80-(0.3/0.3)*20=60.0
        assert compute_readiness_score(1.3, 50.0) == pytest.approx(60.0)

    def test_scenario_8_chronic_zero_returns_none(self):
        # Sc 8: chronic_load_28d==0 -> NULL guard -> None
        assert compute_readiness_score(1.0, 0.0) is None

    def test_scenario_8_acr_none_returns_none(self):
        # Sc 8 variant: acr=None -> None
        assert compute_readiness_score(None, 50.0) is None

    def test_scenario_9_readiness_never_exceeds_80(self):
        # Sc 9 invariant: ACR sweep 0..3 step 0.01 — no value > 80.0 AND no value < 0.0
        import math
        for acr_i in range(0, 301):
            acr = acr_i / 100.0
            result = compute_readiness_score(acr, 100.0)
            assert result is not None
            assert result <= 80.0, f"readiness_score exceeded 80.0 at acr={acr}: {result}"
            # FIX 6a: lower bound — readiness score must never be negative
            assert result >= 0.0, f"readiness_score below 0.0 at acr={acr}: {result}"

    def test_scenario_10_high_load_zone_descent(self):
        # Sc 10: acr=2.0, chronic_28d=50
        # zone >1.3: max(0, 60-((2.0-1.3)/0.7)*60)=max(0,60-60)=0.0
        assert compute_readiness_score(2.0, 50.0) == pytest.approx(0.0)

    def test_scenario_11_undertrained_minimum(self):
        # Sc 11: acr=0.0, chronic_28d=50 -> zone <=0.8: 40+(0.0/0.8)*20=40.0
        assert compute_readiness_score(0.0, 50.0) == pytest.approx(40.0)

    def test_triangulation_undertrained_midpoint(self):
        # acr=0.4 -> zone <=0.8: 40+(0.4/0.8)*20=40+10=50.0
        assert compute_readiness_score(0.4, 50.0) == pytest.approx(50.0)

    def test_triangulation_high_load_clamp_to_zero(self):
        # acr=3.0 (way into danger zone): max(0, 60-((3.0-1.3)/0.7)*60)=max(0,60-145.7)<0 -> 0.0
        result = compute_readiness_score(3.0, 50.0)
        assert result == pytest.approx(0.0)

    # FIX 2: NaN guard — NaN inputs must return None, not propagate NaN.
    def test_acr_nan_returns_none(self):
        # NaN acr must return None — FIX 2.
        result = compute_readiness_score(float("nan"), 100.0)
        assert result is None, f"Expected None for NaN acr, got {result!r}"

    def test_chronic_load_28d_nan_returns_none(self):
        # NaN chronic_load_28d must return None — FIX 2.
        result = compute_readiness_score(1.0, float("nan"))
        assert result is None, f"Expected None for NaN chronic_load_28d, got {result!r}"


# ---------------------------------------------------------------------------
# compute_coaching_flags (metrics-v2, Scenarios 12-16)
# ---------------------------------------------------------------------------


class TestComputeCoachingFlags:
    """Scenarios 12-16: coaching_flags derivation from deload_flag and fatigue_score."""

    def test_scenario_12_single_deload_flag(self):
        # Sc 12: deload=1, fatigue=50 -> ["deload"]
        assert compute_coaching_flags(1, 50.0, None) == ["deload"]

    def test_scenario_13_multiple_flags_simultaneously(self):
        # Sc 13: deload=1, fatigue=85 -> ["deload","high_fatigue"]
        assert compute_coaching_flags(1, 85.0, None) == ["deload", "high_fatigue"]

    def test_scenario_14_empty_array_when_no_flags(self):
        # Sc 14: deload=0, fatigue=55 -> []
        assert compute_coaching_flags(0, 55.0, None) == []

    def test_scenario_15_monitor_flag_at_lower_boundary(self):
        # Sc 15: deload=0, fatigue=70.0 -> ["monitor"] (70 <= f < 80)
        assert compute_coaching_flags(0, 70.0, None) == ["monitor"]

    def test_scenario_16_high_fatigue_at_exact_boundary(self):
        # Sc 16: deload=0, fatigue=80.0 -> ["high_fatigue"] NOT ["monitor"]
        result = compute_coaching_flags(0, 80.0, None)
        assert result == ["high_fatigue"], (
            f"fatigue=80.0 must produce ['high_fatigue'] (>= FATIGUE_HIGH=80), "
            f"not ['monitor'] (70<=f<80). Got: {result}"
        )

    def test_undertrained_flag(self):
        # deload=-1 -> "undertrained"
        assert compute_coaching_flags(-1, 55.0, None) == ["undertrained"]

    def test_none_fatigue_no_fatigue_flags(self):
        # fatigue_score=None -> no high_fatigue, no monitor
        assert compute_coaching_flags(0, None, None) == []

    def test_triangulation_deload_and_monitor(self):
        # deload=1, fatigue=75 -> ["deload","monitor"]
        assert compute_coaching_flags(1, 75.0, None) == ["deload", "monitor"]

    def test_triangulation_high_fatigue_above_boundary(self):
        # fatigue=95 -> high_fatigue
        result = compute_coaching_flags(0, 95.0, None)
        assert result == ["high_fatigue"]
        assert "monitor" not in result, "high_fatigue and monitor must be mutually exclusive"


# ---------------------------------------------------------------------------
# DLQ producer-side size guard (sc-6..sc-7) — RED phase
# ---------------------------------------------------------------------------


class TestMetricsDlqSizeGuard:
    """sc-6, sc-7: build_metrics_dlq_envelope must apply identical size guard as build_dlq_envelope."""

    def test_metrics_dlq_envelope_oversized_sets_truncation_marker(self):
        """sc-6: raw value > 524_288 bytes → original_value='', truncated=True, size_bytes=N."""
        oversized = b"x" * 524_289
        env = build_metrics_dlq_envelope(
            original_key="k1",
            original_value=oversized,
            error_type=VALIDATION_FAILURE,
            error_message="test",
            timestamp=1_000_000,
        )
        assert env["original_value"] == ""
        assert env["original_value_truncated"] is True
        assert env["original_value_size_bytes"] == 524_289

    def test_metrics_dlq_envelope_normal_fields_present_and_correct(self):
        """sc-7: normal-size value → truncated=False, correct base64, size_bytes correct."""
        import base64 as _b64
        value = b"x" * 100
        env = build_metrics_dlq_envelope(
            original_key="k1",
            original_value=value,
            error_type=VALIDATION_FAILURE,
            error_message="test",
            timestamp=1_000_000,
        )
        expected_b64 = _b64.b64encode(value).decode("ascii")
        assert env["original_value"] == expected_b64
        assert env["original_value_truncated"] is False
        assert env["original_value_size_bytes"] == 100
