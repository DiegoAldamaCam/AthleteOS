"""Unit tests for the PURE metrics-computation logic (PR4, task 5.1-5.4 pure half).

These tests run WITHOUT pyflink and WITHOUT Docker -- they exercise
``jobs.metrics.compute`` which is deliberately pyflink-free so the spec metric
formulas (daily_load, acute/chronic rolling windows, ACR, deload state machine)
have full unit coverage on any interpreter (CPython 3.14 included).

Source of truth: serving-store spec "Metric Formulas".
  daily_load(d)       = sum(session_load on day d)
  acute_load          = sum(daily_load for d in [t-6, t])           -- 7d rolling SUM
  chronic_load_28d    = sum(daily_load for d in [t-27, t]) / 28     -- 28d rolling AVG
  chronic_load_42d    = sum(daily_load for d in [t-41, t]) / 42     -- 42d rolling AVG
  acute_chronic_ratio = acute_load / chronic_load_28d               -- NULL if chronic=0
  deload_flag         = +1 if ACR>1.3 for >=3 consecutive days
                      | -1 if ACR<0.8 for >=3 consecutive days
                      | 0  otherwise
"""

from __future__ import annotations

import math

import pytest

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
    compute_deload_flags,
    is_finite_load,
    sum_loads,
    update_deload_state,
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
    def test_chronic_zero_acr_is_none(self):
        # Baseline: chronic==0 -> ACR is None (spec: NULL if chronic=0).
        assert acute_chronic_ratio(100.0, 0.0) is None

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
