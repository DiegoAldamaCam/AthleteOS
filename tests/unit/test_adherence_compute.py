"""Unit tests for jobs.adherence_metrics.compute — adherence score (ADH-C1..C7).

Verifies all 7 spec scenarios with concrete formula values (±0.001 tolerance).
These are pure unit tests — no DB, no I/O, no pyflink.
"""

from __future__ import annotations

import pytest

from jobs.adherence_metrics.compute import (
    compute_adherence_score,
    parse_weekly_volume_targets,
)


# ---------------------------------------------------------------------------
# compute_adherence_score — all 7 ADH-C scenarios
# ---------------------------------------------------------------------------


class TestComputeAdherenceScore:
    """compute_adherence_score formula correctness (ADH-C1..C7)."""

    def test_adh_c1_happy_path_partial_compliance_both_sub_scores(self):
        """ADH-C1: actual=3/4 sessions, 150/200 volume → 0.75."""
        result = compute_adherence_score(
            actual_sessions=3,
            planned_sessions=4,
            actual_volume=150.0,
            target_volume=200.0,
        )
        assert result is not None, "ADH-C1: result must not be None"
        assert abs(result - 0.75) < 0.001, (
            f"ADH-C1: 0.5×(3/4) + 0.5×(150/200) = 0.75, got {result}"
        )

    def test_adh_c2_sessions_and_volume_capped_at_1_0(self):
        """ADH-C2: over-performed on both → capped at 1.0."""
        result = compute_adherence_score(
            actual_sessions=6,
            planned_sessions=4,
            actual_volume=300.0,
            target_volume=200.0,
        )
        assert result is not None, "ADH-C2: result must not be None"
        assert abs(result - 1.0) < 0.001, (
            f"ADH-C2: min(6/4,1)=1.0 + min(300/200,1)=1.0 → 1.0, got {result}"
        )

    def test_adh_c3_e1_zero_actual_returns_0_0(self):
        """ADH-C3: E1 — zero actual training, plan exists → 0.0."""
        result = compute_adherence_score(
            actual_sessions=0,
            planned_sessions=4,
            actual_volume=0.0,
            target_volume=200.0,
        )
        assert result is not None, "ADH-C3: result must be 0.0, not None"
        assert abs(result - 0.0) < 0.001, (
            f"ADH-C3: 0 sessions + 0 volume → 0.0, got {result}"
        )

    def test_adh_c4_e3_target_volume_none_uses_sessions_only(self):
        """ADH-C4: E3 — target_volume=None → sessions-only formula (weight 1.0)."""
        result = compute_adherence_score(
            actual_sessions=3,
            planned_sessions=4,
            actual_volume=150.0,
            target_volume=None,
        )
        assert result is not None, "ADH-C4: sessions-only result must not be None"
        assert abs(result - 0.75) < 0.001, (
            f"ADH-C4: min(3/4,1.0)=0.75 (sessions-only), got {result}"
        )

    def test_adh_c6_e5_planned_sessions_zero_returns_none(self):
        """ADH-C6: E5 — planned_sessions=0 → None (div-by-zero guard)."""
        result = compute_adherence_score(
            actual_sessions=3,
            planned_sessions=0,
            actual_volume=100.0,
            target_volume=200.0,
        )
        assert result is None, (
            f"ADH-C6: planned_sessions=0 must return None, got {result!r}"
        )

    def test_adh_c6_planned_sessions_negative_returns_none(self):
        """E5 guard applies to negative planned_sessions too."""
        result = compute_adherence_score(
            actual_sessions=3,
            planned_sessions=-1,
            actual_volume=100.0,
            target_volume=200.0,
        )
        assert result is None, (
            f"E5: planned_sessions=-1 must return None, got {result!r}"
        )

    def test_adh_c7_mixed_sessions_100_volume_50(self):
        """ADH-C7: sessions=100%, volume=50% → 0.75."""
        result = compute_adherence_score(
            actual_sessions=4,
            planned_sessions=4,
            actual_volume=100.0,
            target_volume=200.0,
        )
        assert result is not None, "ADH-C7: result must not be None"
        assert abs(result - 0.75) < 0.001, (
            f"ADH-C7: 0.5×1.0 + 0.5×0.5 = 0.75, got {result}"
        )

    def test_no_zerodivision_on_planned_sessions_zero(self):
        """ADH-C6: ZeroDivisionError must NOT be raised when planned_sessions=0."""
        try:
            result = compute_adherence_score(3, 0, 100.0, 200.0)
        except ZeroDivisionError:
            pytest.fail("ZeroDivisionError raised for planned_sessions=0 — must return None")

    def test_result_range_is_0_to_1(self):
        """Output range must be [0.0, 1.0] for any valid input."""
        result = compute_adherence_score(3, 4, 150.0, 200.0)
        assert result is not None
        assert 0.0 <= result <= 1.0, f"Result {result} out of [0, 1] range"


# ---------------------------------------------------------------------------
# parse_weekly_volume_targets — ADH-C4 (malformed), ADH-C5 (dict sum)
# ---------------------------------------------------------------------------


class TestParseWeeklyVolumeTargets:
    """parse_weekly_volume_targets — ADH-C4 / ADH-C5 scenarios."""

    def test_adh_c4_malformed_json_returns_none(self):
        """ADH-C4: 'not-json' → None (no exception raised)."""
        result = parse_weekly_volume_targets("not-json")
        assert result is None, (
            f"ADH-C4: malformed JSON must return None, got {result!r}"
        )

    def test_adh_c4_no_exception_on_bad_json(self):
        """ADH-C4: malformed JSON must NOT raise any exception."""
        try:
            parse_weekly_volume_targets("not-json")
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"parse_weekly_volume_targets raised {type(exc).__name__}: {exc}")

    def test_adh_c5_dict_sums_numeric_values(self):
        """ADH-C5: '{"strength": 3, "cardio": 2}' → 5.0."""
        result = parse_weekly_volume_targets('{"strength": 3, "cardio": 2}')
        assert result is not None, "ADH-C5: valid JSON dict must not return None"
        assert abs(result - 5.0) < 0.001, (
            f"ADH-C5: sum(3, 2) = 5.0, got {result!r}"
        )

    def test_empty_json_string_returns_none(self):
        """Empty string is malformed JSON → None."""
        result = parse_weekly_volume_targets("")
        assert result is None

    def test_json_array_returns_none(self):
        """JSON arrays are not a dict — return None."""
        result = parse_weekly_volume_targets("[1, 2, 3]")
        assert result is None, (
            f"JSON array is not a dict — expected None, got {result!r}"
        )

    def test_null_string_returns_none(self):
        """'null' is valid JSON but not a dict → None."""
        result = parse_weekly_volume_targets("null")
        assert result is None

    def test_single_key_dict_returns_correct_sum(self):
        """Triangulation: single-key dict returns that value as float."""
        result = parse_weekly_volume_targets('{"strength": 200}')
        assert result is not None
        assert abs(result - 200.0) < 0.001


# ---------------------------------------------------------------------------
# Integration: parse + compute together (ADH-C4 round-trip)
# ---------------------------------------------------------------------------


class TestParseAndComputeIntegration:
    """Round-trip test: parse_weekly_volume_targets → compute_adherence_score."""

    def test_adh_c4_bad_json_then_sessions_only(self):
        """ADH-C4: malformed JSON returns None → sessions-only formula = 0.75."""
        target_volume = parse_weekly_volume_targets("not-json")
        assert target_volume is None

        result = compute_adherence_score(
            actual_sessions=3,
            planned_sessions=4,
            actual_volume=150.0,
            target_volume=target_volume,
        )
        assert result is not None
        assert abs(result - 0.75) < 0.001, (
            f"ADH-C4 round-trip: sessions-only 0.75, got {result}"
        )

    def test_adh_c5_valid_dict_sum_then_full_formula(self):
        """ADH-C5: dict sum 5.0 used as target_volume for 1-week block."""
        weekly_target = parse_weekly_volume_targets('{"strength": 3, "cardio": 2}')
        assert weekly_target == 5.0

        target_volume = weekly_target * 1  # 1 block_week
        result = compute_adherence_score(
            actual_sessions=3,
            planned_sessions=4,
            actual_volume=4.5,
            target_volume=target_volume,  # 5.0
        )
        # 0.5 * min(3/4,1) + 0.5 * min(4.5/5.0,1) = 0.5*0.75 + 0.5*0.9 = 0.825
        assert result is not None
        assert abs(result - 0.825) < 0.001, (
            f"Expected 0.825, got {result}"
        )
