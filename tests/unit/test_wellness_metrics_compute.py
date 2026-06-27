"""Unit tests for jobs.wellness_metrics.compute — strict TDD (W3-1..W3-6).

Boundary-value table from spec #132 (obs #132):

| Case                          | hrv   | hrv_baseline | sleep_hours | perceived_recovery | Expected         |
|-------------------------------|-------|-------------|-------------|-------------------|-----------------|
| Full A — typical              | 65.0  | 60.0        | 7.5         | 8                 | 73.29 ± 0.01    |
| Full A — HRV at baseline      | 60.0  | 60.0        | 8.0         | 10                | 75.00           |
| Full A — HRV below 50%        | 20.0  | 60.0        | 8.0         | 10                | 50.00           |
| Full A — max all              | 120.0 | 60.0        | 8.0         | 10                | 100.00          |
| hrv_baseline=0 guard          | 65.0  | 0.0         | 8.0         | 10                | (no ZeroDivision)|
| Formula B — hrv null, full    | None  | any         | 8.0         | 10                | 100.00          |
| Formula B — hrv null, partial | None  | any         | 4.0         | 5                 | 50.00           |
| All null → None               | None  | any         | None        | None              | None            |
| hrv present, sleep+perceived null | 65.0 | 60.0   | None        | None              | normalized_hrv*50|
"""

from __future__ import annotations

import math

import pytest

from jobs.wellness_metrics.compute import compute_recovery_score


class TestComputeRecoveryScoreFormulaA:
    """Formula A cases: hrv is not None."""

    def test_w3_1_typical_case(self):
        """W3-1: hrv=65, baseline=60, sleep=7.5, perceived=8 → 73.29 ± 0.01.

        Derivation:
          normalized_hrv = clamp((65/60 - 0.5)/1.0, 0, 1) = clamp(0.5833, 0, 1) = 0.5833
          sleep_score    = clamp(7.5/8, 0, 1) = 0.9375
          perceived_score = clamp(8/10, 0, 1) = 0.8
          score = 0.5833*50 + 0.9375*30 + 0.8*20 = 29.167 + 28.125 + 16.0 = 73.29
        """
        result = compute_recovery_score(65.0, 60.0, 7.5, 8)
        assert result is not None
        assert math.isclose(result, 73.29, abs_tol=0.01), (
            f"Expected ≈73.29 ± 0.01, got {result}"
        )

    def test_w3_hrv_at_baseline_gives_75(self):
        """hrv=baseline=60 → normalized_hrv = clamp((60/60 - 0.5), 0, 1) = 0.5.
        score = 0.5*50 + 1.0*30 + 1.0*20 = 25 + 30 + 20 = 75.0
        """
        result = compute_recovery_score(60.0, 60.0, 8.0, 10)
        assert result is not None
        assert math.isclose(result, 75.0, abs_tol=0.01), (
            f"Expected 75.0, got {result}"
        )

    def test_w3_3_hrv_below_50_pct_baseline_clamps_to_floor(self):
        """W3-3: hrv=20 vs baseline=60 → normalized_hrv = clamp((20/60 - 0.5), 0, 1) = 0.
        score = 0*50 + 1.0*30 + 1.0*20 = 50.0
        """
        result = compute_recovery_score(20.0, 60.0, 8.0, 10)
        assert result is not None
        assert math.isclose(result, 50.0, abs_tol=0.01), (
            f"Expected 50.0 (clamped normalized_hrv=0), got {result}"
        )

    def test_w3_6_max_all_inputs_clamps_to_100(self):
        """W3-6: hrv=120 with baseline=60 → normalized_hrv > 1.0, gets clamped.
        Even with all max inputs final score must be ≤ 100.
        """
        result = compute_recovery_score(120.0, 60.0, 8.0, 10)
        assert result is not None
        assert math.isclose(result, 100.0, abs_tol=0.01), (
            f"Expected 100.0 (clamped), got {result}"
        )

    def test_w3_2_hrv_baseline_zero_guard_no_zero_division(self):
        """W3-2: hrv_baseline=0.0 must use max(hrv_baseline, 1.0) — no ZeroDivisionError."""
        # hrv=65, baseline is treated as 1.0 (guard)
        # normalized_hrv = clamp((65/1.0 - 0.5)/1.0, 0, 1) = clamp(64.5, 0, 1) = 1.0
        # score = 1.0*50 + 1.0*30 + 1.0*20 = 100.0
        result = compute_recovery_score(65.0, 0.0, 8.0, 10)
        assert result is not None, "Must not return None on hrv_baseline=0"
        assert 0.0 <= result <= 100.0, f"Result must be in [0, 100], got {result}"

    def test_hrv_present_sleep_and_perceived_null_degrades_gracefully(self):
        """hrv present but sleep_hours=None, perceived_recovery=None.
        sleep_score=0, perceived_score=0 → score = normalized_hrv*50 only.
        hrv=65, baseline=60 → normalized_hrv≈0.5833 → score≈29.17
        """
        result = compute_recovery_score(65.0, 60.0, None, None)
        assert result is not None, "Must not return None when hrv is present"
        # normalized_hrv = clamp((65/60 - 0.5), 0, 1) ≈ 0.5833
        # score = 0.5833*50 + 0*30 + 0*20 ≈ 29.17
        assert math.isclose(result, 29.167, abs_tol=0.05), (
            f"Expected ≈29.17 (hrv-only degrade), got {result}"
        )


class TestComputeRecoveryScoreFormulaB:
    """Formula B cases: hrv is None, at least one of sleep/perceived not None."""

    def test_w3_4_formula_b_both_inputs_present_gives_100(self):
        """W3-4: hrv=None, sleep=8, perceived=10 → 1.0*60 + 1.0*40 = 100.0."""
        result = compute_recovery_score(None, 60.0, 8.0, 10)
        assert result is not None
        assert math.isclose(result, 100.0, abs_tol=0.01), (
            f"Expected 100.0 (Formula B full), got {result}"
        )

    def test_formula_b_partial_gives_50(self):
        """hrv=None, sleep=4, perceived=5 → 0.5*60 + 0.5*40 = 50.0."""
        result = compute_recovery_score(None, 60.0, 4.0, 5)
        assert result is not None
        assert math.isclose(result, 50.0, abs_tol=0.01), (
            f"Expected 50.0 (Formula B partial), got {result}"
        )

    def test_formula_b_sleep_only_no_perceived(self):
        """hrv=None, sleep=8, perceived=None → sleep_score=1.0, perceived=0 → 60+0=60."""
        result = compute_recovery_score(None, 60.0, 8.0, None)
        assert result is not None
        assert math.isclose(result, 60.0, abs_tol=0.01), (
            f"Expected 60.0 (sleep only in B), got {result}"
        )


class TestComputeRecoveryScoreNullGuard:
    """NULL contract: all inputs null → return None."""

    def test_w3_5_all_inputs_null_returns_none(self):
        """W3-5: hrv=None, sleep_hours=None, perceived_recovery=None → None."""
        result = compute_recovery_score(None, 60.0, None, None)
        assert result is None, (
            f"Expected None when all variable inputs are null, got {result!r}"
        )

    def test_all_null_different_baseline_still_none(self):
        """Triangulation: different baseline still yields None when all inputs null."""
        result = compute_recovery_score(None, 0.0, None, None)
        assert result is None, (
            f"Expected None regardless of hrv_baseline when all inputs null, got {result!r}"
        )


class TestComputeRecoveryScoreRangeBounds:
    """Score must always be in [0.0, 100.0]."""

    def test_score_never_below_zero(self):
        """hrv extremely low → normalized_hrv clamps to 0, score still ≥ 0."""
        result = compute_recovery_score(1.0, 60.0, 0.0, 0)
        assert result is not None
        assert result >= 0.0, f"Score must be ≥ 0, got {result}"

    def test_score_never_above_100(self):
        """Max-all inputs — score must be ≤ 100.0."""
        result = compute_recovery_score(999.0, 1.0, 99.0, 10)
        assert result is not None
        assert result <= 100.0, f"Score must be ≤ 100, got {result}"
