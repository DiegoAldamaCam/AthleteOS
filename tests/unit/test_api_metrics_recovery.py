"""Unit tests for recovery_score in API models and SQL query (W3-10, W3-11).

Verifies:
  W3-10: MetricRow includes recovery_score field; when present, model serializes it correctly.
  W3-11: When recovery_score is None (NULL), model serializes it as null (no crash, no omission).
  Also verifies the metrics router SELECT query includes recovery_score (W3-10).

These are pure unit tests (no real DB, no Docker required).
"""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from api.models import MetricRow
from api.routers.metrics import _SQL_METRICS_RANGE


# ---------------------------------------------------------------------------
# W3-10 / W3-11: MetricRow includes recovery_score field
# ---------------------------------------------------------------------------


class TestMetricRowRecoveryScore:
    """MetricRow Pydantic model must include recovery_score: Optional[float]."""

    def test_w3_10_metric_row_accepts_recovery_score(self):
        """W3-10: MetricRow can be constructed with recovery_score=73.29."""
        row = MetricRow(
            athlete_id="A1",
            metric_date=date(2025, 3, 1),
            acute_load=100.0,
            chronic_load_28d=90.0,
            chronic_load_42d=85.0,
            acute_chronic_ratio=1.1,
            deload_flag=0,
            fatigue_score=20.0,
            readiness_score=65.0,
            coaching_flags=None,
            recovery_score=73.29,
        )
        assert row.recovery_score is not None
        assert abs(row.recovery_score - 73.29) < 0.01, (
            f"Expected recovery_score≈73.29, got {row.recovery_score!r}"
        )

    def test_w3_11_metric_row_accepts_null_recovery_score(self):
        """W3-11: MetricRow can be constructed with recovery_score=None (SQL NULL)."""
        row = MetricRow(
            athlete_id="A1",
            metric_date=date(2025, 3, 1),
            acute_load=100.0,
            chronic_load_28d=90.0,
            chronic_load_42d=85.0,
            acute_chronic_ratio=1.1,
            deload_flag=0,
            recovery_score=None,
        )
        assert row.recovery_score is None, (
            f"recovery_score=None must be preserved as None, got {row.recovery_score!r}"
        )

    def test_metric_row_default_recovery_score_is_none(self):
        """recovery_score defaults to None when not provided (additive field)."""
        row = MetricRow(
            athlete_id="A1",
            metric_date=date(2025, 3, 1),
            acute_load=None,
            chronic_load_28d=None,
            chronic_load_42d=None,
            acute_chronic_ratio=None,
            deload_flag=None,
        )
        # Default must be None — not missing, not 0
        assert row.recovery_score is None

    def test_metric_row_serializes_recovery_score_as_float(self):
        """W3-10 JSON shape: recovery_score serializes as a float in JSON output."""
        row = MetricRow(
            athlete_id="A1",
            metric_date=date(2025, 3, 1),
            acute_load=None,
            chronic_load_28d=None,
            chronic_load_42d=None,
            acute_chronic_ratio=None,
            deload_flag=None,
            recovery_score=73.29,
        )
        data = row.model_dump()
        assert "recovery_score" in data, "recovery_score must be present in serialized output"
        assert data["recovery_score"] is not None
        assert abs(data["recovery_score"] - 73.29) < 0.01

    def test_metric_row_serializes_null_recovery_score_as_none(self):
        """W3-11 JSON shape: recovery_score=None serializes as null (not omitted)."""
        row = MetricRow(
            athlete_id="A1",
            metric_date=date(2025, 3, 1),
            acute_load=None,
            chronic_load_28d=None,
            chronic_load_42d=None,
            acute_chronic_ratio=None,
            deload_flag=None,
            recovery_score=None,
        )
        data = row.model_dump()
        assert "recovery_score" in data, "recovery_score field must be present in output"
        assert data["recovery_score"] is None, (
            "recovery_score=None must serialize as null (not absent)"
        )


# ---------------------------------------------------------------------------
# W3-10 / W3-11: Verify the SQL query includes recovery_score
# ---------------------------------------------------------------------------


class TestMetricsRouterSqlIncludesRecoveryScore:
    """The metrics router SELECT must include recovery_score (W3-10 router contract)."""

    def test_w3_10_sql_metrics_range_selects_recovery_score(self):
        """W3-10: _SQL_METRICS_RANGE must SELECT recovery_score from athlete_metrics."""
        assert "recovery_score" in _SQL_METRICS_RANGE, (
            "W3-10: _SQL_METRICS_RANGE must include 'recovery_score' in the SELECT list. "
            f"Current SQL:\n{_SQL_METRICS_RANGE}"
        )

    def test_w3_10_sql_does_not_hardcode_a_null_for_recovery_score(self):
        """recovery_score must be a real column reference, not a NULL placeholder."""
        # Ensure it's not just 'NULL AS recovery_score' (would always return null)
        import re
        # Should not be: NULL AS recovery_score
        null_pattern = re.search(r'NULL\s+AS\s+recovery_score', _SQL_METRICS_RANGE, re.IGNORECASE)
        assert null_pattern is None, (
            "recovery_score must be a real column SELECT, not NULL AS recovery_score"
        )

    def test_w3_11_metric_row_with_null_recovery_score_builds_without_error(self):
        """W3-11: When recovery_score is None, MetricRow construction must not raise."""
        # Simulate what the router does: build MetricRow from a DB row dict with recovery_score=None
        row_dict = {
            "athlete_id": "A1",
            "metric_date": date(2025, 3, 1),
            "acute_load": 100.0,
            "chronic_load_28d": 90.0,
            "chronic_load_42d": 85.0,
            "acute_chronic_ratio": None,
            "deload_flag": None,
            "fatigue_score": None,
            "readiness_score": None,
            "coaching_flags": None,
            "recovery_score": None,
        }
        row = MetricRow(**row_dict)
        assert row.recovery_score is None

    def test_w3_10_metric_row_with_recovery_score_serializes_correctly(self):
        """W3-10: MetricRow with recovery_score=73.29 serializes as expected."""
        row_dict = {
            "athlete_id": "A1",
            "metric_date": date(2025, 3, 1),
            "acute_load": 100.0,
            "chronic_load_28d": 90.0,
            "chronic_load_42d": 85.0,
            "acute_chronic_ratio": 1.1,
            "deload_flag": 0,
            "fatigue_score": None,
            "readiness_score": None,
            "coaching_flags": None,
            "recovery_score": 73.29,
        }
        row = MetricRow(**row_dict)
        serialized = row.model_dump()
        assert "recovery_score" in serialized
        assert abs(serialized["recovery_score"] - 73.29) < 0.01
