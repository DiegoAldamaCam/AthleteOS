"""Unit tests for DLQ status-mapping logic in api/kafka_admin.py.

Tests pure logic functions extracted from kafka_admin.py WITHOUT touching
a real Kafka broker — mock AdminClient only.

Spec source: obs #65 Domain B (DLQ Depth contract).
Decision source: obs #68 (lazy singleton, degrade-to-200, depth contract).

Functions under test:
  - compute_dlq_status(depth) → "ok" | "warning"
  - build_degraded_response(topics) → degraded envelope dict

TDD RED phase: these tests fail until kafka_admin.py is created.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# compute_dlq_status — pure function mapping depth → status string
# ---------------------------------------------------------------------------


class TestComputeDlqStatus:
    """Status mapping: depth 0 → 'ok'; depth > 0 → 'warning'."""

    def test_depth_zero_returns_ok(self):
        """depth=0 means the DLQ is empty — status must be 'ok'."""
        from api.kafka_admin import compute_dlq_status

        assert compute_dlq_status(0) == "ok"

    def test_depth_one_returns_warning(self):
        """depth=1 means one unprocessed message — status must be 'warning'."""
        from api.kafka_admin import compute_dlq_status

        assert compute_dlq_status(1) == "warning"

    def test_depth_large_returns_warning(self):
        """Large depth (e.g. 1000) still returns 'warning', not a new status."""
        from api.kafka_admin import compute_dlq_status

        assert compute_dlq_status(1000) == "warning"

    def test_negative_depth_treated_as_zero_or_ok(self):
        """Negative depth (shouldn't happen, but defensive) → clamp to 0 → 'ok'.

        The spec says 'non-negative integer'; we guard against edge cases
        where end_offset < committed_offset (consumer ahead of producer).
        """
        from api.kafka_admin import compute_dlq_status

        # depth clamped to 0 → "ok"
        assert compute_dlq_status(-1) == "ok"


# ---------------------------------------------------------------------------
# build_degraded_response — builds the broker-unreachable envelope
# ---------------------------------------------------------------------------


class TestBuildDegradedResponse:
    """Degraded envelope: broker_reachable=false, all depths null, status='unavailable'."""

    def test_returns_broker_unreachable_false(self):
        """broker_reachable key must be False in the degraded envelope."""
        from api.kafka_admin import build_degraded_response

        topics = ["dlq.canonical.training_event", "dlq.canonical.wellness_event"]
        result = build_degraded_response(topics)
        assert result["broker_reachable"] is False

    def test_each_topic_has_null_depth(self):
        """All topic entries must have depth=None (null in JSON) when degraded."""
        from api.kafka_admin import build_degraded_response

        topics = [
            "dlq.canonical.training_event",
            "dlq.canonical.wellness_event",
            "dlq.canonical.planning_block",
        ]
        result = build_degraded_response(topics)
        for entry in result["topics"]:
            assert entry["depth"] is None, f"Expected null depth, got {entry['depth']}"

    def test_each_topic_has_unavailable_status(self):
        """All topic entries must have status='unavailable' when degraded."""
        from api.kafka_admin import build_degraded_response

        topics = [
            "dlq.canonical.training_event",
            "dlq.canonical.wellness_event",
            "dlq.canonical.planning_block",
        ]
        result = build_degraded_response(topics)
        for entry in result["topics"]:
            assert entry["status"] == "unavailable", f"Expected 'unavailable', got {entry['status']}"

    def test_topic_names_preserved_in_output(self):
        """The topic name in each entry must match the input list order."""
        from api.kafka_admin import build_degraded_response

        topics = [
            "dlq.canonical.training_event",
            "dlq.canonical.wellness_event",
            "dlq.canonical.planning_block",
        ]
        result = build_degraded_response(topics)
        output_names = [e["topic"] for e in result["topics"]]
        assert output_names == topics

    def test_three_topics_produces_three_entries(self):
        """One entry per topic — no more, no less."""
        from api.kafka_admin import build_degraded_response

        topics = [
            "dlq.canonical.training_event",
            "dlq.canonical.wellness_event",
            "dlq.canonical.planning_block",
        ]
        result = build_degraded_response(topics)
        assert len(result["topics"]) == 3
