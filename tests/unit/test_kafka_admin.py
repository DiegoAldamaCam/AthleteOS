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


# ---------------------------------------------------------------------------
# OFFSET_INVALID sentinel handling (B9, B10 — Slice B hardening)
# ---------------------------------------------------------------------------


class TestOffsetInvalidGuard:
    """OFFSET_INVALID (-1001) must contribute 0 to depth; broker_reachable stays True.

    Spec: obs #98 Slice B — OFFSET_INVALID Sentinel Handling.
    Design: obs #99 ADR H7 — explicit named guard in _depth_for_topic.
    """

    def _make_mock_admin(self, partitions_with_offsets: dict):
        """Build a minimal mock AdminClient that returns specified offsets.

        partitions_with_offsets: {partition_id: offset_value}
        The topic name is hard-coded to 'dlq.canonical.training_event'.
        """
        from unittest.mock import MagicMock

        topic_name = "dlq.canonical.training_event"

        # Build fake metadata
        metadata = MagicMock()

        part_meta = MagicMock()
        part_meta.partitions = {pid: MagicMock() for pid in partitions_with_offsets}
        metadata.topics = {topic_name: part_meta}

        # Build fake list_offsets futures
        def _list_offsets(specs, request_timeout=5.0):
            futures = {}
            for tp in specs:
                future = MagicMock()
                result = MagicMock()
                result.offset = partitions_with_offsets[tp.partition]
                future.result.return_value = result
                futures[tp] = future
            return futures

        admin = MagicMock()
        admin.list_topics.return_value = metadata
        admin.list_offsets.side_effect = _list_offsets

        return admin, topic_name

    def test_offset_invalid_contributes_zero_to_depth(self):
        """B9: Partition returning OFFSET_INVALID (-1001) → contributes 0, not -1001."""
        from confluent_kafka import OFFSET_INVALID
        from api.kafka_admin import _depth_for_topic

        admin, topic_name = self._make_mock_admin({0: OFFSET_INVALID})
        metadata = admin.list_topics.return_value

        depth = _depth_for_topic(admin, metadata, topic_name, request_timeout=5.0)

        assert depth == 0, (
            f"OFFSET_INVALID partition must contribute 0 to depth, got {depth}"
        )

    def test_offset_invalid_mixed_with_valid_offset(self):
        """B10: One partition has offset 5, another OFFSET_INVALID → total depth == 5."""
        from confluent_kafka import OFFSET_INVALID
        from api.kafka_admin import _depth_for_topic

        admin, topic_name = self._make_mock_admin({0: 5, 1: OFFSET_INVALID})
        metadata = admin.list_topics.return_value

        depth = _depth_for_topic(admin, metadata, topic_name, request_timeout=5.0)

        assert depth == 5, (
            f"Valid offset 5 + OFFSET_INVALID → depth should be 5, got {depth}"
        )
