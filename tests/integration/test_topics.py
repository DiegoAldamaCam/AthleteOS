"""Phase 2.3 integration: Kafka topic topology is created correctly.

Verifies (against a real Kafka broker via the testcontainers Redpanda fixture):
  - all 12 topics (6 raw + 3 canonical + 3 DLQ) exist
  - every topic has exactly 8 partitions (LOCKED ADR-4)
  - retention + cleanup.policy configs match the event-contracts spec
    (raw = time-bound delete-only; canonical = compacted + time window; DLQ =
    time-bound delete-only)

Heavy import (confluent_kafka AdminClient) is resolved lazily inside the test
so collection does not require confluent-kafka to be installed. The Redpanda
fixture in conftest skips these tests when Docker is unavailable.
"""

from __future__ import annotations

import pytest

from bootstrap._topology import (
    CANONICAL_TOPICS,
    DLQ_TOPICS,
    PARTITION_COUNT,
    RAW_TOPICS,
    all_topics,
    topic_config,
)
from bootstrap.create_topics import create_all

pytestmark = pytest.mark.integration


def _admin_for(bootstrap_servers: str):
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap_servers})


def test_all_topics_created_with_eight_partitions(redpanda_endpoints):
    """Every topology topic exists with exactly PARTITION_COUNT partitions."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    create_all(bootstrap)  # idempotent: skips existing

    admin = _admin_for(bootstrap)
    metadata = admin.list_topics(timeout=30)

    for topic in all_topics():
        assert topic in metadata.topics, f"missing topic: {topic}"
        parts = metadata.topics[topic].partitions
        assert len(parts) == PARTITION_COUNT, (
            f"{topic} has {len(parts)} partitions, expected {PARTITION_COUNT}"
        )


def test_canonical_topics_are_compacted_with_time_window(redpanda_endpoints):
    """Canonical topics: cleanup.policy=compact,delete and retention.ms matches spec."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    admin = _admin_for(bootstrap)

    for topic, spec in CANONICAL_TOPICS.items():
        intended = topic_config(spec["retention_ms"], spec["compacted"])
        actual = _describe_config(admin, topic)
        assert actual.get("cleanup.policy") == intended["cleanup.policy"], (
            f"{topic} cleanup.policy={actual.get('cleanup.policy')}"
        )
        assert actual.get("retention.ms") == intended["retention.ms"], (
            f"{topic} retention.ms={actual.get('retention.ms')}, "
            f"expected {intended['retention.ms']}"
        )


def test_raw_topics_are_time_bound_no_compaction(redpanda_endpoints):
    """Raw topics: cleanup.policy=delete-only and per-source retention (7d/14d)."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    admin = _admin_for(bootstrap)

    for topic, spec in RAW_TOPICS.items():
        intended = topic_config(spec["retention_ms"], spec["compacted"])
        actual = _describe_config(admin, topic)
        assert actual.get("cleanup.policy") == "delete", (
            f"{topic} cleanup.policy={actual.get('cleanup.policy')}, expected delete"
        )
        assert actual.get("retention.ms") == intended["retention.ms"], (
            f"{topic} retention.ms={actual.get('retention.ms')}, "
            f"expected {intended['retention.ms']}"
        )


def test_dlq_topics_are_time_bound(redpanda_endpoints):
    """DLQ topics: delete-only cleanup, 7d retention (diagnostic, not compacted)."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    admin = _admin_for(bootstrap)

    for topic, spec in DLQ_TOPICS.items():
        intended = topic_config(spec["retention_ms"], spec["compacted"])
        actual = _describe_config(admin, topic)
        assert actual.get("cleanup.policy") == "delete", (
            f"{topic} cleanup.policy={actual.get('cleanup.policy')}, expected delete"
        )
        assert actual.get("retention.ms") == intended["retention.ms"], (
            f"{topic} retention.ms={actual.get('retention.ms')}, "
            f"expected {intended['retention.ms']}"
        )


def _describe_config(admin, topic: str) -> dict:
    """Fetch a topic's dynamic configs as a flat name->value dict."""
    from confluent_kafka.admin import ConfigResource

    resource = ConfigResource("topic", topic)
    fs = admin.describe_configs([resource])
    fut = fs[resource]
    cfg = fut.result()  # raises on failure
    return {k: v.value for k, v in cfg.items()}