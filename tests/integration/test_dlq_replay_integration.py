"""Integration tests for dlq-replay using a live RedpandaContainer.

These tests require Docker. They are skipped automatically when Docker is
unavailable. The ``redpanda_endpoints`` fixture (from tests/conftest.py) provides
the live bootstrap servers.

Test scenarios covered:
  - sc-3, sc-4: raw-origin DLQ envelope → replay → correct bytes on raw topic
  - ADR-6, sc-7: canonical-origin LATE_DATA envelope → replay → correct topic
  - sc-1, sc-22: dry-run default → zero produced, dry_run_would_replay > 0
  - sc-16: null-key envelope → replay → consumer receives message with key=None
"""

from __future__ import annotations

import base64
import json
import os
import time

import pytest

from tests.conftest import requires_docker

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _build_envelope(
    original_topic: str,
    original_key: str | None,
    original_value: bytes,
    error_type: str = "VALIDATION_FAILURE",
    timestamp: int = 1719619200000,
) -> bytes:
    payload = {
        "original_topic": original_topic,
        "original_key": original_key,
        "original_value": base64.b64encode(original_value).decode(),
        "error_type": error_type,
        "error_message": "integration test",
        "error_stack": None,
        "timestamp": timestamp,
    }
    return json.dumps(payload).encode()


def _produce_to_kafka(bootstrap_servers: str, topic: str, key: bytes | None, value: bytes) -> None:
    """Produce a single message to Kafka and flush."""
    from confluent_kafka import Producer

    p = Producer({"bootstrap.servers": bootstrap_servers})
    p.produce(topic=topic, key=key, value=value)
    p.flush(timeout=10)


def _consume_from_kafka(
    bootstrap_servers: str,
    topic: str,
    timeout_per_poll: float = 2.0,
    max_polls: int = 15,
) -> list[tuple[bytes | None, bytes]]:
    """Consume all available messages from a topic and return (key, value) pairs."""
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "dlq-replay-integration-test",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    meta = c.list_topics(topic, timeout=10)
    tps = [
        TopicPartition(topic, p_id, 0)
        for p_id in meta.topics[topic].partitions
    ]
    c.assign(tps)

    messages: list[tuple[bytes | None, bytes]] = []
    polls_with_no_msg = 0
    for _ in range(max_polls):
        msg = c.poll(timeout=timeout_per_poll)
        if msg is None:
            polls_with_no_msg += 1
            if polls_with_no_msg >= 3:
                break
            continue
        if msg.error():
            continue
        polls_with_no_msg = 0
        messages.append((msg.key(), msg.value()))
    c.close()
    return messages


def _ensure_topics(bootstrap_servers: str, topics: list[str]) -> None:
    """Create topics if they do not already exist."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = [
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in topics
        if t not in existing
    ]
    if new_topics:
        futures = admin.create_topics(new_topics)
        for t, f in futures.items():
            try:
                f.result(timeout=10)
            except Exception:
                pass  # Topic may already exist; ignore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bootstrap_servers(redpanda_endpoints) -> str:
    """Return the bootstrap servers for the live Redpanda instance."""
    return redpanda_endpoints["bootstrap_servers"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_raw_origin_replay_produces_to_raw_topic(bootstrap_servers):
    """sc-3, sc-4: raw-origin DLQ envelope replayed → decoded bytes on raw.strength."""
    requires_docker()

    dlq_topic = "dlq.canonical.training_event"
    raw_topic = "raw.strength"
    inner_bytes = b'{"event_id":"integ-e1","athlete_id":"A99"}'
    envelope = _build_envelope(
        original_topic=raw_topic,
        original_key="A99",
        original_value=inner_bytes,
    )
    _ensure_topics(bootstrap_servers, [dlq_topic, raw_topic])
    _produce_to_kafka(bootstrap_servers, dlq_topic, b"A99", envelope)

    # Run replay --no-dry-run
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap_servers
    from tools.dlq_replay.config import ReplayConfig, build_parser
    from tools.dlq_replay.consumer import DLQConsumer
    from tools.dlq_replay.producer import DLQProducer
    from tools.dlq_replay.replay import run_replay

    parser = build_parser()
    args = parser.parse_args(["--topic", dlq_topic, "--no-dry-run"])
    config = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers})

    consumer = DLQConsumer(config=config)
    producer = DLQProducer(bootstrap_servers=bootstrap_servers)
    report = run_replay(config, consumer, producer)

    assert report.replayed >= 1, f"Expected at least 1 replayed; got {report}"

    # Verify the raw.strength topic received the decoded bytes
    messages = _consume_from_kafka(bootstrap_servers, raw_topic)
    values = [v for _k, v in messages]
    assert inner_bytes in values, f"Expected {inner_bytes!r} in {values}"


@pytest.mark.integration
def test_canonical_origin_late_data_replay(bootstrap_servers):
    """ADR-6, sc-7: canonical-origin LATE_DATA envelope → replayed to canonical.training_event."""
    requires_docker()

    dlq_topic = "dlq.canonical.training_event"
    canonical_topic = "canonical.training_event"
    inner_bytes = b'{"event_id":"late-e1","athlete_id":"A88"}'
    envelope = _build_envelope(
        original_topic=canonical_topic,
        original_key="A88",
        original_value=inner_bytes,
        error_type="LATE_DATA",
    )
    _ensure_topics(bootstrap_servers, [dlq_topic, canonical_topic])
    _produce_to_kafka(bootstrap_servers, dlq_topic, b"A88", envelope)

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap_servers
    from tools.dlq_replay.config import ReplayConfig, build_parser
    from tools.dlq_replay.consumer import DLQConsumer
    from tools.dlq_replay.producer import DLQProducer
    from tools.dlq_replay.replay import run_replay

    parser = build_parser()
    args = parser.parse_args(["--topic", dlq_topic, "--no-dry-run", "--error-type", "LATE_DATA"])
    config = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers})

    consumer = DLQConsumer(config=config)
    producer = DLQProducer(bootstrap_servers=bootstrap_servers)
    report = run_replay(config, consumer, producer)

    assert report.replayed >= 1

    messages = _consume_from_kafka(bootstrap_servers, canonical_topic)
    values = [v for _k, v in messages]
    assert inner_bytes in values, f"Expected {inner_bytes!r} in {values}"


@pytest.mark.integration
def test_dry_run_default_produces_nothing(bootstrap_servers):
    """sc-1, sc-22: dry-run (default) → zero produced, dry_run_would_replay > 0."""
    requires_docker()

    dlq_topic = "dlq.canonical.wellness_event"
    raw_topic = "raw.wellness"
    inner_bytes = b'{"event_id":"dry-e1"}'
    envelope = _build_envelope(
        original_topic=raw_topic,
        original_key="A77",
        original_value=inner_bytes,
    )
    _ensure_topics(bootstrap_servers, [dlq_topic, raw_topic])
    _produce_to_kafka(bootstrap_servers, dlq_topic, b"A77", envelope)

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap_servers
    from tools.dlq_replay.config import ReplayConfig, build_parser
    from tools.dlq_replay.consumer import DLQConsumer
    from tools.dlq_replay.producer import DLQProducer
    from tools.dlq_replay.replay import run_replay

    parser = build_parser()
    # No --no-dry-run → dry_run=True by default
    args = parser.parse_args(["--topic", dlq_topic])
    config = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers})

    consumer = DLQConsumer(config=config)
    producer = DLQProducer(bootstrap_servers=bootstrap_servers)
    report = run_replay(config, consumer, producer)

    assert report.replayed == 0
    assert report.dry_run_would_replay >= 1


@pytest.mark.integration
def test_null_key_replay_produces_with_none_key(bootstrap_servers):
    """sc-16: null-key envelope → replay → consumer receives message with key=None."""
    requires_docker()

    dlq_topic = "dlq.canonical.planning_block"
    raw_topic = "raw.planning"
    inner_bytes = b'{"event_id":"null-key-e1"}'
    envelope = _build_envelope(
        original_topic=raw_topic,
        original_key=None,  # null key
        original_value=inner_bytes,
    )
    _ensure_topics(bootstrap_servers, [dlq_topic, raw_topic])
    _produce_to_kafka(bootstrap_servers, dlq_topic, None, envelope)

    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap_servers
    from tools.dlq_replay.config import ReplayConfig, build_parser
    from tools.dlq_replay.consumer import DLQConsumer
    from tools.dlq_replay.producer import DLQProducer
    from tools.dlq_replay.replay import run_replay

    parser = build_parser()
    args = parser.parse_args(["--topic", dlq_topic, "--no-dry-run"])
    config = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers})

    consumer = DLQConsumer(config=config)
    producer = DLQProducer(bootstrap_servers=bootstrap_servers)
    report = run_replay(config, consumer, producer)

    assert report.replayed >= 1

    messages = _consume_from_kafka(bootstrap_servers, raw_topic)
    # At least one message should have a None key and our inner_bytes as value
    null_key_messages = [(k, v) for k, v in messages if k is None and v == inner_bytes]
    assert len(null_key_messages) >= 1, (
        f"Expected a message with null key and value={inner_bytes!r}; got {messages}"
    )
