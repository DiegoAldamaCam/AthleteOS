"""Unit tests for tools.dlq_replay.replay (strict TDD — RED phase first).

Uses mock consumer (iter_messages) and mock producer; no Docker required.
"""

from __future__ import annotations

import base64
import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from tools.dlq_replay.config import ReplayConfig
from tools.dlq_replay.replay import run_replay
from tools.dlq_replay.report import ReplayReport


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**kwargs) -> ReplayConfig:
    defaults = {
        "bootstrap_servers": "localhost:9092",
        "topics": ["dlq.canonical.training_event"],
        "valid_topics": frozenset(
            ["raw.strength", "raw.cardio", "raw.nutrition", "raw.wellness",
             "raw.recovery", "raw.planning",
             "canonical.training_event", "canonical.wellness_event", "canonical.planning_block"]
        ),
        "dry_run": True,
        "max_size_bytes": 1_048_576,
    }
    defaults.update(kwargs)
    return ReplayConfig(**defaults)


def _make_envelope(
    original_topic: str | None = "raw.strength",
    original_key: str | None = "A1",
    original_value: bytes = b'{"event_id":"e1"}',
    error_type: str = "VALIDATION_FAILURE",
    timestamp: int | None = 1719619200000,
) -> bytes:
    """Build a DLQ envelope as raw bytes (as Kafka consumer would return)."""
    payload = {
        "original_topic": original_topic,
        "original_key": original_key,
        "original_value": base64.b64encode(original_value).decode(),
        "error_type": error_type,
        "error_message": "test error",
        "error_stack": None,
        "timestamp": timestamp,
    }
    return json.dumps(payload).encode()


def _mock_consumer(messages: list[bytes]) -> MagicMock:
    """Build a mock DLQConsumer whose iter_messages yields (bytes, topic, partition, offset) tuples."""
    c = MagicMock()
    # Wrap each raw bytes value in the (raw_bytes, topic, partition, offset) tuple
    # as DLQConsumer.iter_messages() now yields (per ADR-4 logging requirement).
    tuples = [
        (msg, "dlq.canonical.training_event", 0, i)
        for i, msg in enumerate(messages)
    ]
    c.iter_messages.return_value = iter(tuples)
    return c


def _mock_producer() -> MagicMock:
    """Build a mock DLQProducer."""
    p = MagicMock()
    p.produce = MagicMock()
    p.flush = MagicMock()
    return p


# ---------------------------------------------------------------------------
# sc-3: valid raw-origin message → replayed to raw topic
# ---------------------------------------------------------------------------

def test_valid_raw_origin_message_replayed():
    cfg = _make_config(dry_run=False)
    raw = _make_envelope(original_topic="raw.strength", original_key="A1")
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_called_once()
    call_kwargs = producer.produce.call_args[1]
    assert call_kwargs["topic"] == "raw.strength"
    assert call_kwargs["key"] == "A1"
    assert call_kwargs["value"] == b'{"event_id":"e1"}'


# sc-17 (ADR-6): canonical-origin message → replayed (canonical.training_event is valid)
def test_canonical_origin_message_is_valid_and_replayed():
    cfg = _make_config(dry_run=False)
    raw = _make_envelope(original_topic="canonical.training_event", original_key="A1")
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_called_once()
    call_kwargs = producer.produce.call_args[1]
    assert call_kwargs["topic"] == "canonical.training_event"


# sc-17: unknown original_topic → skipped_unrecoverable
def test_unknown_original_topic_is_unrecoverable():
    cfg = _make_config(dry_run=False)
    raw = _make_envelope(original_topic="raw.unknown_topic")
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_unrecoverable == 1
    assert report.replayed == 0


# sc-18: null original_topic → skipped_unrecoverable
def test_null_original_topic_is_unrecoverable():
    cfg = _make_config(dry_run=False)
    raw = _make_envelope(original_topic=None)
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_unrecoverable == 1


# ADR-6 loop prevention: original_topic is a DLQ topic → skipped_unrecoverable
def test_dlq_original_topic_is_unrecoverable():
    cfg = _make_config(dry_run=False)
    raw = _make_envelope(original_topic="dlq.canonical.training_event")
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_unrecoverable == 1


# sc-14: value > max_size_bytes → skipped_oversized + ERROR log
def test_oversized_message_is_skipped(caplog):
    cfg = _make_config(dry_run=False, max_size_bytes=10)
    value = b"x" * 11
    raw = _make_envelope(original_value=value)
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    with caplog.at_level(logging.ERROR):
        report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_oversized == 1
    assert any("oversized" in r.message.lower() or "size" in r.message.lower() for r in caplog.records)


# sc-15: custom --max-size-bytes=256, value=512 → skipped_oversized
def test_custom_max_size_bytes_respected():
    cfg = _make_config(dry_run=False, max_size_bytes=256)
    value = b"y" * 512
    raw = _make_envelope(original_value=value)
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_oversized == 1


# sc-16: null original_key → WARNING logged, produced with key=None, counted as replayed
def test_null_key_replayed_with_warning(caplog):
    cfg = _make_config(dry_run=False)
    raw = _make_envelope(original_key=None)
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    with caplog.at_level(logging.WARNING):
        report = run_replay(cfg, consumer, producer)

    producer.produce.assert_called_once()
    call_kwargs = producer.produce.call_args[1]
    assert call_kwargs["key"] is None
    # Warning must have been logged
    assert any(r.levelno == logging.WARNING for r in caplog.records)


# sc-7: --error-type VALIDATION_FAILURE → only matching messages replayed
def test_error_type_filter_replays_matching_only():
    cfg = _make_config(dry_run=False, error_type="VALIDATION_FAILURE")
    raw_match = _make_envelope(error_type="VALIDATION_FAILURE")
    raw_no_match = _make_envelope(error_type="TRANSFORM_ERROR")
    consumer = _mock_consumer([raw_match, raw_no_match])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    assert producer.produce.call_count == 1


# sc-7: --error-type LATE_DATA filter
def test_error_type_late_data_filter():
    cfg = _make_config(dry_run=False, error_type="LATE_DATA")
    raw_late = _make_envelope(error_type="LATE_DATA")
    raw_validation = _make_envelope(error_type="VALIDATION_FAILURE")
    consumer = _mock_consumer([raw_late, raw_validation])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    assert producer.produce.call_count == 1  # only LATE_DATA replayed


# sc-7: TRANSFORM_ERROR message not counted as unrecoverable when error_type filter active
def test_error_type_non_matching_not_counted_unrecoverable():
    cfg = _make_config(dry_run=False, error_type="VALIDATION_FAILURE")
    raw_transform = _make_envelope(error_type="TRANSFORM_ERROR")
    consumer = _mock_consumer([raw_transform])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    assert report.skipped_unrecoverable == 0
    assert report.replayed == 0


# sc-12: --max-count 3 → stops after 3 messages
def test_max_count_stops_processing():
    cfg = _make_config(dry_run=False, max_count=3)
    messages = [_make_envelope() for _ in range(10)]
    consumer = _mock_consumer(messages)
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    # Total processed = replayed (produce calls)
    assert producer.produce.call_count == 3


# sc-19: CorruptEnvelope from decode → skipped_unrecoverable
def test_corrupt_envelope_non_json_is_unrecoverable():
    cfg = _make_config(dry_run=False)
    consumer = _mock_consumer([b"not json at all"])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_unrecoverable == 1


# sc-20: missing field → skipped_unrecoverable
def test_corrupt_envelope_missing_field_is_unrecoverable():
    cfg = _make_config(dry_run=False)
    bad = json.dumps({"original_key": "A1", "error_type": "VALIDATION_FAILURE"}).encode()
    consumer = _mock_consumer([bad])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_not_called()
    assert report.skipped_unrecoverable == 1


# sc-1: dry_run=True → producer.produce called with dry_run=True
# The DLQProducer.produce() is responsible for the no-op behaviour when dry_run=True.
# run_replay delegates the dry_run decision to the producer as a parameter.
def test_dry_run_passes_dry_run_true_to_producer():
    cfg = _make_config(dry_run=True)
    raw = _make_envelope()
    consumer = _mock_consumer([raw])
    producer = _mock_producer()

    report = run_replay(cfg, consumer, producer)

    producer.produce.assert_called_once()
    call_kwargs = producer.produce.call_args[1]
    assert call_kwargs["dry_run"] is True


# ---------------------------------------------------------------------------
# CRIT-V1 (behavior): run_replay must populate report.per_topic
# These tests drive real messages through run_replay (public entry point)
# and assert per_topic is populated by the function — NOT set manually in
# the test body.  The existing tests all set per_topic by hand on a bare
# ReplayReport; these tests never touch report.per_topic directly.
# ---------------------------------------------------------------------------

def test_run_replay_populates_per_topic_replayed():
    """run_replay must accumulate per-topic replayed counts in report.per_topic.

    Drives two valid messages from the same DLQ topic through run_replay and
    asserts that report.per_topic is non-empty and carries the correct count.
    This is a BEHAVIOR test — the test never sets per_topic itself.
    """
    cfg = _make_config(dry_run=False)

    # Two valid envelopes from the same DLQ topic
    env1 = _make_envelope(original_topic="raw.strength", original_key="K1")
    env2 = _make_envelope(original_topic="raw.cardio", original_key="K2")

    # Consumer returns tuples with a concrete dlq_topic so we can assert on it
    consumer = MagicMock()
    consumer.iter_messages.return_value = iter([
        (env1, "dlq.raw.strength", 0, 0),
        (env2, "dlq.raw.cardio", 0, 1),
    ])

    # Real producer mock that actually increments report.replayed
    producer = MagicMock()
    def _real_produce(topic, key, value, report, dry_run):
        report.replayed += 1
    producer.produce.side_effect = _real_produce
    producer.flush = MagicMock()

    report = run_replay(cfg, consumer, producer)

    # per_topic must be populated by run_replay — never set manually in this test
    assert report.per_topic != {}, (
        "run_replay did not populate per_topic — CRIT-V1: the feature is a ghost"
    )
    # Each DLQ topic must appear with at least the 'replayed' counter
    assert "dlq.raw.strength" in report.per_topic, (
        "dlq.raw.strength not found in per_topic"
    )
    assert "dlq.raw.cardio" in report.per_topic, (
        "dlq.raw.cardio not found in per_topic"
    )
    assert report.per_topic["dlq.raw.strength"].get("replayed", 0) == 1
    assert report.per_topic["dlq.raw.cardio"].get("replayed", 0) == 1


def test_run_replay_per_topic_tracks_unrecoverable():
    """run_replay must record per-topic skipped_unrecoverable counts.

    A corrupt envelope and an oversized message both come from the same DLQ
    topic; per_topic must show those counters for that topic.
    This test never touches report.per_topic directly.
    """
    cfg = _make_config(dry_run=False, max_size_bytes=5)

    corrupt = b"not json"
    oversized = _make_envelope(original_value=b"x" * 100)  # > max_size_bytes=5

    consumer = MagicMock()
    consumer.iter_messages.return_value = iter([
        (corrupt,   "dlq.raw.strength", 0, 0),
        (oversized, "dlq.raw.strength", 0, 1),
    ])

    producer = MagicMock()
    producer.flush = MagicMock()

    report = run_replay(cfg, consumer, producer)

    assert report.per_topic != {}, "per_topic must be non-empty after run_replay"
    topic_counters = report.per_topic.get("dlq.raw.strength", {})
    assert topic_counters.get("skipped_unrecoverable", 0) >= 1, (
        "per_topic[dlq.raw.strength][skipped_unrecoverable] must be >= 1"
    )
    assert topic_counters.get("skipped_oversized", 0) >= 1, (
        "per_topic[dlq.raw.strength][skipped_oversized] must be >= 1"
    )
