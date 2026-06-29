"""Unit tests for tools.dlq_replay.consumer (strict TDD — RED phase first).

All tests use mock Consumer objects — no Docker or real Kafka required.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from tools.dlq_replay.config import ReplayConfig
from tools.dlq_replay.consumer import DLQConsumer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**kwargs) -> ReplayConfig:
    """Build a minimal ReplayConfig for consumer tests."""
    defaults = {
        "bootstrap_servers": "localhost:9092",
        "topics": ["dlq.canonical.training_event"],
        "valid_topics": frozenset(["raw.strength", "canonical.training_event"]),
        "dry_run": True,
    }
    defaults.update(kwargs)
    return ReplayConfig(**defaults)


def _make_mock_message(value: bytes, offset: int = 0, partition: int = 0, topic: str = "dlq.canonical.training_event"):
    """Build a mock confluent_kafka.Message."""
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = value
    msg.offset.return_value = offset
    msg.partition.return_value = partition
    msg.topic.return_value = topic
    return msg


@dataclass
class _FakeTopicPartition:
    """Minimal TopicPartition stand-in for mocking."""
    topic: str
    partition: int
    offset: int = 0


def _make_mock_consumer_cls(
    partitions: list[_FakeTopicPartition],
    messages_by_partition: dict[int, list[bytes]],
    watermarks: dict[int, tuple[int, int]],
    offsets_for_times_result: dict[int, int] | None = None,
):
    """Build a mock confluent_kafka.Consumer class for a single topic.

    Args:
        partitions: List of partitions for the topic.
        messages_by_partition: partition → list of message value bytes.
        watermarks: partition → (lo, hi) tuple.
        offsets_for_times_result: partition → offset returned by offsets_for_times
            (-1 signals "no message at/after timestamp").
    """
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance

    # Topic metadata
    topic_name = partitions[0].topic if partitions else "dlq.canonical.training_event"
    partition_metas = {}
    for p in partitions:
        pm = MagicMock()
        pm.id = p.partition
        partition_metas[p.partition] = pm

    topic_meta = MagicMock()
    topic_meta.partitions = partition_metas
    topics_meta = MagicMock()
    topics_meta.topics = {topic_name: topic_meta}
    mock_instance.list_topics.return_value = topics_meta

    # Watermark offsets per partition
    def get_watermark_offsets(tp, timeout=10, cached=False):
        return watermarks[tp.partition]

    mock_instance.get_watermark_offsets.side_effect = get_watermark_offsets

    # Poll simulation: returns messages round-robin per partition, then None
    poll_sequence: list = []
    for msgs in messages_by_partition.values():
        for i, value in enumerate(msgs):
            partition_id = list(messages_by_partition.keys())[
                list(messages_by_partition.values()).index(msgs)
            ]
            offset_idx = i
            lo, hi = watermarks[partition_id]
            msg = _make_mock_message(
                value=value,
                offset=lo + offset_idx,
                partition=partition_id,
                topic=topic_name,
            )
            poll_sequence.append(msg)
    poll_sequence.append(None)  # terminal None to trigger HWM check path

    mock_instance.poll.side_effect = poll_sequence

    # offsets_for_times mock
    if offsets_for_times_result is not None:
        def _offsets_for_times(tps, timeout=10):
            result = []
            for tp in tps:
                fake = _FakeTopicPartition(
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offsets_for_times_result.get(tp.partition, 0),
                )
                result.append(fake)
            return result
        mock_instance.offsets_for_times.side_effect = _offsets_for_times

    return mock_cls, mock_instance


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# sc-13: HWM snapshot — iter_messages stops at HWM, consumer.close() called
def test_consumer_stops_at_hwm_and_calls_close():
    """iter_messages yields all messages up to HWM then stops; close() is called."""
    p0 = _FakeTopicPartition("dlq.canonical.training_event", 0, offset=0)
    # 2 messages: offsets 0,1; HWM=2 (hi=2 means last offset+1=2)
    mock_cls, mock_instance = _make_mock_consumer_cls(
        partitions=[p0],
        messages_by_partition={0: [b'msg0', b'msg1']},
        watermarks={0: (0, 2)},
    )
    # Patch poll to return 2 messages then None
    msg0 = _make_mock_message(b'msg0', offset=0)
    msg1 = _make_mock_message(b'msg1', offset=1)
    mock_instance.poll.side_effect = [msg0, msg1, None]

    with patch("tools.dlq_replay.consumer.ConfluentConsumer", mock_cls):
        cfg = _make_config()
        consumer = DLQConsumer(config=cfg)
        messages = list(consumer.iter_messages())

    # Should yield both messages as (raw_bytes, topic, partition, offset) tuples
    assert len(messages) == 2
    assert messages[0][0] == b'msg0'  # raw bytes
    assert messages[1][0] == b'msg1'
    mock_instance.close.assert_called_once()


# sc-13: empty partition (lo==hi) → immediately marked done, no poll
def test_empty_partition_no_poll():
    """When lo == hi (empty partition), no polling occurs."""
    p0 = _FakeTopicPartition("dlq.canonical.training_event", 0, offset=0)
    mock_cls, mock_instance = _make_mock_consumer_cls(
        partitions=[p0],
        messages_by_partition={0: []},
        watermarks={0: (5, 5)},  # lo == hi → empty
    )
    mock_instance.poll.side_effect = []  # should never be called

    with patch("tools.dlq_replay.consumer.ConfluentConsumer", mock_cls):
        cfg = _make_config()
        consumer = DLQConsumer(config=cfg)
        messages = list(consumer.iter_messages())

    assert messages == []
    mock_instance.poll.assert_not_called()
    mock_instance.close.assert_called_once()


# sc-10: --from-offset → consumer seeks to the given offset
def test_from_offset_seek():
    """When from_offset is set, assign is called with that offset."""
    p0 = _FakeTopicPartition("dlq.canonical.training_event", 0, offset=0)
    mock_cls, mock_instance = _make_mock_consumer_cls(
        partitions=[p0],
        messages_by_partition={0: [b'msg2']},
        watermarks={0: (0, 3)},
    )
    # Only 1 message at offset 2
    msg2 = _make_mock_message(b'msg2', offset=2)
    mock_instance.poll.side_effect = [msg2, None]

    with patch("tools.dlq_replay.consumer.ConfluentConsumer", mock_cls):
        cfg = _make_config(from_offset=2)
        consumer = DLQConsumer(config=cfg)
        messages = list(consumer.iter_messages())

    assert messages[0][0] == b'msg2'  # raw bytes from the tuple
    # Verify assign was called (seek to offset=2 embedded in tp)
    mock_instance.assign.assert_called_once()
    assigned_tps = mock_instance.assign.call_args[0][0]
    assert assigned_tps[0].offset == 2


# sc-8, sc-9: --from-timestamp → offsets_for_times called; result assigned
def test_from_timestamp_calls_offsets_for_times():
    """When from_timestamp_ms is set, offsets_for_times is called and result is assigned."""
    p0 = _FakeTopicPartition("dlq.canonical.training_event", 0, offset=0)
    ts_ms = 1719619200000
    mock_cls, mock_instance = _make_mock_consumer_cls(
        partitions=[p0],
        messages_by_partition={0: [b'msg3']},
        watermarks={0: (0, 1)},
        offsets_for_times_result={0: 0},  # offset 0 corresponds to timestamp
    )
    msg3 = _make_mock_message(b'msg3', offset=0)
    mock_instance.poll.side_effect = [msg3, None]

    with patch("tools.dlq_replay.consumer.ConfluentConsumer", mock_cls):
        cfg = _make_config(from_timestamp_ms=ts_ms)
        consumer = DLQConsumer(config=cfg)
        messages = list(consumer.iter_messages())

    mock_instance.offsets_for_times.assert_called_once()
    assert messages[0][0] == b'msg3'  # raw bytes from the tuple


# ADR-7: offsets_for_times returns tp.offset=-1 → partition marked done immediately, never assigned
def test_adr7_offset_minus_one_partition_not_assigned():
    """When offsets_for_times returns offset=-1 (timestamp past HWM), the partition
    is marked done immediately and never passed to assign()."""
    p0 = _FakeTopicPartition("dlq.canonical.training_event", 0, offset=0)
    mock_cls, mock_instance = _make_mock_consumer_cls(
        partitions=[p0],
        messages_by_partition={0: []},
        watermarks={0: (0, 5)},
        offsets_for_times_result={0: -1},  # sentinel: timestamp past HWM
    )
    mock_instance.poll.side_effect = []  # should never be called

    with patch("tools.dlq_replay.consumer.ConfluentConsumer", mock_cls):
        cfg = _make_config(from_timestamp_ms=9999999999999)
        consumer = DLQConsumer(config=cfg)
        messages = list(consumer.iter_messages())

    assert messages == []
    # assign must NOT be called with this partition (it was marked done)
    mock_instance.assign.assert_not_called()
    mock_instance.close.assert_called_once()


# ADR-7 triangulate: one partition offset=-1, one valid → only valid is assigned
def test_adr7_mixed_partitions_only_valid_assigned():
    """With two partitions, the -1 one is skipped and the valid one is assigned."""
    p0 = _FakeTopicPartition("dlq.canonical.training_event", 0, offset=0)
    p1 = _FakeTopicPartition("dlq.canonical.training_event", 1, offset=0)

    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance

    topic_name = "dlq.canonical.training_event"
    pm0, pm1 = MagicMock(), MagicMock()
    pm0.id = 0
    pm1.id = 1
    topic_meta = MagicMock()
    topic_meta.partitions = {0: pm0, 1: pm1}
    topics_meta = MagicMock()
    topics_meta.topics = {topic_name: topic_meta}
    mock_instance.list_topics.return_value = topics_meta

    def get_wm(tp, timeout=10, cached=False):
        return (0, 2) if tp.partition == 1 else (0, 2)

    mock_instance.get_watermark_offsets.side_effect = get_wm

    def offsets_for_times_side(tps, timeout=10):
        result = []
        for tp in tps:
            offset = -1 if tp.partition == 0 else 0
            result.append(_FakeTopicPartition(topic=tp.topic, partition=tp.partition, offset=offset))
        return result

    mock_instance.offsets_for_times.side_effect = offsets_for_times_side

    msg_p1 = _make_mock_message(b'p1_msg', offset=0, partition=1, topic=topic_name)
    mock_instance.poll.side_effect = [msg_p1, None]

    with patch("tools.dlq_replay.consumer.ConfluentConsumer", mock_cls):
        cfg = _make_config(from_timestamp_ms=1000)
        consumer = DLQConsumer(config=cfg)
        messages = list(consumer.iter_messages())

    assert messages[0][0] == b'p1_msg'  # raw bytes from the tuple
    # assign called once with only partition 1
    mock_instance.assign.assert_called_once()
    assigned_tps = mock_instance.assign.call_args[0][0]
    assigned_partitions = [tp.partition for tp in assigned_tps]
    assert 0 not in assigned_partitions
    assert 1 in assigned_partitions
