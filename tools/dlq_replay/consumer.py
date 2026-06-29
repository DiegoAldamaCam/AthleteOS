"""Stateless DLQ consumer — reads partitions up to a snapshotted HWM.

Architecture notes (ADR-1, ADR-5, ADR-7):
- Uses assign() (not subscribe()) — no named consumer group, no offset commits.
- Snapshots the high-water mark at startup; terminates when all partitions
  have been consumed to their snapshotted HWM.
- Explicit guard: if offsets_for_times() returns offset == -1 (confluent_kafka
  sentinel meaning the timestamp is past the partition HWM), that partition is
  marked done immediately and never assigned/seeked (ADR-7).
- iter_messages() yields (raw_bytes, topic, partition, offset) tuples so that
  the replay layer can include DLQ coordinates in every ERROR/WARNING log (ADR-4).
"""

from __future__ import annotations

import logging
from typing import Iterator

from confluent_kafka import Consumer as ConfluentConsumer
from confluent_kafka import TopicPartition

from tools.dlq_replay.config import ReplayConfig

logger = logging.getLogger(__name__)

# Type alias for clarity: raw bytes + DLQ message coordinates.
MessageTuple = tuple[bytes, str, int, int]


class DLQConsumer:
    """Stateless consumer for DLQ replay.

    Reads each DLQ partition from a start position (beginning, offset, or
    timestamp) up to the high-water mark snapshotted at construction time.
    Never polls beyond the HWM and never blocks indefinitely.

    Args:
        config: Resolved ReplayConfig with bootstrap_servers, topics, and
            optional seek parameters.
    """

    def __init__(self, config: ReplayConfig) -> None:
        self._config = config
        self._consumer = ConfluentConsumer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "group.id": "dlq-replay-ephemeral",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )

    def iter_messages(self) -> Iterator[MessageTuple]:
        """Yield (raw_bytes, topic, partition, offset) tuples from all configured DLQ partitions.

        The consumer is assigned (not subscribed) to each partition. It stops
        when each partition reaches the high-water mark that was snapshotted
        at the start of this call. Calls consumer.close() before returning.

        Yields:
            (raw_bytes, topic, partition, offset) tuples — the raw message bytes
            plus the DLQ coordinates needed for ADR-4 logging in the replay layer.
        """
        cfg = self._config
        consumer = self._consumer

        try:
            # Collect all TopicPartition objects across all configured topics.
            all_tps: list[TopicPartition] = []
            for topic in cfg.topics:
                meta = consumer.list_topics(topic, timeout=10)
                topic_meta = meta.topics[topic]
                for part_id in topic_meta.partitions:
                    all_tps.append(TopicPartition(topic, part_id))

            # Snapshot HWM for every partition (lo=low watermark, hi=high watermark).
            # hi is last_committed_offset + 1; an empty partition has lo == hi.
            target_hwm: dict[tuple[str, int], int] = {}
            for tp in all_tps:
                lo, hi = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                target_hwm[(tp.topic, tp.partition)] = hi

            # Determine the starting offset for each partition.
            tps_to_assign: list[TopicPartition] = []
            done: set[tuple[str, int]] = set()

            if cfg.from_timestamp_ms is not None:
                # --from-timestamp: call offsets_for_times; apply ADR-7 guard.
                ts_tps = [
                    TopicPartition(tp.topic, tp.partition, cfg.from_timestamp_ms)
                    for tp in all_tps
                ]
                time_results = consumer.offsets_for_times(ts_tps, timeout=10)
                for result_tp in time_results:
                    key = (result_tp.topic, result_tp.partition)
                    if result_tp.offset == -1:
                        # ADR-7: timestamp past HWM → partition produces zero messages.
                        logger.debug(
                            "Partition %s:%d has no messages at/after timestamp %d "
                            "(offsets_for_times returned -1); marking done.",
                            result_tp.topic,
                            result_tp.partition,
                            cfg.from_timestamp_ms,
                        )
                        done.add(key)
                    else:
                        tps_to_assign.append(
                            TopicPartition(result_tp.topic, result_tp.partition, result_tp.offset)
                        )
            elif cfg.from_offset is not None:
                # --from-offset: seek each partition to the given absolute offset.
                for tp in all_tps:
                    tps_to_assign.append(
                        TopicPartition(tp.topic, tp.partition, cfg.from_offset)
                    )
            else:
                # Default: start from the beginning of each partition.
                for tp in all_tps:
                    lo, _ = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                    tps_to_assign.append(TopicPartition(tp.topic, tp.partition, lo))

            # Mark empty partitions as done before assign (lo == hi → nothing to read).
            non_empty_tps: list[TopicPartition] = []
            for tp in tps_to_assign:
                key = (tp.topic, tp.partition)
                if target_hwm[key] == 0 or (
                    cfg.from_offset is None
                    and cfg.from_timestamp_ms is None
                    and target_hwm[key] <= tp.offset
                ):
                    done.add(key)
                else:
                    non_empty_tps.append(tp)

            # Also mark any partition whose HWM equals its low watermark as done.
            for tp in all_tps:
                key = (tp.topic, tp.partition)
                lo, hi = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                if lo == hi:
                    done.add(key)

            # Assign only non-done partitions.
            if non_empty_tps:
                consumer.assign(non_empty_tps)

            # Poll until all partitions are done.
            while len(done) < len(all_tps):
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    # Transient empty poll (broker latency, rebalance, etc.).
                    # ADR-1: only terminate when ALL partitions have reached their
                    # snapshotted HWM.  If any partition is still below its HWM,
                    # continue polling — do NOT break early and drop messages.
                    if len(done) >= len(all_tps):
                        break
                    continue

                if msg.error():
                    logger.error("Consumer error: %s", msg.error())
                    continue

                key = (msg.topic(), msg.partition())
                value = msg.value()
                yield (value, msg.topic(), msg.partition(), msg.offset())

                # Check if this partition has reached the snapshotted HWM.
                hwm = target_hwm.get(key, 0)
                if msg.offset() + 1 >= hwm:
                    done.add(key)

                # Also stop if all assigned partitions are now done.
                if len(done) >= len(all_tps):
                    break

        finally:
            consumer.close()
