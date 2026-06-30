"""Replay orchestration — consume DLQ messages, apply filters and gates, produce.

Data flow:
    consumer.iter_messages()
        → envelope.decode()         (CorruptEnvelope → skipped_unrecoverable)
        → error_type filter         (non-matching → silently skipped)
        → size gate                 (> max_size_bytes → skipped_oversized + ERROR)
        → valid-topic gate          (null/unknown → skipped_unrecoverable + ERROR)
        → null-key gate             (null key → WARNING, still replay)
        → producer.produce()        (dry_run? → dry_run_would_replay : replayed)
    → report
"""

from __future__ import annotations

import logging

from tools.dlq_replay.config import ReplayConfig
from tools.dlq_replay.consumer import DLQConsumer
from tools.dlq_replay.envelope import CorruptEnvelope, decode
from tools.dlq_replay.producer import DLQProducer
from tools.dlq_replay.report import ReplayReport

logger = logging.getLogger(__name__)


def run_replay(
    config: ReplayConfig,
    consumer: DLQConsumer,
    producer: DLQProducer,
) -> ReplayReport:
    """Execute the DLQ replay loop.

    Iterates all messages from ``consumer.iter_messages()``, decodes each DLQ
    envelope, applies filters (error_type, max_count) and safety gates (size,
    valid_topic, corrupt), then either dry-runs or produces to the original topic.

    Args:
        config: Resolved ReplayConfig with filter/gate parameters.
        consumer: A DLQConsumer (or compatible mock) providing iter_messages().
        producer: A DLQProducer (or compatible mock) providing produce().

    Returns:
        A populated ReplayReport with all counters and per-topic breakdown.
    """
    report = ReplayReport()
    processed_count = 0

    for raw_bytes, dlq_topic, dlq_partition, dlq_offset in consumer.iter_messages():
        # sc-12: stop after max_count messages processed.
        if config.max_count is not None and processed_count >= config.max_count:
            break

        # Ensure a per-topic counter dict exists for this DLQ source topic.
        # sc-2, sc-21: per_topic breakdown keyed by the DLQ topic the message
        # came from so callers can see how many messages per source queue were
        # replayed, oversized, or unrecoverable.
        topic_counters = report.per_topic.setdefault(
            dlq_topic,
            {
                "replayed": 0,
                "dry_run_would_replay": 0,
                "skipped_oversized": 0,
                "skipped_unrecoverable": 0,
            },
        )

        # sc-19, sc-20: decode envelope — corrupt → UNRECOVERABLE skip.
        try:
            envelope = decode(raw_bytes)
        except CorruptEnvelope as exc:
            logger.error(
                "UNRECOVERABLE: corrupt DLQ envelope at %s[%d]@%d — %s",
                dlq_topic, dlq_partition, dlq_offset, exc,
            )
            report.skipped_unrecoverable += 1
            topic_counters["skipped_unrecoverable"] += 1
            processed_count += 1
            continue

        # sc-7: --error-type filter — non-matching messages are silently skipped
        # (not counted as unrecoverable per spec).
        if config.error_type is not None and envelope.error_type != config.error_type:
            continue

        # ADR-3/ADR-4: truncation skip — BEFORE the size gate (sc-11).
        # Fires when the producer set original_value_truncated=True (oversized raw bytes
        # dropped at source) or when original_value decoded to b"" (covers legacy
        # truncated envelopes lacking the flag; also genuine empty-payload envelopes
        # which are equally unreplayable — both cases are counted as unrecoverable).
        if envelope.original_value_truncated or envelope.original_value == b"":
            logger.warning(
                "TRUNCATED_PRODUCER: skipping producer-truncated DLQ envelope at %s[%d]@%d "
                "(original_value dropped at source; cannot replay — also covers legitimate "
                "empty-payload envelopes which are unreplayable)",
                dlq_topic, dlq_partition, dlq_offset,
            )
            report.skipped_unrecoverable += 1
            topic_counters["skipped_unrecoverable"] += 1
            processed_count += 1
            continue

        # sc-14, sc-15: oversized gate — size of decoded bytes.
        value_size = len(envelope.original_value)
        if value_size > config.max_size_bytes:
            logger.error(
                "OVERSIZED: DLQ message at %s[%d]@%d skipped — decoded value %d bytes "
                "exceeds max_size_bytes=%d",
                dlq_topic, dlq_partition, dlq_offset, value_size, config.max_size_bytes,
            )
            report.skipped_oversized += 1
            topic_counters["skipped_oversized"] += 1
            processed_count += 1
            continue

        # sc-17, sc-18 (ADR-6): valid-topic gate.
        # original_topic must be in valid_topics (RAW ∪ CANONICAL, not DLQ).
        original_topic = envelope.original_topic
        if not original_topic or original_topic not in config.valid_topics:
            logger.error(
                "UNRECOVERABLE: DLQ message at %s[%d]@%d — original_topic=%r is not "
                "a valid replay target (not in RAW ∪ CANONICAL topology or is null/empty)",
                dlq_topic, dlq_partition, dlq_offset, original_topic,
            )
            report.skipped_unrecoverable += 1
            topic_counters["skipped_unrecoverable"] += 1
            processed_count += 1
            continue

        # sc-16: null original_key — warn but still replay.
        original_key = envelope.original_key
        if original_key is None:
            logger.warning(
                "NULL KEY: DLQ message at %s[%d]@%d — replaying with null original_key "
                "to topic=%r (message will be assigned to a random partition)",
                dlq_topic, dlq_partition, dlq_offset, original_topic,
            )

        # Produce or dry-run; accumulate per-topic counter to match the global one.
        pre_replayed = report.replayed
        pre_dry_run = report.dry_run_would_replay
        producer.produce(
            topic=original_topic,
            key=original_key,
            value=envelope.original_value,
            report=report,
            dry_run=config.dry_run,
        )
        # Mirror whichever global counter the producer incremented.
        if report.replayed > pre_replayed:
            topic_counters["replayed"] += report.replayed - pre_replayed
        if report.dry_run_would_replay > pre_dry_run:
            topic_counters["dry_run_would_replay"] += (
                report.dry_run_would_replay - pre_dry_run
            )
        processed_count += 1

    producer.flush()
    return report
