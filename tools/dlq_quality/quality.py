"""DLQ quality scan orchestration.

scan() performs a single consumer pass over configured DLQ topics and feeds
up to 3 in-memory aggregators (error-type, age, triage). Returns a QualityResult.

Read-only guarantee (ADR-7): this module NEVER imports or constructs
confluent_kafka.Producer. All Kafka I/O is via DLQConsumer.iter_messages().
"""

from __future__ import annotations

import time

from tools.dlq_replay.consumer import DLQConsumer
from tools.dlq_replay.envelope import CorruptEnvelope, decode

from tools.dlq_quality.config import QualityConfig
from tools.dlq_quality.reports import (
    AgeAgg,
    ErrorTypeAgg,
    QualityResult,
    TriageAgg,
)


def scan(config: QualityConfig) -> QualityResult:
    """Perform a single-pass read-only scan of DLQ topics.

    Snapshots now_ms ONCE before the consumer loop (ADR-3) and feeds all
    enabled aggregators in a single pass (ADR-8). max_count is enforced here
    in the loop — DLQConsumer.iter_messages() does not enforce it (ADR-2).

    W1 carry-forward: valid_topics intentionally empty — DLQConsumer never
    reads it; regression guard: integration test.

    Args:
        config: Resolved QualityConfig with bootstrap_servers, topics, reports, etc.

    Returns:
        QualityResult populated by the enabled aggregators.
    """
    # Snapshot now_ms ONCE before constructing the consumer (ADR-3 / sc-6).
    # All age calculations use this single reference time.
    now_ms = int(time.time() * 1000)

    replay_config = config.build_replay_config()

    # Construct DLQConsumer exactly once (sc-19).
    consumer = DLQConsumer(replay_config)

    error_agg = ErrorTypeAgg()
    age_agg = AgeAgg()
    triage_agg = TriageAgg()

    reports = config.reports
    use_error_type = "error-type" in reports
    use_age = "age" in reports
    use_triage = "triage" in reports

    scanned = 0
    corrupt = 0

    for raw_bytes, topic, _partition, _offset in consumer.iter_messages():
        # max_count guard (ADR-2): consumer.iter_messages() does NOT enforce this.
        if config.max_count is not None and scanned >= config.max_count:
            break

        scanned += 1

        try:
            envelope = decode(raw_bytes)
        except CorruptEnvelope:
            corrupt += 1
            continue

        if use_error_type:
            error_agg.add(topic, envelope.error_type)

        if use_age:
            age_agg.add(topic, envelope.timestamp, now_ms)

        if use_triage:
            triage_agg.add(topic, envelope, config.sample_count)

    return QualityResult(
        error_type=dict(error_agg.counts) if use_error_type else {},
        age=dict(age_agg.counts) if use_age else {},
        age_extremes=dict(age_agg.extremes) if use_age else {},
        triage_fix=dict(triage_agg.fix_counts) if use_triage else {},
        triage_origin=dict(triage_agg.origin_counts) if use_triage else {},
        samples=dict(triage_agg.samples) if use_triage else {},
        corrupt=corrupt,
        scanned=scanned,
    )
