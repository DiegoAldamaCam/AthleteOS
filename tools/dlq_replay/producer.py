"""DLQ replay producer — wraps confluent_kafka.Producer with dry-run support."""

from __future__ import annotations

from confluent_kafka import Producer as ConfluentProducer

from tools.dlq_replay.report import ReplayReport


class DLQProducer:
    """Kafka producer wrapper for the DLQ replay tool.

    Produces decoded DLQ messages back to their original topics, or no-ops
    when dry_run is True (ADR-3).

    Args:
        bootstrap_servers: Kafka bootstrap servers connection string.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = ConfluentProducer({"bootstrap.servers": bootstrap_servers})

    def produce(
        self,
        topic: str,
        key: str | None,
        value: bytes,
        report: ReplayReport,
        dry_run: bool,
    ) -> None:
        """Produce a message to the given topic, or no-op in dry-run mode.

        Args:
            topic: Destination Kafka topic (the original_topic from the envelope).
            key: Kafka message key (original_key from the envelope; may be None).
            value: Decoded message bytes (base64-decoded original_value).
            report: ReplayReport accumulator — increments the appropriate counter.
            dry_run: When True, no message is sent and dry_run_would_replay
                is incremented instead of replayed.
        """
        if dry_run:
            report.dry_run_would_replay += 1
            return

        self._producer.produce(topic=topic, key=key, value=value)
        report.replayed += 1

    def flush(self) -> None:
        """Flush any pending messages to Kafka."""
        self._producer.flush()
