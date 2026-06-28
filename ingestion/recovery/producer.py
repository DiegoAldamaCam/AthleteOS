"""Raw-envelope builder + Kafka producer for the recovery connector (PR-R1).

``build_envelope`` is a PURE function: it turns a typed ``RecoveryRecord``
into the raw JSON envelope dict for the ``raw.recovery`` topic.

event_time formula (mirrors wellness connector W1-5, spec-locked):
    event_time = int(datetime.fromisoformat(record.date + "T00:00:00+00:00").timestamp() * 1000)

The payload carries the 5 Apple Health source fields verbatim. No ``event_type``
in the payload — ``event_type = "RECOVERY_SNAPSHOT"`` is hardcoded by the
canonicalize transform (ADR-R1), not by the producer.

Mirrors ``ingestion/wellness/producer.py`` symbol-for-symbol.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Callable, Protocol

from ingestion.recovery.parser import RecoveryRecord

DEFAULT_TOPIC = "raw.recovery"
DEFAULT_SOURCE = "apple_health"


def _default_now() -> int:
    """Return the current UTC time as epoch-ms integer."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _date_to_utc_midnight_epoch_ms(date_str: str) -> int:
    """Convert an ISO date string (YYYY-MM-DD) to UTC midnight epoch-ms.

    Example: '2025-06-01' -> 1748736000000
    Mirrors the wellness connector formula exactly (W1-5).
    """
    return int(datetime.fromisoformat(date_str + "T00:00:00+00:00").timestamp() * 1000)


def build_envelope(
    record: RecoveryRecord,
    source: str = DEFAULT_SOURCE,
    now: Callable[[], int] | None = None,
    uuid_factory: Callable[[], object] | None = None,
) -> dict:
    """Build the raw recovery envelope dict.

    - event_id:    UUID v4 string (injectable for deterministic tests)
    - event_time:  UTC midnight epoch-ms LONG of record.date (mirrors wellness W1-5)
    - ingest_time: epoch-ms integer from ``now`` (wall-clock at ingestion)
    - source:      origin identifier (default ``apple_health``)
    - athlete_id:  partition key (also used as the Kafka message key)
    - payload:     Apple Health source fields verbatim (no event_type)
    """
    now_fn = now or _default_now
    uuid_fn = uuid_factory or (lambda: uuid.uuid4())

    return {
        "event_id": str(uuid_fn()),
        "event_time": _date_to_utc_midnight_epoch_ms(record.date),
        "ingest_time": now_fn(),
        "source": source,
        "athlete_id": record.athlete_id,
        "payload": {
            "athlete_id": record.athlete_id,
            "date": record.date,
            "sleep_hours": record.sleep_hours,
            "resting_hr": record.resting_hr,
            "hrv": record.hrv,
            "steps": record.steps,
            "body_weight_kg": record.body_weight_kg,
        },
    }


class _KafkaProducerLike(Protocol):
    """Structural type matching the confluent-kafka Producer surface we use."""

    def produce(self, topic: str, value: str, key: str) -> None: ...
    def flush(self) -> None: ...


class RecoveryPublisher:
    """Publishes recovery records to the ``raw.recovery`` Kafka topic.

    The underlying confluent-kafka ``Producer`` is created lazily from
    ``bootstrap_servers`` when not injected, so the module stays importable
    without confluent-kafka installed (e.g. ``pytest --collect-only``).
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = DEFAULT_TOPIC,
        kafka_producer: _KafkaProducerLike | None = None,
    ) -> None:
        self.topic = topic
        self._producer = kafka_producer or self._make_producer(bootstrap_servers)

    @staticmethod
    def _make_producer(bootstrap_servers: str) -> _KafkaProducerLike:
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": bootstrap_servers})

    def publish(
        self,
        record: RecoveryRecord,
        source: str = DEFAULT_SOURCE,
        now: Callable[[], int] | None = None,
        uuid_factory: Callable[[], object] | None = None,
    ) -> str:
        """Build the envelope for ``record`` and produce it to Kafka.

        Returns the generated ``event_id`` (idempotency key). The message key is
        ``athlete_id`` (co-partitioning, ADR-4); the value is JSON-encoded envelope.
        """
        envelope = build_envelope(record, source=source, now=now, uuid_factory=uuid_factory)
        self._producer.produce(
            self.topic,
            value=json.dumps(envelope),
            key=record.athlete_id,
        )
        return envelope["event_id"]

    def flush(self) -> None:
        """Flush the underlying producer so queued records are delivered."""
        self._producer.flush()
