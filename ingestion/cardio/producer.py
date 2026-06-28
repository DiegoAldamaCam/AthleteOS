"""Raw-envelope builder + Kafka producer for the cardio connector (PR-C1).

``build_envelope`` is a PURE function: it turns a typed ``CardioRecord``
into the raw JSON envelope dict for the ``raw.cardio`` topic.

``event_time`` is the epoch-ms LONG (integer) derived from the ISO-8601
``timestamp`` field of the record (W1-5 compliant).

The formula:
    event_time = int(datetime.fromisoformat(record.timestamp).replace(tzinfo=timezone.utc).timestamp() * 1000)

If the timestamp already carries timezone info, it is used as-is.
If it is naive (no tzinfo), it is assumed to be UTC.

Mirrors ``ingestion/wellness/producer.py`` symbol-for-symbol except for the
field names (cardio vs wellness) and the event_time computation (ISO datetime
vs ISO date).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Callable, Protocol

from ingestion.cardio.parser import CardioRecord

DEFAULT_TOPIC = "raw.cardio"
DEFAULT_SOURCE = "synthetic_cardio"


def _default_now() -> int:
    """Return the current UTC time as epoch-ms integer."""
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _to_epoch_ms(timestamp_str: str) -> int:
    """Convert an ISO-8601 datetime string to epoch-ms.

    Naive timestamps are interpreted as UTC.

    Examples:
        '2025-06-01T10:00:00'    -> 1748772000000
        '2025-01-01T00:00:00Z'   -> 1735689600000
    """
    dt = datetime.fromisoformat(timestamp_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def build_envelope(
    record: CardioRecord,
    source: str = DEFAULT_SOURCE,
    now: Callable[[], int] | None = None,
    uuid_factory: Callable[[], object] | None = None,
) -> dict:
    """Build the raw cardio envelope dict.

    - event_id:    UUID v4 string (injectable for deterministic tests)
    - event_time:  epoch-ms LONG from record.timestamp (ISO datetime -> UTC epoch-ms)
    - ingest_time: epoch-ms integer from ``now`` (wall-clock at ingestion)
    - source:      origin identifier (default ``synthetic_cardio``)
    - athlete_id:  partition key (also used as the Kafka message key)
    - payload:     source fields verbatim
    """
    now_fn = now or _default_now
    uuid_fn = uuid_factory or (lambda: uuid.uuid4())

    return {
        "event_id": str(uuid_fn()),
        "event_time": _to_epoch_ms(record.timestamp),
        "ingest_time": now_fn(),
        "source": source,
        "athlete_id": record.athlete_id,
        "payload": {
            "athlete_id": record.athlete_id,
            "activity_type": record.activity_type,
            "duration_sec": record.duration_sec,
            "timestamp": record.timestamp,
            "distance_km": record.distance_km,
            "avg_hr": record.avg_hr,
            "tss": record.tss,
        },
    }


class _KafkaProducerLike(Protocol):
    """Structural type matching the confluent-kafka Producer surface we use."""

    def produce(self, topic: str, value: str, key: str) -> None: ...
    def flush(self) -> None: ...


class CardioPublisher:
    """Publishes cardio records to the ``raw.cardio`` Kafka topic.

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
        record: CardioRecord,
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
