"""Raw-envelope builder + Kafka producer for the strength connector (PR2, 3.1).

``build_envelope`` is a PURE function: it turns a typed ``StrengthSetRecord``
into the raw JSON envelope dict defined by the event-contracts spec "Raw Topic
JSON Shape". It does not touch Kafka and takes injectable ``now`` / ``uuid_factory``
callables so tests can assert exact values deterministically.

``StrengthPublisher`` is the thin side-effecting wrapper: it builds the envelope
and produces it to ``raw.strength`` (JSON) with ``athlete_id`` as the message key
(co-partitioning requirement, ADR-4). The underlying confluent-kafka ``Producer``
is injectable so unit tests can verify topic/key/value without a broker.

Per the spec, raw topics use JSON (not Avro) - the locked hybrid serialization
decision (Avro only on canonical topics via Schema Registry). session_load is
NOT set here: it is derived at canonicalization (PR3).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Callable, Protocol

from ingestion.strength.parser import StrengthSetRecord

DEFAULT_TOPIC = "raw.strength"
DEFAULT_SOURCE = "strong_csv"


def _default_now() -> datetime:
    return datetime.now(timezone.utc)


def build_envelope(
    record: StrengthSetRecord,
    source: str = DEFAULT_SOURCE,
    now: Callable[[], datetime] | None = None,
    uuid_factory: Callable[[], object] | None = None,
) -> dict:
    """Build the raw strength envelope dict (spec: Raw Topic JSON Shape).

    - event_id: UUID v4 string (injectable for deterministic tests)
    - event_time: ISO-8601 string derived from the record timestamp (normalized)
    - ingest_time: ISO-8601 string from ``now`` (wall-clock at ingestion)
    - source: origin identifier (default ``strong_csv``)
    - athlete_id: partition key (also used as the Kafka message key)
    - payload: the source fields verbatim
      {workout_id, exercise_id, set_number, reps, weight_kg, rpe, rir, timestamp}

    The payload ``timestamp`` is the ORIGINAL source string; the top-level
    ``event_time`` is the normalized ISO-8601 form derived from it. This keeps
    source fidelity (replay/auditing) while giving Flink a clean event-time.
    """
    now_fn = now or _default_now
    uuid_fn = uuid_factory or (lambda: uuid.uuid4())

    parsed_timestamp = datetime.fromisoformat(record.timestamp)

    return {
        "event_id": str(uuid_fn()),
        "event_time": parsed_timestamp.isoformat(),
        "ingest_time": now_fn().isoformat(),
        "source": source,
        "athlete_id": record.athlete_id,
        "payload": {
            "workout_id": record.workout_id,
            "exercise_id": record.exercise_id,
            "set_number": record.set_number,
            "reps": record.reps,
            "weight_kg": record.weight_kg,
            "rpe": record.rpe,
            "rir": record.rir,
            "timestamp": record.timestamp,
        },
    }


class _KafkaProducerLike(Protocol):
    """Structural type matching the confluent-kafka Producer surface we use."""

    def produce(self, topic: str, value: str, key: str) -> None: ...
    def flush(self) -> None: ...


class StrengthPublisher:
    """Publishes strength set records to the ``raw.strength`` Kafka topic.

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
        record: StrengthSetRecord,
        source: str = DEFAULT_SOURCE,
        now: Callable[[], datetime] | None = None,
        uuid_factory: Callable[[], object] | None = None,
    ) -> str:
        """Build the envelope for ``record`` and produce it to Kafka.

        Returns the generated ``event_id`` (idempotency key). The message key is
        ``athlete_id`` (co-partitioning); the value is the JSON-encoded envelope.
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
