"""Phase 3.3 integration: Strong CSV drop -> events land in raw.strength.

End-to-end verification of the strength ingestion connector against a real Kafka
broker (testcontainers Redpanda). Drops a Strong CSV file, runs the connector,
then consumes from ``raw.strength`` and asserts the published JSON envelopes
match the event-contracts spec "Raw Topic JSON Shape":

  - message key == athlete_id (co-partitioning, ADR-4)
  - event_id is a valid UUID v4
  - event_time / ingest_time are ISO-8601 strings
  - source == "strong_csv"
  - payload carries the source fields verbatim
    {workout_id, exercise_id, set_number, reps, weight_kg, rpe, rir, timestamp}
  - session_load is NOT present (it is derived at canonicalization, PR3)

Docker-gated: the ``redpanda`` fixture (tests/conftest.py) skips this test when
the Docker daemon is unreachable, so collection never requires a broker and the
test degrades cleanly in CI/sandboxed runners without faking a pass.
"""

from __future__ import annotations

import csv
import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from bootstrap.create_topics import create_all
from ingestion.strength.producer import StrengthPublisher
from ingestion.strength.watcher import process_csv_file

pytestmark = pytest.mark.integration

_HEADER = (
    "athlete_id,workout_id,exercise_id,set_number,reps,weight_kg,rpe,rir,timestamp\n"
)
_ROWS = (
    "athlete-123,w-001,bench-press,1,8,100,8.5,2,2024-01-15T10:30:00\n"
    "athlete-123,w-001,bench-press,2,6,105,9,1,2024-01-15T10:35:00\n"
)


def _consume_n(
    bootstrap_servers: str,
    topic: str,
    n: int,
    timeout: float = 30.0,
    key_filter: str | None = None,
):
    """Consume exactly ``n`` messages from ``topic`` or raise on timeout.

    Uses a unique consumer group and ``auto.offset.reset=earliest`` so the test
    reads from the start of the topic regardless of prior committed offsets.

    ``raw.strength`` is a SHARED, fixed-name topic on the session-scoped broker.
    Without filtering, this helper would take the first ``n`` records on the
    topic — which may be records produced by an unrelated test that ran earlier,
    causing cross-test contamination flakiness. When ``key_filter`` is provided,
    records whose key does not match are drained and ignored, so the helper
    returns only the ``n`` records this test produced.
    """
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"test-strength-ingestion-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    try:
        deadline = datetime.now().timestamp() + timeout
        while len(messages) < n and datetime.now().timestamp() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue  # ignore transient broker errors during startup
            if key_filter is not None:
                raw_key = msg.key()
                key = (
                    raw_key.decode("utf-8")
                    if isinstance(raw_key, (bytes, bytearray))
                    else raw_key
                )
                if key != key_filter:
                    continue  # drain unrelated records (cross-test isolation)
            messages.append(msg)
    finally:
        consumer.close()
    assert len(messages) == n, (
        f"expected {n} messages on {topic}"
        + (f" with key {key_filter!r}" if key_filter is not None else "")
        + f", got {len(messages)} within {timeout}s"
    )
    return messages


def test_csv_drop_lands_envelopes_in_raw_strength(redpanda_endpoints, tmp_path):
    """A CSV file processed by the connector produces spec-shaped envelopes on
    raw.strength keyed by athlete_id."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    create_all(bootstrap)  # idempotent: ensures raw.strength exists with 8 partitions

    csv_path = tmp_path / "workout.csv"
    csv_path.write_text(_HEADER + _ROWS, encoding="utf-8")

    publisher = StrengthPublisher(bootstrap_servers=bootstrap)
    summary = process_csv_file(csv_path, publisher)

    assert summary.published == 2
    assert summary.skipped == 0

    messages = _consume_n(bootstrap, "raw.strength", n=2, key_filter="athlete-123")

    envelopes = []
    for msg in messages:
        # co-partitioning: the message key is the athlete_id string
        assert msg.key() is not None
        key = msg.key().decode("utf-8") if isinstance(msg.key(), (bytes, bytearray)) else msg.key()
        assert key == "athlete-123"

        envelope = json.loads(msg.value().decode("utf-8"))
        envelopes.append(envelope)

        # --- raw envelope shape (event-contracts: Raw Topic JSON Shape) ---
        assert uuid.UUID(envelope["event_id"]).version == 4
        assert envelope["source"] == "strong_csv"
        assert envelope["athlete_id"] == "athlete-123"
        # event_time / ingest_time are ISO-8601 strings in the raw layer
        datetime.fromisoformat(envelope["event_time"])
        datetime.fromisoformat(envelope["ingest_time"])

        # payload carries the source fields verbatim
        payload = envelope["payload"]
        assert set(payload.keys()) == {
            "workout_id",
            "exercise_id",
            "set_number",
            "reps",
            "weight_kg",
            "rpe",
            "rir",
            "timestamp",
        }
        # session_load is a canonicalization-layer field -> NOT in the raw envelope
        assert "session_load" not in envelope
        assert "session_load" not in payload

    # the two sets are distinct (set_number 1 and 2), proving both rows landed
    set_numbers = sorted(e["payload"]["set_number"] for e in envelopes)
    assert set_numbers == [1, 2]
    # event_time is the normalized ISO-8601 form of the CSV timestamp
    assert envelopes[0]["payload"]["timestamp"] == "2024-01-15T10:30:00"
    assert envelopes[0]["event_time"] == "2024-01-15T10:30:00"
