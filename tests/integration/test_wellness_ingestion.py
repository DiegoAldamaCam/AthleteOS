"""Phase W1 integration: Wellness CSV drop -> events land in raw.wellness.

End-to-end verification of the wellness ingestion connector against a real Kafka
broker (testcontainers Redpanda). Drops a wellness CSV file, runs the connector,
then consumes from ``raw.wellness`` and asserts the published JSON envelopes
match the event-contracts spec:

  - message key == athlete_id (co-partitioning, ADR-4)
  - event_id is a valid UUID v4 string
  - event_time is a UTC-midnight epoch-ms LONG (NOT an ISO-8601 string)
    DIVERGENCE from strength connector — intentional, spec-locked (W1-5)
  - ingest_time is an integer (epoch-ms)
  - source == "synthetic_wellness"
  - payload carries all source fields verbatim

Docker-gated: the ``redpanda`` fixture (tests/conftest.py) skips this test when
the Docker daemon is unreachable, so collection never requires a broker.

Mirrors tests/integration/test_strength_ingestion.py structure.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from bootstrap.create_topics import create_all
from ingestion.wellness.producer import WellnessPublisher
from ingestion.wellness.watcher import process_csv_file

pytestmark = pytest.mark.integration

_HEADER = (
    "athlete_id,date,hrv,sleep_hours,resting_hr,steps,body_weight_kg,"
    "energy,soreness,mood,stress,perceived_recovery\n"
)
_ROWS = (
    "A1,2025-03-01,65.0,7.5,52,9000,78.5,7,3,8,4,8\n"
    "A2,2025-03-02,70.0,8.0,50,10000,75.0,8,2,9,3,9\n"
)

# 2025-03-01 UTC midnight epoch-ms
_EXPECTED_EVENT_TIME_ROW1 = 1740787200000
# 2025-03-02 UTC midnight epoch-ms
_EXPECTED_EVENT_TIME_ROW2 = 1740873600000


def _consume_n(bootstrap_servers: str, topic: str, n: int, timeout: float = 30.0):
    """Consume exactly ``n`` messages from ``topic`` or raise on timeout."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"test-wellness-ingestion-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    try:
        deadline = time.monotonic() + timeout
        while len(messages) < n and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            messages.append(msg)
    finally:
        consumer.close()
    assert len(messages) == n, (
        f"expected {n} messages on {topic}, got {len(messages)} within {timeout}s"
    )
    return messages


def test_csv_drop_lands_envelopes_in_raw_wellness(redpanda_endpoints, tmp_path):
    """A wellness CSV processed by the connector produces spec-shaped envelopes on
    raw.wellness keyed by athlete_id, with event_time as epoch-ms LONG."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    create_all(bootstrap)  # idempotent: ensures raw.wellness exists

    csv_path = tmp_path / "wellness.csv"
    csv_path.write_text(_HEADER + _ROWS, encoding="utf-8")

    publisher = WellnessPublisher(bootstrap_servers=bootstrap)
    summary = process_csv_file(csv_path, publisher)

    assert summary.published == 2
    assert summary.skipped == 0

    messages = _consume_n(bootstrap, "raw.wellness", n=2)

    envelopes = []
    for msg in messages:
        assert msg.key() is not None
        key = msg.key().decode("utf-8") if isinstance(msg.key(), (bytes, bytearray)) else msg.key()
        assert key in ("A1", "A2")

        envelope = json.loads(msg.value().decode("utf-8"))
        envelopes.append(envelope)

        # --- raw envelope shape (exact key set) ---
        assert set(envelope.keys()) == {
            "event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"
        }
        assert uuid.UUID(envelope["event_id"]).version == 4
        assert envelope["source"] == "synthetic_wellness"
        assert envelope["athlete_id"] in ("A1", "A2")

        # CRITICAL: event_time is epoch-ms LONG, NOT ISO-8601 string (W1-5)
        assert isinstance(envelope["event_time"], int), (
            f"event_time must be epoch-ms int, got {type(envelope['event_time'])}"
        )
        # ingest_time is also epoch-ms int
        assert isinstance(envelope["ingest_time"], int)

        # payload carries the source fields verbatim
        payload = envelope["payload"]
        expected_payload_keys = {
            "athlete_id", "date", "hrv", "sleep_hours", "resting_hr",
            "steps", "body_weight_kg", "energy", "soreness", "mood",
            "stress", "perceived_recovery",
        }
        assert set(payload.keys()) == expected_payload_keys

    # verify epoch-ms values are UTC midnight for each date
    by_athlete = {e["athlete_id"]: e for e in envelopes}
    assert by_athlete["A1"]["event_time"] == _EXPECTED_EVENT_TIME_ROW1
    assert by_athlete["A2"]["event_time"] == _EXPECTED_EVENT_TIME_ROW2

    # verify payload content is verbatim
    assert by_athlete["A1"]["payload"]["date"] == "2025-03-01"
    assert by_athlete["A1"]["payload"]["hrv"] == 65.0
    assert by_athlete["A2"]["payload"]["sleep_hours"] == 8.0
