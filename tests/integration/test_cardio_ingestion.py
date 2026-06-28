"""Phase C1 integration: cardio CSV drop -> events land in raw.cardio.

End-to-end verification of the cardio ingestion connector against a real Kafka
broker (testcontainers Redpanda). Drops a cardio CSV file, runs the connector,
then consumes from ``raw.cardio`` and asserts the published JSON envelopes
match the event-contracts spec "Raw Topic JSON Shape":

  - message key == athlete_id (co-partitioning, ADR-4)
  - event_id is a valid UUID v4 string
  - event_time is an epoch-ms LONG integer (ISO datetime -> UTC epoch-ms)
  - ingest_time is an integer (epoch-ms)
  - source == "synthetic_cardio"
  - payload carries source fields verbatim
  - session_load is NOT present (canonicalization-layer field, PR-C2)

Docker-gated: the ``redpanda`` fixture (tests/conftest.py) skips this test when
the Docker daemon is unreachable, so collection never requires a broker.

Mirrors tests/integration/test_wellness_ingestion.py structure.
"""

from __future__ import annotations

import json
import uuid

import pytest

from bootstrap.create_topics import create_all
from ingestion.cardio.producer import CardioPublisher
from ingestion.cardio.watcher import process_csv_file

pytestmark = pytest.mark.integration

_HEADER = (
    "athlete_id,activity_type,duration_sec,timestamp,distance_km,avg_hr,tss\n"
)
_ROWS = (
    "A1,Run,3600,2025-06-01T10:00:00,10.0,150,70.0\n"
    "A2,Ride,5400,2025-06-01T08:00:00,40.0,140,85.0\n"
)

# 2025-06-01T10:00:00 UTC epoch-ms
_EXPECTED_EVENT_TIME_ROW1 = 1748772000000
# 2025-06-01T08:00:00 UTC epoch-ms
_EXPECTED_EVENT_TIME_ROW2 = 1748764800000


def _consume_n(bootstrap_servers: str, topic: str, n: int, timeout: float = 30.0):
    """Consume exactly ``n`` messages from ``topic`` or raise on timeout."""
    import time
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"test-cardio-ingestion-{uuid.uuid4()}",
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


def test_csv_drop_lands_envelopes_in_raw_cardio(redpanda_endpoints, tmp_path):
    """A cardio CSV processed by the connector produces spec-shaped envelopes on
    raw.cardio keyed by athlete_id, with event_time as epoch-ms LONG."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    create_all(bootstrap)  # idempotent: ensures raw.cardio exists

    csv_path = tmp_path / "cardio.csv"
    csv_path.write_text(_HEADER + _ROWS, encoding="utf-8")

    publisher = CardioPublisher(bootstrap_servers=bootstrap)
    summary = process_csv_file(csv_path, publisher)

    assert summary.published == 2
    assert summary.skipped == 0

    messages = _consume_n(bootstrap, "raw.cardio", n=2)

    envelopes = []
    for msg in messages:
        assert msg.key() is not None
        key = msg.key().decode("utf-8") if isinstance(msg.key(), (bytes, bytearray)) else msg.key()
        assert key in ("A1", "A2")

        envelope = json.loads(msg.value().decode("utf-8"))
        envelopes.append(envelope)

        # --- raw envelope shape ---
        assert set(envelope.keys()) == {
            "event_id", "event_time", "ingest_time", "source", "athlete_id", "payload"
        }
        assert uuid.UUID(envelope["event_id"]).version == 4
        assert envelope["source"] == "synthetic_cardio"
        assert envelope["athlete_id"] in ("A1", "A2")

        # event_time must be epoch-ms LONG (not ISO string)
        assert isinstance(envelope["event_time"], int), (
            f"event_time must be epoch-ms int, got {type(envelope['event_time'])}"
        )
        assert isinstance(envelope["ingest_time"], int)

        # payload carries source fields verbatim
        payload = envelope["payload"]
        assert set(payload.keys()) == {
            "athlete_id",
            "activity_type",
            "duration_sec",
            "timestamp",
            "distance_km",
            "avg_hr",
            "tss",
        }

        # session_load is a canonicalization-layer field -> NOT in the raw envelope
        assert "session_load" not in envelope
        assert "session_load" not in payload

    # verify epoch-ms values match UTC ISO datetimes from CSV
    by_athlete = {e["athlete_id"]: e for e in envelopes}
    assert by_athlete["A1"]["event_time"] == _EXPECTED_EVENT_TIME_ROW1
    assert by_athlete["A2"]["event_time"] == _EXPECTED_EVENT_TIME_ROW2

    # verify payload content is verbatim
    assert by_athlete["A1"]["payload"]["activity_type"] == "Run"
    assert by_athlete["A1"]["payload"]["tss"] == 70.0
    assert by_athlete["A2"]["payload"]["activity_type"] == "Ride"
    assert by_athlete["A2"]["payload"]["distance_km"] == 40.0
