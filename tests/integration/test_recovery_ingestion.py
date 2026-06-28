"""Phase R1 integration: Recovery CSV drop -> events land in raw.recovery.

End-to-end verification of the recovery ingestion connector against a real Kafka
broker (testcontainers Redpanda). Drops a recovery CSV file, runs the connector,
then consumes from ``raw.recovery`` and asserts the published JSON envelopes
match the event-contracts spec:

  - message key == athlete_id (co-partitioning, ADR-4)
  - event_id is a valid UUID v4 string
  - event_time is a UTC-midnight epoch-ms LONG for the record's date
  - ingest_time is an integer (epoch-ms)
  - source == "apple_health"
  - payload carries all 5 Apple Health source fields verbatim (no event_type)

Docker-gated: the ``redpanda`` fixture (tests/conftest.py) skips this test when
the Docker daemon is unreachable, so collection never requires a broker.

Mirrors tests/integration/test_wellness_ingestion.py structure.
CRITICAL: uses ``from testcontainers.kafka import RedpandaContainer`` (via the
shared ``redpanda_endpoints`` fixture — ADR-R4, obs #214 guard).
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from bootstrap.create_topics import create_all
from ingestion.recovery.producer import RecoveryPublisher
from ingestion.recovery.watcher import process_csv_file

pytestmark = pytest.mark.integration

_HEADER = "athlete_id,date,sleep_hours,resting_hr,hrv,steps,body_weight_kg\n"
_ROWS = (
    "A1,2025-06-01,7.5,58,42.0,8500,72.3\n"
    "A2,2025-06-02,8.0,55,45.0,9000,70.0\n"
)

# 2025-06-01 UTC midnight epoch-ms
_EXPECTED_EVENT_TIME_ROW1 = 1748736000000
# 2025-06-02 UTC midnight epoch-ms
_EXPECTED_EVENT_TIME_ROW2 = 1748822400000


def _consume_n(bootstrap_servers: str, topic: str, n: int, timeout: float = 30.0):
    """Consume exactly ``n`` messages from ``topic`` or raise on timeout."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"test-recovery-ingestion-{uuid.uuid4()}",
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


def test_csv_drop_lands_envelopes_in_raw_recovery(redpanda_endpoints, tmp_path):
    """A recovery CSV processed by the connector produces spec-shaped envelopes on
    raw.recovery keyed by athlete_id, with event_time as epoch-ms LONG."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    create_all(bootstrap)  # idempotent: ensures raw.recovery exists

    csv_path = tmp_path / "recovery.csv"
    csv_path.write_text(_HEADER + _ROWS, encoding="utf-8")

    publisher = RecoveryPublisher(bootstrap_servers=bootstrap)
    summary = process_csv_file(csv_path, publisher)

    assert summary.published == 2
    assert summary.skipped == 0

    messages = _consume_n(bootstrap, "raw.recovery", n=2)

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
        assert envelope["source"] == "apple_health"
        assert envelope["athlete_id"] in ("A1", "A2")

        # event_time is epoch-ms LONG, NOT ISO-8601 string
        assert isinstance(envelope["event_time"], int), (
            f"event_time must be epoch-ms int, got {type(envelope['event_time'])}"
        )
        assert isinstance(envelope["ingest_time"], int)

        # payload carries the 5 Apple Health source fields verbatim + athlete_id + date
        payload = envelope["payload"]
        expected_payload_keys = {
            "athlete_id", "date", "sleep_hours", "resting_hr", "hrv", "steps", "body_weight_kg"
        }
        assert set(payload.keys()) == expected_payload_keys

        # payload must NOT contain event_type (set by canonicalize job)
        assert "event_type" not in payload

    # verify epoch-ms values are UTC midnight for each date
    by_athlete = {e["athlete_id"]: e for e in envelopes}
    assert by_athlete["A1"]["event_time"] == _EXPECTED_EVENT_TIME_ROW1
    assert by_athlete["A2"]["event_time"] == _EXPECTED_EVENT_TIME_ROW2

    # verify payload content is verbatim
    assert by_athlete["A1"]["payload"]["date"] == "2025-06-01"
    assert by_athlete["A1"]["payload"]["hrv"] == 42.0
    assert by_athlete["A2"]["payload"]["sleep_hours"] == 8.0
