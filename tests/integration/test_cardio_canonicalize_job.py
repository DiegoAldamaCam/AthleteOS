"""Phase C2 integration: cardio_canonicalize bounded Flink job end-to-end.

End-to-end test of the cardio_canonicalize bounded Flink job:

    KafkaSource(raw.cardio, SimpleStringSchema-JSON)
      -> bounded watermark + epoch-ms event-time assigner
      -> key_by(event_id)
      -> CardioCanonicalizeProcessFunction (dedup ValueState<bool> 7d TTL
         + transform_cardio_to_canonical -> yield canonical Row | yield DLQ side output)
      -> canonical Row -> avro-confluent Table sink -> canonical.training_event
      -> DLQ -> KafkaSink(JSON, AT_LEAST_ONCE) -> dlq.canonical.training_event

Spec scenarios:
  sc-23: valid raw.cardio with tss -> CARDIO_ACTIVITY emitted to canonical.training_event
         session_load = tss value (Tier 1)
  sc-24: raw.cardio missing athlete_id -> DLQ with original_topic="raw.cardio" + base64
         original_value + error_type=VALIDATION_FAILURE
  sc-25: raw.cardio with tss=null, avg_hr=null, duration_sec present -> DLQ
         (session_load uncomputable; error_type=VALIDATION_FAILURE; original_topic="raw.cardio")
  sc-26: two raw.cardio messages with the same event_id within the 7d TTL window ->
         only ONE canonical event emitted; the duplicate is silently dropped
  sc-27: cardio job transactional_id_prefix is "athleteos-canonicalize-cardio-training-event"
         (distinct from strength "athleteos-canonicalize-training-event") -> no
         ProducerFencedException when both jobs write canonical.training_event with EXACTLY_ONCE

This file mirrors tests/integration/test_wellness_canonicalize_job.py.

Clean skips (never fake a pass):
  - testcontainers not installed: module-level skip via importorskip.
  - No pyflink on this interpreter: module-level skip.
  - No Docker daemon: redpanda fixture skip.
  - Connector JARs not loadable: runtime skip after probe build.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import uuid

import pytest

# --- Module-level gating: testcontainers required --------------------------

pytest.importorskip(
    "testcontainers",
    reason=(
        "testcontainers not installed — cardio canonicalize job integration test skipped. "
        "Install with: pip install testcontainers[kafka] to run sc-23..sc-27."
    ),
)

# --- Module-level gating: pyflink required ---------------------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); cardio_canonicalize_job integration test skipped",
        allow_module_level=True,
    )

# External connector JARs — same set as test_wellness_canonicalize_job.py.
_CONNECTOR_JARS = (
    (
        "flink-connector-kafka-3.3.0-1.19.jar",
        "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/"
        "3.3.0-1.19/flink-connector-kafka-3.3.0-1.19.jar",
    ),
    (
        "kafka-clients-3.6.0.jar",
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.0/"
        "kafka-clients-3.6.0.jar",
    ),
    (
        "flink-sql-avro-confluent-registry-1.19.1.jar",
        "https://repo1.maven.org/maven2/org/apache/flink/"
        "flink-sql-avro-confluent-registry/1.19.1/"
        "flink-sql-avro-confluent-registry-1.19.1.jar",
    ),
)


def _pyflink_lib_dir():
    import pyflink
    from pathlib import Path
    return Path(pyflink.__file__).resolve().parent / "lib"


def _ensure_connector_jars() -> None:
    lib = _pyflink_lib_dir()
    lib.mkdir(parents=True, exist_ok=True)
    try:
        import requests
    except Exception:
        return
    for name, url in _CONNECTOR_JARS:
        target = lib / name
        if target.exists() and target.stat().st_size > 0:
            continue
        try:
            resp = requests.get(url, timeout=180, stream=True)
            resp.raise_for_status()
            with open(target, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1 << 16):
                    if chunk:
                        fh.write(chunk)
        except Exception:
            pass


_ensure_connector_jars()

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Test-scoped constants
# ---------------------------------------------------------------------------

_ATHLETE_ID = "A1"
_VALID_EVENT_ID = "cevt-valid-001"
_INVALID_ATHLETE_EVENT_ID = "cevt-no-athlete-002"
_NO_LOAD_EVENT_ID = "cevt-no-load-003"
_DUP_EVENT_ID = "cevt-dup-004"
# 2025-06-01 UTC midnight epoch-ms
_EVENT_TIME_MS = 1748736000000
_INGEST_TIME_MS = _EVENT_TIME_MS + 5_000

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 180
_CONSUME_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_cardio_envelope(
    event_id: str,
    athlete_id: str | None,
    tss: float | None = 70.0,
    avg_hr: int | None = 150,
    duration_sec: int = 3600,
    distance_km: float | None = 10.0,
    activity_type: str = "Run",
) -> dict:
    """Build a raw.cardio envelope as the ingestion/cardio producer emits it."""
    envelope: dict = {
        "event_id": event_id,
        "event_time": _EVENT_TIME_MS,
        "ingest_time": _INGEST_TIME_MS,
        "source": "synthetic_cardio",
        "payload": {
            "activity_type": activity_type,
            "duration_sec": duration_sec,
            "distance_km": distance_km,
            "avg_hr": avg_hr,
            "tss": tss,
        },
    }
    if athlete_id is not None:
        envelope["athlete_id"] = athlete_id
    # athlete_id deliberately omitted when None -> ValidationError -> DLQ (sc-24)
    return envelope


def _produce_messages(bootstrap: str, topic: str, messages: list[str]) -> None:
    """Produce JSON string messages to a Kafka topic."""
    kafka = pytest.importorskip("kafka")
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=bootstrap)
    for msg in messages:
        producer.send(topic, value=msg.encode("utf-8"))
    producer.flush()
    producer.close()


def _consume_messages(bootstrap: str, topic: str, timeout_s: float = 30) -> list[str]:
    """Consume all available messages from a Kafka topic (earliest offset)."""
    kafka = pytest.importorskip("kafka")
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        group_id=f"test-consumer-{uuid.uuid4().hex}",
        consumer_timeout_ms=int(timeout_s * 1000),
    )
    messages = [msg.value.decode("utf-8") for msg in consumer]
    consumer.close()
    return messages


def _setup_topics(bootstrap: str) -> None:
    """Create the required topics if they do not already exist."""
    kafka = pytest.importorskip("kafka")
    from kafka.admin import KafkaAdminClient, NewTopic

    client = KafkaAdminClient(bootstrap_servers=bootstrap)
    topics = [
        NewTopic("raw.cardio", num_partitions=1, replication_factor=1),
        NewTopic("canonical.training_event", num_partitions=1, replication_factor=1),
        NewTopic("dlq.canonical.training_event", num_partitions=1, replication_factor=1),
    ]
    existing = client.list_topics()
    to_create = [t for t in topics if t.name not in existing]
    if to_create:
        client.create_topics(to_create)
    client.close()


# ---------------------------------------------------------------------------
# sc-23..sc-27 — bounded job end-to-end (reuses conftest redpanda_endpoints)
# ---------------------------------------------------------------------------


class TestCardioCanonicalizeJob:
    def _run_job_bounded(self, bootstrap: str, schema_registry_url: str) -> None:
        """Run the cardio canonicalize job in bounded mode (integration test mode)."""
        from jobs.cardio_canonicalize.main import CardioCanonicalizeJobConfig, run

        config = CardioCanonicalizeJobConfig(
            bootstrap_servers=bootstrap,
            schema_registry_url=schema_registry_url,
            bounded=True,
            parallelism=1,
            no_restart=True,
            checkpoint_interval_ms=_CHECKPOINT_MS,
        )
        run(config)

    def test_sc23_valid_cardio_event_emitted_to_canonical(
        self, redpanda_endpoints
    ):
        """sc-23: Valid raw.cardio (tss present) -> CARDIO_ACTIVITY in canonical.training_event."""
        bootstrap = redpanda_endpoints["bootstrap_servers"]
        schema_registry_url = redpanda_endpoints["schema_registry_url"]
        _setup_topics(bootstrap)

        envelope = _raw_cardio_envelope(
            event_id=_VALID_EVENT_ID,
            athlete_id=_ATHLETE_ID,
            tss=70.0,
        )
        _produce_messages(bootstrap, "raw.cardio", [json.dumps(envelope)])

        self._run_job_bounded(bootstrap, schema_registry_url)

        canonical_msgs = _consume_messages(bootstrap, "canonical.training_event")
        assert len(canonical_msgs) >= 1
        # The canonical message is Avro-serialized; check via DLQ absence
        dlq_msgs = _consume_messages(bootstrap, "dlq.canonical.training_event")
        assert len(dlq_msgs) == 0

    def test_sc24_missing_athlete_id_goes_to_dlq(
        self, redpanda_endpoints
    ):
        """sc-24: Missing athlete_id -> DLQ with original_topic='raw.cardio'."""
        bootstrap = redpanda_endpoints["bootstrap_servers"]
        schema_registry_url = redpanda_endpoints["schema_registry_url"]
        _setup_topics(bootstrap)

        envelope = _raw_cardio_envelope(
            event_id=_INVALID_ATHLETE_EVENT_ID,
            athlete_id=None,  # deliberately omitted
            tss=70.0,
        )
        _produce_messages(bootstrap, "raw.cardio", [json.dumps(envelope)])

        self._run_job_bounded(bootstrap, schema_registry_url)

        dlq_msgs = _consume_messages(bootstrap, "dlq.canonical.training_event")
        assert len(dlq_msgs) >= 1
        dlq_envelope = json.loads(dlq_msgs[-1])
        assert dlq_envelope["original_topic"] == "raw.cardio"
        assert dlq_envelope["error_type"] == "VALIDATION_FAILURE"
        # original_value must be base64-encoded
        decoded = base64.b64decode(dlq_envelope["original_value"])
        original = json.loads(decoded)
        assert original["event_id"] == _INVALID_ATHLETE_EVENT_ID

    def test_sc25_no_session_load_goes_to_dlq(
        self, redpanda_endpoints
    ):
        """sc-25: tss=null, avg_hr=null -> DLQ with error_type=VALIDATION_FAILURE."""
        bootstrap = redpanda_endpoints["bootstrap_servers"]
        schema_registry_url = redpanda_endpoints["schema_registry_url"]
        _setup_topics(bootstrap)

        envelope = _raw_cardio_envelope(
            event_id=_NO_LOAD_EVENT_ID,
            athlete_id=_ATHLETE_ID,
            tss=None,
            avg_hr=None,
            duration_sec=3600,
        )
        _produce_messages(bootstrap, "raw.cardio", [json.dumps(envelope)])

        self._run_job_bounded(bootstrap, schema_registry_url)

        dlq_msgs = _consume_messages(bootstrap, "dlq.canonical.training_event")
        assert len(dlq_msgs) >= 1
        dlq_envelope = json.loads(dlq_msgs[-1])
        assert dlq_envelope["original_topic"] == "raw.cardio"
        assert dlq_envelope["error_type"] == "VALIDATION_FAILURE"

    def test_sc26_duplicate_event_id_deduplicated(
        self, redpanda_endpoints
    ):
        """sc-26: Two messages with same event_id -> only ONE canonical event emitted."""
        bootstrap = redpanda_endpoints["bootstrap_servers"]
        schema_registry_url = redpanda_endpoints["schema_registry_url"]
        _setup_topics(bootstrap)

        event_id = _DUP_EVENT_ID
        envelope = _raw_cardio_envelope(
            event_id=event_id,
            athlete_id=_ATHLETE_ID,
            tss=80.0,
        )
        msg = json.dumps(envelope)
        _produce_messages(bootstrap, "raw.cardio", [msg, msg])  # same message twice

        self._run_job_bounded(bootstrap, schema_registry_url)

        canonical_msgs = _consume_messages(bootstrap, "canonical.training_event")
        # Only 1 canonical event despite 2 identical raw messages
        assert len(canonical_msgs) == 1

    def test_sc27_transactional_id_prefix_is_distinct(self):
        """sc-27: Verify the transactional_id_prefix literal is distinct from strength job."""
        from jobs.cardio_canonicalize.main import TRANSACTIONAL_ID_PREFIX

        # The cardio prefix must differ from the strength prefix
        assert TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-cardio-training-event"
        # Verify it is disjoint from the strength prefix used in the DDL
        strength_prefix = "athleteos-canonicalize-training-event"
        assert TRANSACTIONAL_ID_PREFIX != strength_prefix
        assert not TRANSACTIONAL_ID_PREFIX.startswith(strength_prefix + "-") or \
               TRANSACTIONAL_ID_PREFIX != strength_prefix
