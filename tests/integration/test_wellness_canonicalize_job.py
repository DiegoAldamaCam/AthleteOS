"""Phase W2 integration: raw.wellness → canonical.wellness_event Avro + DLQ.

End-to-end test of the wellness_canonicalize bounded Flink job:

    KafkaSource(raw.wellness, SimpleStringSchema-JSON)
      -> bounded watermark + epoch-ms event-time assigner
      -> key_by(event_id)
      -> WellnessCanonicalizeProcessFunction (dedup ValueState<bool> 7d TTL
         + transform + validate → yield canonical Row | yield DLQ side output)
      -> canonical Row → avro-confluent Table sink → canonical.wellness_event
      -> DLQ → KafkaSink(JSON, AT_LEAST_ONCE) → dlq.canonical.wellness_event

Spec scenarios:
  W2-5: raw.wellness message missing required ``athlete_id`` → routed to DLQ
         with base64 original_value + error_type = VALIDATION_FAILURE.
  W2-6: two raw.wellness messages with the same event_id within the 7d TTL →
         only ONE canonical event emitted; the duplicate is silently dropped.

Mirrors tests/integration/test_canonicalize_job.py structure exactly.

Clean skips (never fake a pass):
  - No pyflink on this interpreter (CPython 3.14): module-level skip.
  - No Docker daemon: redpanda fixture skip.
  - Connector JARs not loadable: runtime skip after probe build.
"""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import threading
import time
import uuid
from datetime import datetime

import pytest

# --- Module-level gating (no pyflink → skip whole file) --------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); wellness canonicalize job integration test skipped",
        allow_module_level=True,
    )


# External connector JARs — same set as test_canonicalize_job.py.
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
_VALID_EVENT_ID = "wevt-valid-001"
_INVALID_EVENT_ID = "wevt-invalid-002"
# 2025-03-01 UTC midnight epoch-ms
_EVENT_TIME_MS = 1740787200000
_INGEST_TIME_MS = 1740790800000

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 180
_CONSUME_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_wellness_envelope(
    event_id: str,
    athlete_id: str | None,
    event_type: str = "WELLNESS_DAILY",
) -> dict:
    """Build a raw.wellness envelope similar to what the W1 producer emits."""
    env: dict = {
        "event_id": event_id,
        "event_time": _EVENT_TIME_MS,
        "ingest_time": _INGEST_TIME_MS,
        "source": "synthetic_wellness",
        "payload": {
            "event_type": event_type,
            "hrv": 65.0,
            "sleep_hours": 7.5,
            "resting_hr": 52,
            "steps": 9000,
            "body_weight_kg": 78.5,
            "energy": 7,
            "soreness": 3,
            "mood": 8,
            "stress": 4,
            "perceived_recovery": 8,
            "calories": None,
            "protein_g": None,
            "carbs_g": None,
            "fat_g": None,
            "nutrition_adherence": None,
        },
    }
    if athlete_id is not None:
        env["athlete_id"] = athlete_id
    # athlete_id deliberately omitted when None → ValidationError → DLQ (W2-5)
    return env


def _create_topics(bootstrap_servers: str, topics: list) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=30).topics.keys())
    new = [
        NewTopic(topic=t, num_partitions=1, replication_factor=1)
        for t in topics
        if t not in existing
    ]
    if new:
        fs = admin.create_topics(new)
        for _, f in fs.items():
            f.result()


def _produce_records(bootstrap_servers: str, topic: str, values: list) -> None:
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _on_err(err, _msg):
        if err is not None:
            raise RuntimeError(f"kafka produce failed: {err}")

    for value in values:
        producer.produce(
            topic=topic,
            key=_ATHLETE_ID.encode("utf-8"),
            value=value,
            callback=_on_err,
        )
    producer.flush(timeout=30)


def _consume_available(bootstrap_servers: str, topic: str, timeout: float) -> list:
    """Consume all available committed messages up to timeout seconds."""
    from confluent_kafka import Consumer
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"e2e-wellness-consume-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "fetch.wait.max.ms": "500",
    })
    consumer.subscribe([topic])
    messages = []
    deadline = time.monotonic() + timeout
    quiet_since = None
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if messages:
                    quiet_since = quiet_since or time.monotonic()
                    if time.monotonic() - quiet_since > 4.0:
                        break
                continue
            if msg.error():
                continue
            messages.append(msg)
        return messages
    finally:
        consumer.close()


def _normalize_ms(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return int(value)


def _decode_confluent_avro(raw: bytes, registry_url: str) -> tuple:
    """Decode Confluent-wire Avro (magic byte + 4-byte schema-id + Avro payload)."""
    from fastavro import schemaless_reader
    import requests as req
    assert raw[0] == 0, f"expected Confluent magic byte 0, got {raw[0]}"
    schema_id = int.from_bytes(raw[1:5], "big")
    schema_doc = req.get(registry_url + f"/schemas/ids/{schema_id}", timeout=10).json()
    schema = json.loads(schema_doc["schema"])
    record = schemaless_reader(io.BytesIO(raw[5:]), schema)
    return record, schema_id


# ---------------------------------------------------------------------------
# W2-5 + W2-6 — bounded job end-to-end
# ---------------------------------------------------------------------------


def test_wellness_canonicalize_job_dlq_and_dedup(redpanda_endpoints):
    """W2-5: raw.wellness missing athlete_id → DLQ (VALIDATION_FAILURE + base64).
    W2-6: duplicate event_id → only one canonical out, second silently dropped."""
    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    # Runtime probe: are the Kafka connector JARs loadable?
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaSource
        env_probe = StreamExecutionEnvironment.get_execution_environment()
        KafkaSource.builder()
        del env_probe
    except TypeError as exc:
        pytest.skip(
            "Kafka connector JARs not loadable by the pyflink gateway; "
            f"underlying error: {exc}"
        )

    # Isolated per-run topics to avoid cross-test data interference
    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.wellness.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    # Set BACKWARD compatibility on the per-test subject
    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap_servers, [raw_topic, canonical_topic, dlq_topic])

    # Build three raw.wellness records:
    #   1. valid record (VALID_EVENT_ID)
    #   2. duplicate of the valid record (same VALID_EVENT_ID → must be dropped)
    #   3. invalid record (INVALID_EVENT_ID, missing athlete_id → DLQ)
    valid_env = _raw_wellness_envelope(_VALID_EVENT_ID, athlete_id=_ATHLETE_ID)
    dup_env = _raw_wellness_envelope(_VALID_EVENT_ID, athlete_id=_ATHLETE_ID)
    invalid_env = _raw_wellness_envelope(_INVALID_EVENT_ID, athlete_id=None)  # W2-5

    _produce_records(
        bootstrap_servers,
        raw_topic,
        [
            json.dumps(valid_env).encode("utf-8"),
            json.dumps(dup_env).encode("utf-8"),
            json.dumps(invalid_env).encode("utf-8"),
        ],
    )

    # Run the bounded wellness canonicalize job
    from jobs.wellness_canonicalize.main import WellnessCanonicalizeJobConfig, run

    config = WellnessCanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"wellness-canonicalize-e2e-{run_id}",
        raw_topic=raw_topic,
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        schema_version=1,
        bounded=True,
        parallelism=1,
        no_restart=True,
    )

    outcome: dict = {}

    def _run_job():
        try:
            run(config)
            outcome["done"] = True
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    worker = threading.Thread(target=_run_job, daemon=True)
    worker.start()
    worker.join(timeout=_JOB_RUN_TIMEOUT_S)
    if worker.is_alive():
        pytest.fail(
            f"wellness canonicalize job did not terminate within "
            f"{_JOB_RUN_TIMEOUT_S}s (bounded source should drain)"
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")

    # --- W2-6: exactly ONE canonical record (duplicate dropped) ---------------
    canonical_msgs = _consume_available(
        bootstrap_servers, canonical_topic, _CONSUME_TIMEOUT_S
    )
    assert len(canonical_msgs) == 1, (
        f"expected exactly ONE canonical record (duplicate must be deduped), "
        f"got {len(canonical_msgs)}"
    )
    canonical_record, canonical_schema_id = _decode_confluent_avro(
        canonical_msgs[0].value(), registry_url
    )
    assert canonical_record["event_id"] == _VALID_EVENT_ID
    assert canonical_record["athlete_id"] == _ATHLETE_ID
    # ADR-16: event_type is STRING in the Avro record
    assert canonical_record["event_type"] == "WELLNESS_DAILY"
    assert canonical_record["schema_version"] == 1
    assert canonical_schema_id is not None and canonical_schema_id > 0
    assert _normalize_ms(canonical_record["event_time"]) == _EVENT_TIME_MS
    assert canonical_record["hrv"] == pytest.approx(65.0)

    # Dedup proof (FIX 4 — R3-C3): the test produced a valid record AND a
    # duplicate with the same event_id. Exactly one canonical message must have
    # come through (proving the duplicate was dropped). The count assertion at
    # line 341 already enforces len == 1; this assertion names the surviving
    # event_id explicitly so a regression (two records out) produces a clear
    # message instead of a silent pass.
    assert canonical_record["event_id"] == _VALID_EVENT_ID, (
        "duplicate event_id must be silently dropped by ValueState dedup; "
        "the surviving canonical record must carry the valid event_id"
    )

    # --- W2-5: invalid record (missing athlete_id) → DLQ --------------------
    dlq_msgs = _consume_available(
        bootstrap_servers, dlq_topic, _CONSUME_TIMEOUT_S
    )
    # FIX 2 (R3-C1): exactly one DLQ message — the invalid record only.
    # The valid record and its duplicate must NOT appear in the DLQ.
    assert len(dlq_msgs) == 1, (
        f"expected EXACTLY ONE DLQ record (the invalid missing-athlete_id record "
        f"only; valid + duplicate must not appear in DLQ), got {len(dlq_msgs)}"
    )
    dlq_record = json.loads(dlq_msgs[0].value().decode("utf-8"))

    assert dlq_record["original_topic"] == raw_topic
    assert dlq_record["error_type"] == "VALIDATION_FAILURE", (
        f"missing athlete_id → VALIDATION_FAILURE, got {dlq_record.get('error_type')!r}"
    )
    assert dlq_record["error_message"], "DLQ envelope must carry an error_message"
    # FIX 3 (R3-C2): original_key for a record missing athlete_id.
    # In process_element, athlete_id = raw.get("athlete_id") → None when the
    # field is absent. build_dlq_envelope stores it as-is, so original_key is
    # None in the envelope JSON.
    assert dlq_record["original_key"] is None, (
        f"DLQ envelope original_key must be None when athlete_id is missing, "
        f"got {dlq_record.get('original_key')!r}"
    )
    # W2-5: original_value is base64-encoded bytes of the raw JSON envelope
    decoded_original = base64.b64decode(dlq_record["original_value"]).decode("utf-8")
    assert json.loads(decoded_original)["event_id"] == _INVALID_EVENT_ID
    assert isinstance(dlq_record["timestamp"], int) and dlq_record["timestamp"] > 0
