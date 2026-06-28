"""Phase N2 integration: nutrition_canonicalize bounded Flink job end-to-end.

End-to-end test of the nutrition_canonicalize bounded Flink job:

    KafkaSource(raw.nutrition, SimpleStringSchema-JSON)
      -> bounded watermark + epoch-ms event-time assigner
      -> key_by(event_id)
      -> NutritionCanonicalizeProcessFunction (dedup ValueState<bool> 7d TTL
         + transform_nutrition_to_canonical -> yield canonical Row | yield DLQ side output)
      -> canonical Row -> avro-confluent Table sink -> canonical.wellness_event
      -> DLQ -> KafkaSink(JSON, AT_LEAST_ONCE) -> dlq.canonical.wellness_event

Spec scenarios:
  sc-20: valid raw.nutrition → NUTRITION_DAILY emitted to canonical.wellness_event
         with calories=2000, nutrition_adherence=0.8, all recovery/wellness fields = None
  sc-21: raw.nutrition missing athlete_id → DLQ with original_topic="raw.nutrition" +
         base64 original_value + error_type=VALIDATION_FAILURE
  sc-22: raw.nutrition with event_time absent/null → DLQ with error_type indicating
         validation failure
  sc-23: two raw.nutrition messages with same event_id within 7d TTL → only ONE
         canonical event emitted; duplicate is silently dropped
  sc-24: re-delivered event after TTL expiry → second canonical event emitted; PG UPSERT
         idempotent (TTL not testable in bounded job; second message asserts emission;
         accepted limitation documented)
  sc-25: TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-nutrition-wellness-event" (no Docker)
  sc-26: NUTRITION_DAILY gracefully ignored by wellness_metrics W3-5 guard (all-null
         HRV/sleep_hours/perceived_recovery → no DB write; structural assertion only)

This file mirrors tests/integration/test_recovery_canonicalize_job.py (the CORRECTED version).

Clean skips (never fake a pass):
  - testcontainers not installed: module-level skip via importorskip.
  - No pyflink on this interpreter: module-level skip.
  - No Docker daemon: redpanda fixture skip.
  - Connector JARs not loadable: runtime skip after probe build.

CRITICAL: uses ``from testcontainers.kafka import RedpandaContainer`` (via conftest
``redpanda_endpoints`` fixture — NEVER testcontainers.redpanda; obs #214 cardio CI bug).
NO local redpanda fixture defined here.
"""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import threading
import uuid

import pytest

# --- Module-level gating: testcontainers required --------------------------

pytest.importorskip(
    "testcontainers",
    reason=(
        "testcontainers not installed — nutrition canonicalize job integration test skipped. "
        "Install with: pip install testcontainers[kafka] to run sc-20..sc-26."
    ),
)

# --- Module-level gating: pyflink required ---------------------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); nutrition_canonicalize_job integration test skipped",
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
_VALID_EVENT_ID = "nevt-valid-001"
_INVALID_ATHLETE_EVENT_ID = "nevt-no-athlete-002"
_NO_EVENT_TIME_EVENT_ID = "nevt-no-time-003"
_DUP_EVENT_ID = "nevt-dup-004"
_TTL_REDELIVERY_EVENT_ID_BASE = "nevt-sc24"
# 2025-06-01 UTC midnight epoch-ms
_EVENT_TIME_MS = 1748736000000
_INGEST_TIME_MS = _EVENT_TIME_MS + 5_000

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 180
_CONSUME_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_nutrition_envelope(
    event_id: str,
    athlete_id: str | None,
    calories: int | None = 2000,
    protein_g: float | None = 120.0,
    carbs_g: float | None = 250.0,
    fat_g: float | None = 70.0,
    adherence_score: float | None = 0.8,
    event_time: int | None = _EVENT_TIME_MS,
) -> dict:
    """Build a raw.nutrition envelope as the ingestion/nutrition producer emits it.

    NOTE: payload key is 'adherence_score' (source-faithful per sc-8).
    The rename to 'nutrition_adherence' happens in the transform (sc-16/sc-20).
    """
    envelope: dict = {
        "event_id": event_id,
        "ingest_time": _INGEST_TIME_MS,
        "source": "nutrition_csv",
        "payload": {
            "calories": calories,
            "protein_g": protein_g,
            "carbs_g": carbs_g,
            "fat_g": fat_g,
            "adherence_score": adherence_score,  # source-faithful (sc-8)
        },
    }
    if event_time is not None:
        envelope["event_time"] = event_time
    # event_time deliberately omitted when None -> ValidationError -> DLQ (sc-22)
    if athlete_id is not None:
        envelope["athlete_id"] = athlete_id
    # athlete_id deliberately omitted when None -> ValidationError -> DLQ (sc-21)
    return envelope


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


def _produce_records(
    bootstrap_servers: str,
    topic: str,
    values: list,
    key: str = _ATHLETE_ID,
) -> None:
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _on_err(err, _msg):
        if err is not None:
            raise RuntimeError(f"kafka produce failed: {err}")

    for value in values:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            callback=_on_err,
        )
    producer.flush(timeout=30)


def _consume_available(bootstrap_servers: str, topic: str, timeout: float) -> list:
    """Consume all available committed messages up to timeout seconds."""
    import time
    from confluent_kafka import Consumer
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"e2e-nutrition-consume-{uuid.uuid4()}",
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
# Shared helper: run the bounded nutrition job in a background thread
# ---------------------------------------------------------------------------


def _run_bounded_job(
    bootstrap_servers: str,
    schema_registry_url: str,
    raw_topic: str,
    canonical_topic: str,
    dlq_topic: str,
    run_id: str,
) -> None:
    """Probe connector JARs, run the bounded nutrition canonicalize job."""
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

    from jobs.nutrition_canonicalize.main import NutritionCanonicalizeJobConfig, run

    config = NutritionCanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        group_id=f"nutrition-canonicalize-e2e-{run_id}",
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
            f"nutrition canonicalize job did not terminate within "
            f"{_JOB_RUN_TIMEOUT_S}s (bounded source should drain)"
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")


# ---------------------------------------------------------------------------
# sc-20: valid raw.nutrition → NUTRITION_DAILY in canonical.wellness_event
# ---------------------------------------------------------------------------


def test_sc20_valid_nutrition_event_emitted_to_canonical(redpanda_endpoints):
    """sc-20: Valid raw.nutrition with calories=2000, adherence_score=0.8 →
    NUTRITION_DAILY in canonical.wellness_event; nutrition_adherence=0.8,
    all recovery/subjective fields = None."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.nutrition.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_nutrition_envelope(
        event_id=_VALID_EVENT_ID,
        athlete_id=_ATHLETE_ID,
        calories=2000,
        adherence_score=0.8,
    )
    _produce_records(bootstrap, raw_topic, [json.dumps(envelope).encode("utf-8")])

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    assert len(canonical_msgs) >= 1, "expected at least one canonical NUTRITION_DAILY record"

    canonical_record, canonical_schema_id = _decode_confluent_avro(
        canonical_msgs[0].value(), registry_url
    )
    assert canonical_record["event_type"] == "NUTRITION_DAILY"
    assert canonical_record["athlete_id"] == _ATHLETE_ID
    assert canonical_record["schema_version"] == 1
    assert canonical_schema_id is not None and canonical_schema_id > 0
    # calories must be 2000; nutrition_adherence must be 0.8
    assert canonical_record["calories"] == 2000
    assert canonical_record["nutrition_adherence"] == pytest.approx(0.8)
    # The rename must have happened: adherence_score MUST NOT appear
    assert "adherence_score" not in canonical_record
    # Recovery/subjective fields must be None
    assert canonical_record["sleep_hours"] is None
    assert canonical_record["energy"] is None
    assert canonical_record["perceived_recovery"] is None

    # No DLQ records for a valid message
    dlq_msgs = _consume_available(bootstrap, dlq_topic, _CONSUME_TIMEOUT_S)
    assert len(dlq_msgs) == 0


# ---------------------------------------------------------------------------
# sc-21: missing athlete_id → DLQ with original_topic="raw.nutrition"
# ---------------------------------------------------------------------------


def test_sc21_missing_athlete_id_goes_to_dlq(redpanda_endpoints):
    """sc-21: Missing athlete_id → DLQ with original_topic='raw.nutrition',
    base64 original_value, error_type=VALIDATION_FAILURE."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.nutrition.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_nutrition_envelope(
        event_id=_INVALID_ATHLETE_EVENT_ID,
        athlete_id=None,  # deliberately omitted
    )
    _produce_records(
        bootstrap, raw_topic,
        [json.dumps(envelope).encode("utf-8")],
        key="",  # no athlete_id → use empty key
    )

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    dlq_msgs = _consume_available(bootstrap, dlq_topic, _CONSUME_TIMEOUT_S)
    assert len(dlq_msgs) >= 1, "expected DLQ record for missing athlete_id"

    dlq_record = json.loads(dlq_msgs[-1].value().decode("utf-8"))
    assert dlq_record["original_topic"] == raw_topic, (
        f"DLQ original_topic must be '{raw_topic}', got {dlq_record.get('original_topic')!r}"
    )
    assert dlq_record["error_type"] == "VALIDATION_FAILURE", (
        f"missing athlete_id → VALIDATION_FAILURE, got {dlq_record.get('error_type')!r}"
    )
    # original_value must be base64-encoded bytes of the raw JSON envelope
    decoded = base64.b64decode(dlq_record["original_value"])
    original = json.loads(decoded)
    assert original["event_id"] == _INVALID_ATHLETE_EVENT_ID

    # No canonical record for an invalid message
    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    assert len(canonical_msgs) == 0


# ---------------------------------------------------------------------------
# sc-22: missing event_time → DLQ with validation error_type
# ---------------------------------------------------------------------------


def test_sc22_missing_event_time_goes_to_dlq(redpanda_endpoints):
    """sc-22: Missing event_time → DLQ with error_type indicating validation failure."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.nutrition.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    # event_time deliberately omitted
    envelope = _raw_nutrition_envelope(
        event_id=_NO_EVENT_TIME_EVENT_ID,
        athlete_id=_ATHLETE_ID,
        event_time=None,
    )
    _produce_records(bootstrap, raw_topic, [json.dumps(envelope).encode("utf-8")])

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    dlq_msgs = _consume_available(bootstrap, dlq_topic, _CONSUME_TIMEOUT_S)
    assert len(dlq_msgs) >= 1, "expected DLQ record for missing event_time"

    dlq_record = json.loads(dlq_msgs[-1].value().decode("utf-8"))
    assert dlq_record["error_type"] == "VALIDATION_FAILURE"
    assert dlq_record["original_topic"] == raw_topic


# ---------------------------------------------------------------------------
# sc-23: duplicate event_id deduplicated
# ---------------------------------------------------------------------------


def test_sc23_duplicate_event_id_deduplicated(redpanda_endpoints):
    """sc-23: Two raw.nutrition messages with same event_id within 7d TTL →
    only ONE canonical event emitted; second is silently dropped."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.nutrition.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_nutrition_envelope(
        event_id=_DUP_EVENT_ID,
        athlete_id=_ATHLETE_ID,
        calories=2200,
    )
    msg = json.dumps(envelope).encode("utf-8")
    _produce_records(bootstrap, raw_topic, [msg, msg])  # same message twice

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    # Only 1 canonical event despite 2 identical raw messages (ValueState dedup)
    assert len(canonical_msgs) == 1, (
        f"expected exactly ONE canonical record (duplicate must be deduped), "
        f"got {len(canonical_msgs)}"
    )


# ---------------------------------------------------------------------------
# sc-24: re-delivered event after TTL expiry → second emission (accepted limitation)
# ---------------------------------------------------------------------------


def test_sc24_post_ttl_redelivery_emits_second_canonical(redpanda_endpoints):
    """sc-24: Re-delivered event after 7d TTL expiry → second canonical event emitted.

    NOTE: The 7-day TTL is not practically testable in a bounded integration job
    (we cannot fast-forward Flink state TTL clock). This test validates the
    REDELIVERY path by sending two messages with DIFFERENT event_ids simulating
    the scenario where the first arrived before TTL and the second after TTL
    (both should produce canonical records since they are distinct events).

    The PG UPSERT on (athlete_id, metric_date) collapses re-delivered rows
    idempotently — no data corruption (decision #238, accepted limitation).

    Accepted limitation documented: TTL fast-forward not supported in bounded
    integration mode. This test proves the emission path; TTL enforcement is
    an operational concern verified by the ValueState configuration in main.py.
    """
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.nutrition.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    # Two DISTINCT event_ids (simulates re-delivery after TTL expiry per the spec).
    # Both are valid and should produce canonical records.
    first_envelope = _raw_nutrition_envelope(
        event_id=f"{_TTL_REDELIVERY_EVENT_ID_BASE}-first",
        athlete_id=_ATHLETE_ID,
        calories=2100,
    )
    second_envelope = _raw_nutrition_envelope(
        event_id=f"{_TTL_REDELIVERY_EVENT_ID_BASE}-second",
        athlete_id=_ATHLETE_ID,
        calories=2100,
    )
    _produce_records(
        bootstrap, raw_topic,
        [json.dumps(first_envelope).encode("utf-8"), json.dumps(second_envelope).encode("utf-8")],
    )

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    # Both distinct events must produce canonical records (emission path verified)
    assert len(canonical_msgs) == 2, (
        f"expected 2 canonical records (distinct event_ids; re-delivery path), "
        f"got {len(canonical_msgs)}"
    )


# ---------------------------------------------------------------------------
# sc-25: TRANSACTIONAL_ID_PREFIX literal (no Docker required — unit-level check)
# ---------------------------------------------------------------------------


def test_sc25_transactional_id_prefix_exact_literal_no_docker():
    """sc-25: TRANSACTIONAL_ID_PREFIX must equal the exact literal string
    'athleteos-canonicalize-nutrition-wellness-event'.
    Does NOT require Docker — asserts the module-level constant directly."""
    from jobs.nutrition_canonicalize.main import TRANSACTIONAL_ID_PREFIX

    assert TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-nutrition-wellness-event"
    # Verify disjoint from wellness prefix → no ProducerFencedException
    assert TRANSACTIONAL_ID_PREFIX != "athleteos-canonicalize-wellness-event"
    # Verify disjoint from recovery prefix
    assert TRANSACTIONAL_ID_PREFIX != "athleteos-canonicalize-recovery-wellness-event"


# ---------------------------------------------------------------------------
# sc-26: NUTRITION_DAILY gracefully ignored by wellness_metrics W3-5 guard
# ---------------------------------------------------------------------------


def test_sc26_nutrition_daily_wellness_metrics_guard(redpanda_endpoints):
    """sc-26: NUTRITION_DAILY canonical event in canonical.wellness_event →
    wellness_metrics W3-5 all-null guard fires (HRV, sleep_hours,
    perceived_recovery all None) → no DB write for this event.

    This test validates that the nutrition job emits a NUTRITION_DAILY event
    without error and that the canonical output carries all the None fields
    that would trigger the wellness_metrics W3-5 guard. The actual guard logic
    is in jobs/wellness_metrics/main.py (unchanged per decision #238); we
    verify the structural conditions that cause it to fire.
    """
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.nutrition.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_nutrition_envelope(
        event_id="nevt-sc26-wellness-guard",
        athlete_id=_ATHLETE_ID,
        calories=1800,
        adherence_score=0.95,
    )
    _produce_records(bootstrap, raw_topic, [json.dumps(envelope).encode("utf-8")])

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    # Nutrition event MUST emit without error (no DLQ)
    dlq_msgs = _consume_available(bootstrap, dlq_topic, _CONSUME_TIMEOUT_S)
    assert len(dlq_msgs) == 0, (
        f"sc-26: no DLQ record expected for valid NUTRITION_DAILY, "
        f"got {len(dlq_msgs)} DLQ messages"
    )

    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    assert len(canonical_msgs) >= 1, "NUTRITION_DAILY must be emitted to canonical topic"
    canonical_record, _ = _decode_confluent_avro(canonical_msgs[0].value(), registry_url)
    assert canonical_record["event_type"] == "NUTRITION_DAILY"
    assert canonical_record["athlete_id"] == _ATHLETE_ID

    # W3-5 guard fields: HRV, sleep_hours, perceived_recovery MUST be None
    # (these are the fields the wellness_metrics job checks — all None → no DB write)
    assert canonical_record["hrv"] is None, (
        "sc-26: hrv must be None for NUTRITION_DAILY → W3-5 guard fires"
    )
    assert canonical_record["sleep_hours"] is None, (
        "sc-26: sleep_hours must be None for NUTRITION_DAILY → W3-5 guard fires"
    )
    assert canonical_record["perceived_recovery"] is None, (
        "sc-26: perceived_recovery must be None for NUTRITION_DAILY → W3-5 guard fires"
    )
