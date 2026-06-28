"""Phase R2 integration: recovery_canonicalize bounded Flink job end-to-end.

End-to-end test of the recovery_canonicalize bounded Flink job:

    KafkaSource(raw.recovery, SimpleStringSchema-JSON)
      -> bounded watermark + epoch-ms event-time assigner
      -> key_by(event_id)
      -> RecoveryCanonicalizeProcessFunction (dedup ValueState<bool> 7d TTL
         + transform_recovery_to_canonical -> yield canonical Row | yield DLQ side output)
      -> canonical Row -> avro-confluent Table sink -> canonical.wellness_event
      -> DLQ -> KafkaSink(JSON, AT_LEAST_ONCE) -> dlq.canonical.wellness_event

Spec scenarios:
  sc-18: valid raw.recovery -> RECOVERY_SNAPSHOT emitted to canonical.wellness_event
         with sleep_hours=7.0 and all wellness/nutrition fields = None
  sc-19: raw.recovery missing athlete_id -> DLQ with original_topic="raw.recovery" +
         base64 original_value + error_type=VALIDATION_FAILURE
  sc-20: raw.recovery with event_time absent/null -> DLQ with error_type indicating
         validation failure
  sc-21: two raw.recovery messages with same event_id within 7d TTL -> only ONE
         canonical event emitted; duplicate is silently dropped
  sc-22: re-delivered event after TTL expiry -> second canonical event emitted; PG UPSERT
         idempotent (TTL not testable in bounded job; second message asserts emission;
         accepted limitation documented)
  sc-23: TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-recovery-wellness-event" (no Docker)
  sc-24: RECOVERY_SNAPSHOT + WELLNESS_DAILY for same (athlete_id, date) -> last-writer-wins
         (no error on concurrent UPSERT; decision #222)

This file mirrors tests/integration/test_cardio_canonicalize_job.py (the CORRECTED version).

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
        "testcontainers not installed — recovery canonicalize job integration test skipped. "
        "Install with: pip install testcontainers[kafka] to run sc-18..sc-24."
    ),
)

# --- Module-level gating: pyflink required ---------------------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); recovery_canonicalize_job integration test skipped",
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
_VALID_EVENT_ID = "revt-valid-001"
_INVALID_ATHLETE_EVENT_ID = "revt-no-athlete-002"
_NO_EVENT_TIME_EVENT_ID = "revt-no-time-003"
_DUP_EVENT_ID = "revt-dup-004"
_LWW_RECOVERY_EVENT_ID = "revt-lww-recovery-005"
# 2025-06-01 UTC midnight epoch-ms
_EVENT_TIME_MS = 1748736000000
_INGEST_TIME_MS = _EVENT_TIME_MS + 5_000

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 180
_CONSUME_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_recovery_envelope(
    event_id: str,
    athlete_id: str | None,
    sleep_hours: float | None = 7.0,
    resting_hr: int | None = 58,
    hrv: float | None = 42.0,
    steps: int | None = 8500,
    body_weight_kg: float | None = 72.3,
    event_time: int | None = _EVENT_TIME_MS,
) -> dict:
    """Build a raw.recovery envelope as the ingestion/recovery producer emits it."""
    envelope: dict = {
        "event_id": event_id,
        "ingest_time": _INGEST_TIME_MS,
        "source": "apple_health",
        "payload": {
            "sleep_hours": sleep_hours,
            "resting_hr": resting_hr,
            "hrv": hrv,
            "steps": steps,
            "body_weight_kg": body_weight_kg,
        },
    }
    if event_time is not None:
        envelope["event_time"] = event_time
    # event_time deliberately omitted when None -> ValidationError -> DLQ (sc-20)
    if athlete_id is not None:
        envelope["athlete_id"] = athlete_id
    # athlete_id deliberately omitted when None -> ValidationError -> DLQ (sc-19)
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
        "group.id": f"e2e-recovery-consume-{uuid.uuid4()}",
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
# Shared helper: run the bounded recovery job in a background thread
# ---------------------------------------------------------------------------


def _run_bounded_job(
    bootstrap_servers: str,
    schema_registry_url: str,
    raw_topic: str,
    canonical_topic: str,
    dlq_topic: str,
    run_id: str,
) -> None:
    """Probe connector JARs, run the bounded recovery canonicalize job."""
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

    from jobs.recovery_canonicalize.main import RecoveryCanonicalizeJobConfig, run

    config = RecoveryCanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        group_id=f"recovery-canonicalize-e2e-{run_id}",
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
            f"recovery canonicalize job did not terminate within "
            f"{_JOB_RUN_TIMEOUT_S}s (bounded source should drain)"
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")


# ---------------------------------------------------------------------------
# sc-18: valid raw.recovery → RECOVERY_SNAPSHOT in canonical.wellness_event
# ---------------------------------------------------------------------------


def test_sc18_valid_recovery_event_emitted_to_canonical(redpanda_endpoints):
    """sc-18: Valid raw.recovery with sleep_hours=7.0 → RECOVERY_SNAPSHOT in
    canonical.wellness_event; all wellness/nutrition fields = None."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.recovery.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_recovery_envelope(
        event_id=_VALID_EVENT_ID,
        athlete_id=_ATHLETE_ID,
        sleep_hours=7.0,
    )
    _produce_records(bootstrap, raw_topic, [json.dumps(envelope).encode("utf-8")])

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    assert len(canonical_msgs) >= 1, "expected at least one canonical RECOVERY_SNAPSHOT record"

    canonical_record, canonical_schema_id = _decode_confluent_avro(
        canonical_msgs[0].value(), registry_url
    )
    assert canonical_record["event_type"] == "RECOVERY_SNAPSHOT"
    assert canonical_record["athlete_id"] == _ATHLETE_ID
    assert canonical_record["schema_version"] == 1
    assert canonical_schema_id is not None and canonical_schema_id > 0
    # sleep_hours must be 7.0; nutrition/subjective fields must be None
    assert canonical_record["sleep_hours"] == pytest.approx(7.0)
    assert canonical_record["calories"] is None
    assert canonical_record["energy"] is None
    assert canonical_record["perceived_recovery"] is None

    # No DLQ records for a valid message
    dlq_msgs = _consume_available(bootstrap, dlq_topic, _CONSUME_TIMEOUT_S)
    assert len(dlq_msgs) == 0


# ---------------------------------------------------------------------------
# sc-19: missing athlete_id → DLQ with original_topic="raw.recovery"
# ---------------------------------------------------------------------------


def test_sc19_missing_athlete_id_goes_to_dlq(redpanda_endpoints):
    """sc-19: Missing athlete_id → DLQ with original_topic='raw.recovery',
    base64 original_value, error_type=VALIDATION_FAILURE."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.recovery.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_recovery_envelope(
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
# sc-20: missing event_time → DLQ with validation error_type
# ---------------------------------------------------------------------------


def test_sc20_missing_event_time_goes_to_dlq(redpanda_endpoints):
    """sc-20: Missing event_time → DLQ with error_type indicating validation failure."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.recovery.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    # event_time deliberately omitted
    envelope = _raw_recovery_envelope(
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
# sc-21: duplicate event_id deduplicated
# ---------------------------------------------------------------------------


def test_sc21_duplicate_event_id_deduplicated(redpanda_endpoints):
    """sc-21: Two raw.recovery messages with same event_id within 7d TTL →
    only ONE canonical event emitted; second is silently dropped."""
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.recovery.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    envelope = _raw_recovery_envelope(
        event_id=_DUP_EVENT_ID,
        athlete_id=_ATHLETE_ID,
        sleep_hours=8.0,
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
# sc-22: re-delivered event after TTL expiry → second emission (accepted limitation)
# ---------------------------------------------------------------------------


def test_sc22_post_ttl_redelivery_emits_second_canonical(redpanda_endpoints):
    """sc-22: Re-delivered event after 7d TTL expiry → second canonical event emitted.

    NOTE: The 7-day TTL is not practically testable in a bounded integration job
    (we cannot fast-forward Flink state TTL clock). This test validates the
    REDELIVERY path by sending two messages with DIFFERENT event_ids simulating
    the scenario where the first arrived before TTL and the second after TTL
    (both should produce canonical records since they are distinct events).

    The PG UPSERT on (athlete_id, metric_date) collapses re-delivered rows
    idempotently — no data corruption (decision #222, accepted limitation).

    Accepted limitation documented: TTL fast-forward not supported in bounded
    integration mode. This test proves the emission path; TTL enforcement is
    an operational concern verified by the ValueState configuration in main.py.
    """
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.recovery.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    # Two DISTINCT event_ids (simulates re-delivery after TTL expiry per the spec).
    # Both are valid and should produce canonical records.
    first_envelope = _raw_recovery_envelope(
        event_id="revt-sc22-first",
        athlete_id=_ATHLETE_ID,
        sleep_hours=7.5,
    )
    second_envelope = _raw_recovery_envelope(
        event_id="revt-sc22-second",
        athlete_id=_ATHLETE_ID,
        sleep_hours=7.5,
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
# sc-23: TRANSACTIONAL_ID_PREFIX literal (no Docker required — unit-level check)
# ---------------------------------------------------------------------------


def test_sc23_transactional_id_prefix_exact_literal_no_docker():
    """sc-23: TRANSACTIONAL_ID_PREFIX must equal the exact literal string
    'athleteos-canonicalize-recovery-wellness-event'.
    Does NOT require Docker — asserts the module-level constant directly."""
    from jobs.recovery_canonicalize.main import TRANSACTIONAL_ID_PREFIX

    assert TRANSACTIONAL_ID_PREFIX == "athleteos-canonicalize-recovery-wellness-event"
    # Verify disjoint from wellness prefix → no ProducerFencedException
    assert TRANSACTIONAL_ID_PREFIX != "athleteos-canonicalize-wellness-event"


# ---------------------------------------------------------------------------
# sc-24: RECOVERY_SNAPSHOT + WELLNESS_DAILY → last-writer-wins (no error)
# ---------------------------------------------------------------------------


def test_sc24_last_writer_wins_no_error(redpanda_endpoints):
    """sc-24: RECOVERY_SNAPSHOT and WELLNESS_DAILY for the same (athlete_id, date)
    → both UPSERT the same PG row; last-writer-wins; no error (decision #222).

    This test validates that the recovery job emits a RECOVERY_SNAPSHOT event
    without error when a wellness event exists for the same date. The actual
    PG UPSERT collision is handled at the jobs/wellness_metrics/ layer
    (unchanged — no modification per ADR-R3). The test confirms:
    1. The recovery job processes the message without DLQ routing.
    2. A RECOVERY_SNAPSHOT record is emitted to canonical.wellness_event.
    """
    bootstrap = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.recovery.e2e.{run_id}"
    canonical_topic = f"canonical.wellness_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.wellness_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap, [raw_topic, canonical_topic, dlq_topic])

    # Recovery event for the same date as a hypothetical wellness event
    envelope = _raw_recovery_envelope(
        event_id=_LWW_RECOVERY_EVENT_ID,
        athlete_id=_ATHLETE_ID,
        sleep_hours=7.2,
    )
    _produce_records(bootstrap, raw_topic, [json.dumps(envelope).encode("utf-8")])

    _run_bounded_job(bootstrap, registry_url, raw_topic, canonical_topic, dlq_topic, run_id)

    # Recovery event MUST emit without error (no DLQ) — last-writer-wins is accepted
    dlq_msgs = _consume_available(bootstrap, dlq_topic, _CONSUME_TIMEOUT_S)
    assert len(dlq_msgs) == 0, (
        f"last-writer-wins: no DLQ record expected for valid RECOVERY_SNAPSHOT, "
        f"got {len(dlq_msgs)} DLQ messages"
    )

    canonical_msgs = _consume_available(bootstrap, canonical_topic, _CONSUME_TIMEOUT_S)
    assert len(canonical_msgs) >= 1, "RECOVERY_SNAPSHOT must be emitted to canonical topic"
    canonical_record, _ = _decode_confluent_avro(canonical_msgs[0].value(), registry_url)
    assert canonical_record["event_type"] == "RECOVERY_SNAPSHOT"
    assert canonical_record["athlete_id"] == _ATHLETE_ID
