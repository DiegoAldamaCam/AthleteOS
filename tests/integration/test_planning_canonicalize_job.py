"""Phase PL2b integration: raw.planning → canonical.planning_block Avro + DLQ.

End-to-end test of the planning_canonicalize bounded Flink job:

    KafkaSource(raw.planning, SimpleStringSchema-JSON)
      -> bounded watermark + epoch-ms event-time assigner
      -> key_by(athlete_id)
      -> PlanningCanonicalizeProcessFunction
           (event_id MapState<event_id, bool> 7d TTL — NO block_id state per ADR-20
            + transform + validate → yield canonical Row | yield DLQ side output)
      -> canonical Row → avro-confluent Table sink → canonical.planning_block
      -> DLQ → KafkaSink(JSON, AT_LEAST_ONCE) → dlq.canonical.planning_block

Spec scenarios covered:
  PL2-1: two events for same (athlete_id, block_id) with DIFFERENT event_id and
          ingest_time → TWO rows in canonical.planning_block (versioning, not dedup)
   PL2-2: two events with the SAME event_id → only ONE canonical event emitted
          (event_id dedup MapState<event_id, bool> 7d TTL)
  PL2-3: two blocks with overlapping date ranges same athlete → BOTH emitted
          (no overlap validation per BR-2 / ADR-20)
  PL2-9: first schema registration under canonical.planning_block-value succeeds
          (greenfield subject, TopicNameStrategy ADR-10)
  PL2-13: existing test suite unaffected (structural isolation verified)

Mirrors tests/integration/test_wellness_canonicalize_job.py structure exactly.

Clean skips (never fake a pass):
  - No pyflink on this interpreter (CPython 3.14): module-level skip.
  - No Docker daemon: redpanda fixture skip.
  - Connector JARs not loadable: runtime skip after probe build.
"""

from __future__ import annotations

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
        "(no CPython 3.12+ wheel); planning canonicalize job integration test skipped",
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
# 2025-06-01 UTC midnight epoch-ms
_START_DATE_1_MS = 1748736000000
# 2025-08-31 UTC midnight epoch-ms
_END_DATE_1_MS = 1756598400000
# 2025-07-01 UTC midnight epoch-ms
_START_DATE_2_MS = 1751328000000
# 2025-09-30 UTC midnight epoch-ms
_END_DATE_2_MS = 1759190400000

_INGEST_TIME_A = 1748740000000
_INGEST_TIME_B = 1748800000000

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 180
_CONSUME_TIMEOUT_S = 60


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_planning_envelope(
    *,
    event_id: str,
    athlete_id: str = _ATHLETE_ID,
    block_id: str = "BLK-001",
    goal: str = "Build aerobic base",
    start_date: str = "2025-06-01",
    end_date: str = "2025-08-31",
    planned_sessions_per_week: int = 5,
    weekly_volume_targets: str = '{"strength": 3, "cardio": 2}',
    ingest_time: int = _INGEST_TIME_A,
    source: str = "planning_connector",
) -> dict:
    """Build a raw.planning JSON envelope matching what the P1 producer emits."""
    return {
        "event_id": event_id,
        "event_time": _START_DATE_1_MS,
        "ingest_time": ingest_time,
        "source": source,
        "athlete_id": athlete_id,
        "block_id": block_id,
        "goal": goal,
        "start_date": start_date,
        "end_date": end_date,
        "planned_sessions_per_week": planned_sessions_per_week,
        "weekly_volume_targets": weekly_volume_targets,
    }


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


def _produce_records(bootstrap_servers: str, topic: str, records: list) -> None:
    """Produce (key, value) tuples to a topic. value must be bytes."""
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _on_err(err, _msg):
        if err is not None:
            raise RuntimeError(f"kafka produce failed: {err}")

    for key, value in records:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8") if isinstance(key, str) else key,
            value=value,
            callback=_on_err,
        )
    producer.flush(timeout=30)


def _consume_available(bootstrap_servers: str, topic: str, timeout: float) -> list:
    """Consume all available committed messages up to timeout seconds."""
    from confluent_kafka import Consumer
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"e2e-planning-consume-{uuid.uuid4()}",
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


def _probe_kafka_connector() -> None:
    """Skip the test if Kafka connector JARs aren't loadable by the Flink gateway."""
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


def _run_planning_job(config) -> None:
    """Run the bounded planning canonicalize job in a thread and assert clean finish."""
    from jobs.planning_canonicalize.main import run

    outcome: dict = {}

    def _target():
        try:
            run(config)
            outcome["done"] = True
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    worker = threading.Thread(target=_target, daemon=True)
    worker.start()
    worker.join(timeout=_JOB_RUN_TIMEOUT_S)
    if worker.is_alive():
        pytest.fail(
            f"planning canonicalize job did not terminate within "
            f"{_JOB_RUN_TIMEOUT_S}s (bounded source should drain)"
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")


# ---------------------------------------------------------------------------
# PL2-9: Schema registration (greenfield subject)
# ---------------------------------------------------------------------------


def test_planning_block_schema_registration_safe(redpanda_endpoints):
    """PL2-9: First schema registration under canonical.planning_block-value succeeds.

    No prior schema registered → registration must succeed without HTTP 409 error.
    The schema must be retrievable under the registered subject.
    """
    _probe_kafka_connector()

    registry_url = redpanda_endpoints["schema_registry_url"]
    run_id = uuid.uuid4().hex[:8]
    subject = f"canonical.planning_block.e2e.{run_id}-value"

    # Register PlanningBlock.avsc directly via Schema Registry REST API
    from pathlib import Path
    import requests

    schema_path = Path(__file__).resolve().parents[2] / "schemas" / "canonical" / "PlanningBlock.avsc"
    schema_str = schema_path.read_text(encoding="utf-8")

    resp = requests.post(
        f"{registry_url}/subjects/{subject}/versions",
        json={"schema": schema_str},
        timeout=30,
    )
    assert resp.status_code in (200, 201), (
        f"PL2-9: Schema registration failed with {resp.status_code}: {resp.text}"
    )
    schema_id = resp.json().get("id")
    assert isinstance(schema_id, int) and schema_id > 0, (
        f"PL2-9: Expected numeric schema_id > 0, got: {resp.json()!r}"
    )

    # Retrieve and verify
    retrieve = requests.get(
        f"{registry_url}/subjects/{subject}/versions/latest",
        timeout=10,
    )
    assert retrieve.status_code == 200, (
        f"PL2-9: Could not retrieve schema under {subject}: {retrieve.text}"
    )
    registered = retrieve.json()
    assert registered["id"] == schema_id
    assert "PlanningBlock" in registered["schema"]


# ---------------------------------------------------------------------------
# PL2-1 + PL2-2 + PL2-3: Bounded job end-to-end
# ---------------------------------------------------------------------------


def test_planning_canonicalize_versioning_and_dedup(redpanda_endpoints):
    """PL2-1: Same (athlete_id, block_id), different event_id → TWO canonical events.
    PL2-2: Same event_id sent twice → only ONE canonical event emitted (dedup).
    """
    _probe_kafka_connector()

    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.planning.e2e.{run_id}"
    canonical_topic = f"canonical.planning_block.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.planning_block.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap_servers, [raw_topic, canonical_topic, dlq_topic])

    # PL2-1: Two events — same (athlete_id, block_id), DIFFERENT event_id + ingest_time.
    #        Expect: TWO canonical outputs (no block_id dedup per ADR-20).
    env_revision_1 = _raw_planning_envelope(
        event_id="plan-evt-rev1",
        block_id="BLK-001",
        ingest_time=_INGEST_TIME_A,
    )
    env_revision_2 = _raw_planning_envelope(
        event_id="plan-evt-rev2",
        block_id="BLK-001",
        ingest_time=_INGEST_TIME_B,
    )
    # PL2-2: Exact duplicate of env_revision_1 (same event_id → must be deduped).
    #        Expect: NOT emitted — dedup via event_id MapState.
    env_dup = _raw_planning_envelope(
        event_id="plan-evt-rev1",  # same event_id as revision 1
        block_id="BLK-001",
        ingest_time=_INGEST_TIME_A,
    )

    _produce_records(
        bootstrap_servers,
        raw_topic,
        [
            (_ATHLETE_ID, json.dumps(env_revision_1).encode("utf-8")),
            (_ATHLETE_ID, json.dumps(env_revision_2).encode("utf-8")),
            (_ATHLETE_ID, json.dumps(env_dup).encode("utf-8")),
        ],
    )

    from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

    config = PlanningCanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"planning-canonicalize-e2e-{run_id}",
        raw_topic=raw_topic,
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        schema_version=1,
        bounded=True,
        parallelism=1,
        no_restart=True,
    )

    _run_planning_job(config)

    # PL2-1: Exactly TWO canonical records (both revisions emitted, dup dropped).
    canonical_msgs = _consume_available(
        bootstrap_servers, canonical_topic, _CONSUME_TIMEOUT_S
    )
    assert len(canonical_msgs) == 2, (
        f"PL2-1: Expected 2 canonical records (two different event_ids for same block_id), "
        f"got {len(canonical_msgs)}. "
        f"ADR-20: repeat block_id = new revision, must NOT be deduplicated."
    )

    event_ids = set()
    for msg in canonical_msgs:
        record, schema_id = _decode_confluent_avro(msg.value(), registry_url)
        assert record["athlete_id"] == _ATHLETE_ID
        assert record["block_id"] == "BLK-001"
        assert schema_id is not None and schema_id > 0
        event_ids.add(record["event_id"])

    # PL2-2: The duplicate (same event_id as rev1) must NOT appear — only 2 distinct.
    assert "plan-evt-rev1" in event_ids, (
        "PL2-2: event_id 'plan-evt-rev1' must be in canonical output (first occurrence)"
    )
    assert "plan-evt-rev2" in event_ids, (
        "PL2-1: event_id 'plan-evt-rev2' (second revision) must be in canonical output"
    )
    # The dedup proof: we produced 3 messages but only 2 came out.
    # event_ids has exactly 2 entries → the third (dup event_id) was dropped.
    assert len(event_ids) == 2, (
        "PL2-2: Exactly 2 unique event_ids expected — duplicate event_id must be dropped"
    )

    # DLQ must be empty — all three inputs were valid (one is just a dup, silently dropped).
    dlq_msgs = _consume_available(bootstrap_servers, dlq_topic, 10)
    assert len(dlq_msgs) == 0, (
        f"PL2-1/PL2-2: DLQ must be empty (no invalid records), got {len(dlq_msgs)}"
    )


def test_planning_canonicalize_overlapping_blocks_accepted(redpanda_endpoints):
    """PL2-3: Two blocks with overlapping date ranges for same athlete → BOTH emitted.

    Per BR-2 / ADR-20: overlap validation is explicitly NOT performed.
    Both must appear in canonical.planning_block with no DLQ routing.
    """
    _probe_kafka_connector()

    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.planning.e2e.{run_id}"
    canonical_topic = f"canonical.planning_block.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.planning_block.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap_servers, [raw_topic, canonical_topic, dlq_topic])

    # Block A: 2025-06-01 → 2025-08-31
    env_block_a = _raw_planning_envelope(
        event_id="plan-evt-overlap-a",
        block_id="BLK-OVERLAP-A",
        start_date="2025-06-01",
        end_date="2025-08-31",
        ingest_time=_INGEST_TIME_A,
    )
    # Block B: 2025-07-01 → 2025-09-30 (overlaps block A — valid per BR-2)
    env_block_b = _raw_planning_envelope(
        event_id="plan-evt-overlap-b",
        block_id="BLK-OVERLAP-B",
        start_date="2025-07-01",
        end_date="2025-09-30",
        ingest_time=_INGEST_TIME_B,
    )

    _produce_records(
        bootstrap_servers,
        raw_topic,
        [
            (_ATHLETE_ID, json.dumps(env_block_a).encode("utf-8")),
            (_ATHLETE_ID, json.dumps(env_block_b).encode("utf-8")),
        ],
    )

    from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

    config = PlanningCanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"planning-overlap-e2e-{run_id}",
        raw_topic=raw_topic,
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        schema_version=1,
        bounded=True,
        parallelism=1,
        no_restart=True,
    )

    _run_planning_job(config)

    # PL2-3: Both blocks must appear in canonical output.
    canonical_msgs = _consume_available(
        bootstrap_servers, canonical_topic, _CONSUME_TIMEOUT_S
    )
    assert len(canonical_msgs) == 2, (
        f"PL2-3: Expected 2 canonical records (overlapping blocks both accepted), "
        f"got {len(canonical_msgs)}. "
        f"ADR-20/BR-2: overlap between planning blocks is VALID by design."
    )

    block_ids = set()
    for msg in canonical_msgs:
        record, _ = _decode_confluent_avro(msg.value(), registry_url)
        assert record["athlete_id"] == _ATHLETE_ID
        block_ids.add(record["block_id"])

    assert "BLK-OVERLAP-A" in block_ids, (
        "PL2-3: block A (2025-06-01 → 2025-08-31) must be in canonical output"
    )
    assert "BLK-OVERLAP-B" in block_ids, (
        "PL2-3: block B (2025-07-01 → 2025-09-30, overlaps A) must be in canonical output"
    )

    # DLQ must be empty — no validation rejection for overlaps.
    dlq_msgs = _consume_available(bootstrap_servers, dlq_topic, 10)
    assert len(dlq_msgs) == 0, (
        f"PL2-3: DLQ must be empty (overlap is valid, not a DLQ condition), "
        f"got {len(dlq_msgs)}"
    )


def test_planning_canonicalize_invalid_record_to_dlq(redpanda_endpoints):
    """PL2-3 (DLQ path): Invalid planning record → routed to DLQ.

    Verifies the DLQ path is wired correctly: a record that fails validation
    (end_date < start_date) must be routed to dlq.canonical.planning_block
    with error_type=VALIDATION_FAILURE and base64-encoded original_value.
    """
    _probe_kafka_connector()

    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.planning.e2e.{run_id}"
    canonical_topic = f"canonical.planning_block.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.planning_block.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility
    set_compatibility(registry_url, subject, "BACKWARD")

    _create_topics(bootstrap_servers, [raw_topic, canonical_topic, dlq_topic])

    # Valid record (must produce canonical output)
    env_valid = _raw_planning_envelope(
        event_id="plan-evt-valid-dlq-test",
        block_id="BLK-VALID",
    )
    # Invalid record: end_date < start_date (PL2-6 validation)
    env_invalid = _raw_planning_envelope(
        event_id="plan-evt-invalid-dates",
        block_id="BLK-INVALID",
        start_date="2025-08-31",
        end_date="2025-06-01",  # end before start → ValidationError
    )

    _produce_records(
        bootstrap_servers,
        raw_topic,
        [
            (_ATHLETE_ID, json.dumps(env_valid).encode("utf-8")),
            (_ATHLETE_ID, json.dumps(env_invalid).encode("utf-8")),
        ],
    )

    from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

    config = PlanningCanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"planning-dlq-e2e-{run_id}",
        raw_topic=raw_topic,
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        schema_version=1,
        bounded=True,
        parallelism=1,
        no_restart=True,
    )

    _run_planning_job(config)

    # Canonical: exactly ONE record (the valid one)
    canonical_msgs = _consume_available(
        bootstrap_servers, canonical_topic, _CONSUME_TIMEOUT_S
    )
    assert len(canonical_msgs) == 1, (
        f"Expected exactly 1 canonical record (invalid one goes to DLQ), "
        f"got {len(canonical_msgs)}"
    )
    record, _ = _decode_confluent_avro(canonical_msgs[0].value(), registry_url)
    assert record["event_id"] == "plan-evt-valid-dlq-test"

    # DLQ: exactly ONE record (the invalid dates record)
    dlq_msgs = _consume_available(
        bootstrap_servers, dlq_topic, _CONSUME_TIMEOUT_S
    )
    assert len(dlq_msgs) == 1, (
        f"Expected exactly 1 DLQ record (end_date < start_date), "
        f"got {len(dlq_msgs)}"
    )
    import base64
    dlq_record = json.loads(dlq_msgs[0].value().decode("utf-8"))
    assert dlq_record["error_type"] == "VALIDATION_FAILURE", (
        f"DLQ record must carry error_type=VALIDATION_FAILURE, "
        f"got {dlq_record.get('error_type')!r}"
    )
    assert dlq_record["error_message"], "DLQ envelope must carry an error_message"
    decoded_original = base64.b64decode(dlq_record["original_value"]).decode("utf-8")
    assert json.loads(decoded_original)["event_id"] == "plan-evt-invalid-dates"
    assert isinstance(dlq_record["timestamp"], int) and dlq_record["timestamp"] > 0
