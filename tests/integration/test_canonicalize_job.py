"""Phase 4.3 integration (HIGHEST RISK): canonicalize job end-to-end against a
real Redpanda broker + Confluent-compatible Schema Registry, executed by a REAL
PyFlink runtime (apache-flink 1.19 on CPython 3.11).

This is the first time the canonicalize job actually RUNS. It exercises the full
PyFlink wiring from ``jobs.canonicalize.main.run``:

    KafkaSource(raw.strength, SimpleStringSchema-JSON)
      -> bounded out-of-orderness watermark + ISO->epoch-ms event-time assigner
      -> key_by(event_id)
      -> KeyedProcessFunction (dedup ValueState<bool> 7d TTL + transform/validate
         + yield canonical Row / yield DLQ side output)
      -> canonical Row DataStream
         -> StreamTableEnvironment.from_data_stream -> Kafka sink table
            ('value.format'='avro-confluent' against the Registry, RAW athlete_id
            key, EXACTLY_ONCE)  [Table API -- the only real PyFlink 1.19 path to
            the Confluent-Registry Avro serde]
      -> DLQ side output -> KafkaSink (JSON string, AT_LEAST_ONCE)

How the streaming job is made deterministic + terminating inside a test
-----------------------------------------------------------------------
A live streaming KafkaSource is unbounded, so ``env.execute()`` would block
forever. The job therefore honors a BOUNDED mode on ``CanonicalizeJobConfig``:
when ``bounded=True`` the ``KafkaSource`` is built with
``set_bounded(KafkaOffsetsInitializer.latest())`` so it reads from the earliest
offset up to the end offset captured at source startup, then sends MAX_WATERMARK
and finishes. The bounded streaming pipeline drains, the final checkpoint fires
on source-end (which commits the EXACTLY_ONCE KafkaSink transaction), and
``env.execute()`` returns -- a deterministic, self-terminating run.

The test produces the raw.strength records FIRST (producer.flush), THEN starts
the bounded job, so the "latest" end offset captured at startup covers all of
them. Checkpoint interval is short (2s) so the exactly-once transaction commits
promptly. Parallelism is pinned to 1 so the exactly-once KafkaSink runs a single
sink subtask (no per-subtask transaction-id bookkeeping to reason about).

Connector jars
--------------
PyFlink 1.19 ships the Flink core/table runtime JARs in ``pyflink/lib`` but does
NOT bundle the Kafka connector (``flink-connector-kafka``) nor the
``avro-confluent`` Table format (``flink-sql-avro-confluent-registry``) -- those
are external connector artifacts. ``env.add_jars(...)`` does NOT make them
resolvable in local execution because the py4j gateway JVM reads its classpath
from ``pyflink/lib`` at startup. So the test provision those three connector
JARs (download once from Maven Central into ``pyflink/lib`` if missing, cached
across runs). The fat ``flink-sql-avro-confluent-registry`` JAR bundles the
Confluent Schema Registry client + Avro deps; combined with
``flink-connector-kafka`` + ``kafka-clients`` it makes both the DataStream
KafkaSource/KafkaSink AND the Table API ``'connector'='kafka'`` +
``'value.format'='avro-confluent'`` paths resolvable.

Clean skips (never fake a pass)
-------------------------------
- No pyflink on this interpreter (e.g. CPython 3.14): module-level skip.
- No Docker daemon: redpanda fixture skip.
- Connector jars not loadable (no network to download + not pre-provisioned):
  runtime skip after a probe build of ``KafkaSource.builder()``.
Pure canonicalization logic is ALSO covered by unit tests
(``tests/unit/test_canonicalize_transform.py``), so these skips never silently
pass a broken transform contract.
"""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

# --- Module-level gating ----------------------------------------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); canonicalize-job integration test skipped",
        allow_module_level=True,
    )


# The three external connector JARs PyFlink 1.19 needs for the Kafka source/sink
# + the Confluent-Registry Avro Table format. Pinned to the artifacts that match
# apache-flink 1.19.1 (Flink kafka connector 3.3.0-1.19 targets Flink 1.19 and
# pulls kafka-clients 3.x; the SQL avro-confluent fat jar is built per Flink).
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


def _pyflink_lib_dir() -> Path:
    import pyflink

    return Path(pyflink.__file__).resolve().parent / "lib"


def _ensure_connector_jars() -> None:
    """Download the Kafka + avro-confluent connector JARs into pyflink/lib if
    missing so the py4j gateway JVM resolves them at startup (cached across
    runs; never re-downloads). No-op when already present. Network failure is
    swallowed here -- the runtime probe below skips the test cleanly if the
    jars still cannot load."""
    lib = _pyflink_lib_dir()
    lib.mkdir(parents=True, exist_ok=True)
    try:
        import requests
    except Exception:
        return  # requests is a dev dep; if absent we can't download -> rely on presence
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
            # Leave absent; the runtime probe will skip the test cleanly.
            pass


# Provision at import time, BEFORE any pyflink gateway JVM starts (so the first
# gateway start loads them). This is safe: pure file work + lazy `import requests`.
_ensure_connector_jars()


pytestmark = pytest.mark.integration


# --- Test-scoped constants ---------------------------------------------------

_VALID_EVENT_ID = "evt-valid-001"
_INVALID_EVENT_ID = "evt-invalid-002"
_ATHLETE_ID = "athlete-123"
_VALID_EVENT_TIME_ISO = "2024-01-15T10:30:00"

# Short checkpoint interval so the exactly-once KafkaSink transaction commits
# quickly; bounded run with parallelism 1 for determinism.
_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 180
_CONSUME_TIMEOUT_S = 60


# --- Helpers -----------------------------------------------------------------


def _raw_envelope(event_id: str, payload: dict, event_time_iso: str) -> dict:
    return {
        "event_id": event_id,
        "event_time": event_time_iso,
        "ingest_time": "2024-01-15T10:31:00",
        "source": "strong_csv",
        "athlete_id": _ATHLETE_ID,
        "payload": payload,
    }


def _valid_payload() -> dict:
    # session_load = reps*weight_kg*(rpe/10) = 8*100*8.5/10 = 680.0 (spec scenario)
    return {
        "workout_id": "w-001",
        "exercise_id": "bench-press",
        "set_number": 1,
        "reps": 8,
        "weight_kg": 100.0,
        "rpe": 8.5,
        "rir": 2,
        "timestamp": _VALID_EVENT_TIME_ISO,
    }


def _invalid_payload_missing_reps() -> dict:
    # Missing required `reps` -> ValidationError -> DLQ (VALIDATION_FAILURE)
    payload = _valid_payload()
    payload.pop("reps")
    payload["set_number"] = 2
    return payload


def _create_topics(bootstrap_servers: str, topics: list[str]) -> None:
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
        for t, f in fs.items():
            f.result()  # raises on failure


def _normalize_ms(value) -> int | None:
    """fastavro decodes Avro logicalType 'timestamp-millis' as a UTC datetime;
    convert back to epoch-ms for comparison with the spec's long epoch-ms."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return int(value)


def test_canonicalize_job_e2e_valid_dlq_dedup(redpanda_endpoints):
    """REAL end-to-end: valid event -> canonical Avro via the Confluent Registry,
    invalid event -> DLQ JSON, duplicate event_id dropped by ValueState dedup."""
    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    # --- Runtime probe: are the Kafka connector jars actually loadable? -----
    # (Triggers the py4j gateway JVM, which loads pyflink/lib at startup. If the
    # jars are absent + could not be provisioned, skip cleanly -- never fake it.)
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaSource

        env_probe = StreamExecutionEnvironment.get_execution_environment()
        KafkaSource.builder()  # resolves the Java builder class; raises if jar absent
        del env_probe
    except TypeError as exc:
        pytest.skip(
            "Kafka connector JARs not loadable by the pyflink gateway "
            "(flink-connector-kafka / kafka-clients / flink-sql-avro-confluent"
            "-registry must be present in pyflink/lib); "
            f"underlying error: {exc}"
        )

    # --- Isolated, per-run topic names (avoid cross-test partition/data clash) -
    run_id = uuid.uuid4().hex[:8]
    raw_topic = f"raw.strength.e2e.{run_id}"
    canonical_topic = f"canonical.training_event.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.training_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    # --- Subject compatibility: BACKWARD (contracts future versions) ---------
    #
    # RUNTIME-VERIFIED BUG (this real run, not the static review):
    # The canonical sink CANNOT rehearse ``schemas/canonical/TrainingEvent.avsc``
    # as its writers schema through Flink 1.19's ``avro-confluent`` Table format
    # because the Table API infers the Avro record schema from the sink's DDL
    # column types (Context7 confirmed -- there is NO ``value.avro-confluent
    # .schema`` option to supply a writers schema explicitly). Flink's Table
    # type system has NO Avro ``enum`` type: the DDL column ``event_type STRING``
    # generates Avro ``{"type":"string"}``, which is INCOMPATIBLE under BACKWARD
    # with the spec avsc's enum ``{"type":"enum", "name":"TrainingEventType",
    # "symbols":[...]}`` (Avro forbids enum<->string promotion -> SR returns
    # ``error code: 409 Schema being registered is incompatible with an earlier
    # schema for subject {...-value}``).
    #
    # Pre-registering the spec avsc against the live sink subject therefore
    # fatally broke the very first Flink sink emission. The fix is the test
    # route endorsed in PR3's apply brief: let the Flink avro-confluent sink
    # OWN the live writers schema for the per-test subject (first registration
    # has no prior schema -> compatibility check vacuously passes; BACKWARD is
    # still SET on the subject so future versions honor the production
    # contract). The design contract itself stays verified in
    # ``tests/unit/test_canonicalize_transform.py`` (validate_training_event
    # against the avsc) and by ``bootstrap.register_schemas`` in the deploy
    # pipeline; this integration test verifies the RUNTIME sink path against
    # the schema Flink actually emits.
    #
    # FOLLOW-UP (out of scope here -- do NOT redesign the topology minimally):
    # either relax ``TrainingEvent.avsc`` to ``event_type STRING`` (validate
    # the symbol set explicitly in the transform), or wrap the canonical sink
    # in a TypeInformation-driven DataStream Avro serializer that emits the
    # enum. Recorded against PR3 apply-progress; tracked as a known design
    # divergence surfaced by the real runtime.
    from bootstrap.register_schemas import set_compatibility

    set_compatibility(registry_url, subject, "BACKWARD")
    # ``schema_version`` here is the canonical TrainingEvent contract version
    # emitted as an INT field of the Avro record (avsc PR1 `schema_version`
    # REQUIRED). It does NOT come from the per-test Schema Registry subject
    # version -- see the divergence note below for why we no longer
    # pre-register ``TrainingEvent.avsc`` against the live sink subject.
    schema_version = 1

    # --- Create the three topics (1 partition keeps the test fast + single -
    # subtask exactly-once sink; the 8-partition contract is covered by
    # tests/integration/test_topics.py). ----------------------------------------
    _create_topics(bootstrap_servers, [raw_topic, canonical_topic, dlq_topic])

    # --- Produce the raw.strength envelopes (valid, DUPLICATE, invalid) ------
    valid_env = _raw_envelope(_VALID_EVENT_ID, _valid_payload(), _VALID_EVENT_TIME_ISO)
    dup_env = _raw_envelope(_VALID_EVENT_ID, _valid_payload(), _VALID_EVENT_TIME_ISO)
    invalid_env = _raw_envelope(
        _INVALID_EVENT_ID,
        _invalid_payload_missing_reps(),
        "2024-01-15T10:40:00",
    )
    produce_records(
        bootstrap_servers,
        raw_topic,
        [
            json.dumps(valid_env).encode("utf-8"),
            json.dumps(dup_env).encode("utf-8"),
            json.dumps(invalid_env).encode("utf-8"),
        ],
    )

    # --- Run the REAL canonicalize job (bounded -> self-terminating) ---------
    from jobs.canonicalize.main import CanonicalizeJobConfig, run

    config = CanonicalizeJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"canonicalize-e2e-{run_id}",
        raw_topic=raw_topic,
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        schema_version=schema_version,
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
            f"canonicalize job did not terminate within {_JOB_RUN_TIMEOUT_S}s "
            "(bounded source should drain + final checkpoint should commit and "
            "env.execute() should return)"  # leave thread as daemon; process exits
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")

    # --- Consume canonical.training_event (EXACTLY_ONCE -> read_committed) ---
    canonical_msgs = consume_exact(
        bootstrap_servers, canonical_topic, n=None, timeout=_CONSUME_TIMEOUT_S
    )
    assert len(canonical_msgs) == 1, (
        f"expected EXACTLY ONE canonical record (the duplicate must be deduped), "
        f"got {len(canonical_msgs)} -> {canonical_msgs}"
    )
    canonical_record, canonical_schema_id = decode_confluent_avro(
        canonical_msgs[0].value(), registry_url
    )

    # record key == athlete_id (RAW key, co-partitioning per design)
    key = canonical_msgs[0].key()
    assert key is not None, "canonical record must carry the athlete_id key"
    assert key.decode("utf-8") == _ATHLETE_ID, f"unexpected canonical key: {key!r}"

    # spec event-contracts: epoch-ms event_time, schema_version present,
    # event_type=STRENGTH_SET, session_load=680.0 (8*100*8.5/10, spec scenario).
    assert canonical_record["event_id"] == _VALID_EVENT_ID
    assert canonical_record["athlete_id"] == _ATHLETE_ID
    assert canonical_record["event_type"] == "STRENGTH_SET"
    assert canonical_record["schema_version"] == schema_version
    assert canonical_schema_id is not None and canonical_schema_id > 0

    from jobs.canonicalize.transform import parse_iso_to_epoch_ms

    expected_event_time_ms = parse_iso_to_epoch_ms(_VALID_EVENT_TIME_ISO)
    assert _normalize_ms(canonical_record["event_time"]) == expected_event_time_ms, (
        f"event_time not epoch-ms long: {canonical_record['event_time']!r}"
    )
    assert abs(float(canonical_record["session_load"]) - 680.0) < 0.01, (
        f"session_load != 680.0 (reps*weight_kg*(rpe/10)): {canonical_record['session_load']!r}"
    )
    # strength-sourced event: cardio fields null
    assert canonical_record["activity_type"] is None
    assert canonical_record["distance_km"] is None

    # --- Consume dlq.canonical.training_event (AT_LEAST_ONCE) ----------------
    dlq_msgs = consume_exact(
        bootstrap_servers, dlq_topic, n=None, timeout=_CONSUME_TIMEOUT_S
    )
    assert len(dlq_msgs) >= 1, (
        f"expected the INVALID event routed to DLQ, got {len(dlq_msgs)} messages"
    )
    dlq_record = json.loads(dlq_msgs[0].value().decode("utf-8"))

    assert dlq_record["original_topic"] == raw_topic
    assert dlq_record["error_type"] == "VALIDATION_FAILURE", (
        f"missing required `reps` -> VALIDATION_FAILURE, got {dlq_record.get('error_type')}"
    )
    assert dlq_record["error_message"], "DLQ envelope must carry an error_message"
    # original_value is base64-encoded-bytes of the raw JSON envelope per spec
    decoded_original = base64.b64decode(dlq_record["original_value"]).decode("utf-8")
    assert json.loads(decoded_original)["event_id"] == _INVALID_EVENT_ID
    assert isinstance(dlq_record["timestamp"], int) and dlq_record["timestamp"] > 0

    # --- Dedup proof: the duplicate event_id produced no second canonical ----
    # (already asserted via the exactly-one canonical record above; spell it out)
    canonical_event_ids = [canonical_record["event_id"]]
    assert canonical_event_ids == [_VALID_EVENT_ID], (
        "duplicate event_id must be dropped by ValueState dedup; only the first "
        "occurrence should reach canonical.training_event"
    )


# --- HTTP / Kafka IO helpers -------------------------------------------------


def requests_get(base: str, path: str) -> dict:
    import requests

    resp = requests.get(base + path, timeout=10)
    resp.raise_for_status()
    return resp.json()


def produce_records(bootstrap_servers: str, topic: str, values: list[bytes]) -> None:
    """Publish each value-bytes (keyed by athlete_id) to the topic, then flush."""
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


def consume_exact(bootstrap_servers: str, topic: str, n: int | None, timeout: float):
    """Consume ALL committed messages up to ``timeout`` seconds (``n=None`` means
    poll until quiet for a while after the first message, then return what we
    have).``isolation.level=read_committed`` so exactly-once transactions are
    only visible after the transactional commit finishes."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"e2e-consume-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            # librdkafka property name (NOT "fetch.max.wait.ms"; the prior name
            # caused KafkaError _INVALID_ARG "No such configuration property").
            "fetch.wait.max.ms": "500",
        }
    )
    consumer.subscribe([topic])
    messages = []
    deadline = time.time() + timeout
    quiet_since = None
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if messages:
                    quiet_since = quiet_since or time.time()
                    if time.time() - quiet_since > 4.0:
                        break  # broker quiet for 4s after we saw something -> done
                continue
            if msg.error():
                continue
            messages.append(msg)
        return messages
    finally:
        consumer.close()


def decode_confluent_avro(raw: bytes, registry_url: str) -> tuple[dict, int]:
    """Decode a Confluent-wire Avro message (magic byte 0 + 4-byte big-endian
    schema-id + Avro payload) against the schema fetched from the Registry."""
    from fastavro import schemaless_reader

    assert raw[0] == 0, f"expected Confluent magic byte 0, got {raw[0]}"
    schema_id = int.from_bytes(raw[1:5], "big")
    schema_doc = requests_get(registry_url, f"/schemas/ids/{schema_id}")
    schema = json.loads(schema_doc["schema"])
    record = schemaless_reader(io.BytesIO(raw[5:]), schema)
    return record, schema_id