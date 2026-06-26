"""Phase 5.5 integration (HIGHEST RISK): metrics job end-to-end against a real
Redpanda broker + Confluent-compatible Schema Registry, executed by a REAL
PyFlink runtime (apache-flink 1.19 on CPython 3.11).

This is the first time the metrics job actually RUNS. It exercises the full
PyFlink wiring from ``jobs.metrics.main.run``:

    Table source(canonical.training_event, avro-confluent, bounded=latest-offset)
      -> to_data_stream -> assign_timestamps_and_watermarks(24h, event_time)
      -> key_by(event_id) -> DedupAndGuardFunction
           (ValueState<bool> 7d TTL + NaN guard -> DLQ VALIDATION_FAILURE)
      -> key_by(athlete_id) -> TumblingEventTimeWindows(1d)
           .trigger(ContinuousEventTimeTrigger.of(1d))
           .allowed_lateness(24h) .side_output_late_data(late.daily)
           .aggregate(SumSessionLoadAgg, DailyLoadWindowFn)  -> daily_load
      -> key_by(athlete_id) -> SlidingEventTimeWindows(42d, slide 1d)
           .allowed_lateness(24h) .side_output_late_data(late.rolling)
           .process(RollingMetricsWindowFn)  -> acute/chronic_28d/chronic_42d/ACR
      -> key_by(athlete_id) -> DeloadKeyedProcessFunction  -> deload_flag
      -> map -> JSON -> KafkaSink(metrics output, AT_LEAST_ONCE)
    DLQ (NaN + late) -> union -> KafkaSink(dlq.canonical.training_event)

How the streaming job is made deterministic + terminating inside a test
-----------------------------------------------------------------------
The Table Kafka source is declared with ``scan.bounded.mode = 'latest-offset'``
so it reads from the earliest offset up to the end offset captured at source
startup, then finishes -> MAX_WATERMARK fires all event-time windows ->
``env.execute()`` returns. The test produces the canonical events FIRST, THEN
starts the bounded job. Checkpoint interval is short (2s) so the AT_LEAST_ONCE
KafkaSinks flush promptly. Parallelism = 1 + no_restart for determinism.

How late-DLQ routing is tested reliably (C4 fix)
------------------------------------------------
The original test produced a "late" event with event_time=day-1 alongside
in-window events for days 1-12. In bounded mode with a 24h watermark, the
watermark would advance to day-12 - 24h = day-11, and the day-1 window (closed
at day-2) + 24h allowed lateness = day-3. A day-1 event READ BEFORE the
watermark reaches day-3 would be processed in-window, NOT late.

The correct approach (C4 structural fix): produce events for day 1 through day
N_NORMAL_DAYS (>= 3), where N_NORMAL_DAYS + 1 - 24h > day-1 + 24h (i.e.
N_NORMAL_DAYS >= 3 guarantees the watermark driven by day-N_NORMAL_DAYS events
has advanced past day-1's window_end + 24h allowed lateness). Produce those
in ascending event_time order, THEN produce the late event at the END of the
topic. In bounded mode the records are consumed in topic-offset order; the late
event's event_time (day-1) will arrive AFTER the watermark has advanced past
day-1 + 24h, so Flink's native late-data path fires -> side_output_late_data ->
DLQ (LATE_DATA). The assertion is now structurally sound: late -> DLQ because
the watermark genuinely passed the window.

N_NORMAL_DAYS is set to 4: events for days 1-4 drive the watermark to
day-4 - 24h = day-3. Day-1's window ends at day-2; day-2 + 24h = day-3.
The late event (event_time = day-1) arrives last, AFTER day-3 is in the
watermark, so it is past window_end + allowed_lateness -> routed to DLQ.

Producing canonical Avro without the confluent_kafka AvroSerializer
-------------------------------------------------------------------
The confluent_kafka.schema_registry client pulls in ``httpx`` (not installed).
We instead register the TrainingEvent schema via ``requests`` (the same path
``bootstrap.register_schemas`` uses) and encode the Confluent wire format
manually with ``fastavro`` (magic byte 0 + 4-byte big-endian schema-id +
schemaless Avro payload) -- the inverse of the canonicalize test's
``decode_confluent_avro``. The producer schema uses plain ``long`` for
event_time/ingest_time (matching the canonicalize sink's DDL-inferred wire
schema and the metrics source DDL ``event_time BIGINT``).

Clean skips (never fake a pass)
-------------------------------
- No pyflink (e.g. CPython 3.14): module-level skip.
- No Docker: redpanda fixture skip.
- Connector jars not loadable: runtime skip after a probe.
Pure metric math is ALSO covered by unit tests
(``tests/unit/test_metrics_compute.py``), so these skips never silently pass a
broken formula contract.
"""

from __future__ import annotations

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
        "(no CPython 3.12+ wheel); metrics-job integration test skipped",
        allow_module_level=True,
    )


# The three external connector JARs PyFlink 1.19 needs for the Kafka Table
# source (avro-confluent) + the DataStream KafkaSink. Same set as the
# canonicalize integration test.
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


# Provision at import time, BEFORE any pyflink gateway JVM starts.
_ensure_connector_jars()

pytestmark = pytest.mark.integration


# --- Test-scoped constants --------------------------------------------------

_ATHLETE_ID = "athlete-m1"
# Base day = 2024-01-01 00:00 UTC (a Kafka/event-time day boundary, UTC).
_BASE_DT = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
_BASE_MS = int(_BASE_DT.timestamp() * 1000)
# Import the canonical MILLIS_PER_DAY from compute.py (single source of truth).
# (WARNING W2: avoid duplicating the constant in 3 places)
from jobs.metrics.compute import MILLIS_PER_DAY as _MS_PER_DAY  # noqa: E402

# Number of in-window normal days (days 1..N_NORMAL_DAYS, ascending event_time).
# Must be >= 3 so the watermark driven by day-N events passes day-1's
# window_end (BASE_MS + 1d) + 24h allowed lateness (= BASE_MS + 2d).
# With N=4: watermark ~ BASE_MS + 4d - 24h = BASE_MS + 3d > BASE_MS + 2d. (C4)
_N_NORMAL_DAYS = 12  # keep 12 days to preserve ACR/deload assertions below

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 240
_CONSUME_TIMEOUT_S = 60


# --- Topic / schema helpers -------------------------------------------------


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
            f.result()


def _training_event_producer_schema() -> dict:
    """TrainingEvent.avsc with event_time/ingest_time as plain ``long`` (no
    logicalType), matching the canonicalize sink's DDL-inferred wire schema and
    the metrics source DDL ``event_time BIGINT``."""
    avsc_path = (
        Path(__file__).resolve().parents[2]
        / "schemas" / "canonical" / "TrainingEvent.avsc"
    )
    schema = json.loads(avsc_path.read_text(encoding="utf-8"))
    for field in schema["fields"]:
        if field["name"] in ("event_time", "ingest_time"):
            # Drop the logicalType; keep the underlying long.
            field["type"] = "long"
    return schema


def _register_schema(registry_url: str, subject: str, schema_dict: dict) -> int:
    import requests

    schema_str = json.dumps(schema_dict)
    resp = requests.post(
        f"{registry_url}/subjects/{subject}/versions",
        json={"schema": schema_str, "schemaType": "AVRO"},
        timeout=10,
    )
    if resp.status_code >= 400:
        # Already registered under this exact schema -> fetch its id.
        resp2 = requests.post(
            f"{registry_url}/subjects/{subject}",
            json={"schema": schema_str, "schemaType": "AVRO"},
            timeout=10,
        )
        resp2.raise_for_status()
        return int(resp2.json()["id"])
    return int(resp.json()["id"])


def _encode_confluent_avro(schema_dict: dict, event_dict: dict, schema_id: int) -> bytes:
    """Confluent wire format: magic byte 0 + 4-byte big-endian schema-id + Avro."""
    from fastavro import schemaless_writer

    bio = io.BytesIO()
    bio.write(b"\x00")
    bio.write(schema_id.to_bytes(4, "big"))
    schemaless_writer(bio, schema_dict, event_dict)
    return bio.getvalue()


def _canonical_event(event_id: str, day_index: int, session_load: float) -> dict:
    """Build a canonical TrainingEvent dict for a given day (1-indexed)."""
    day_start = _BASE_MS + (day_index - 1) * _MS_PER_DAY
    event_time = day_start + 10 * 60 * 60 * 1000  # 10:00 UTC that day
    return {
        "event_id": event_id,
        "event_time": event_time,
        "ingest_time": event_time,
        "source": "strong_csv",
        "schema_version": 1,
        "athlete_id": _ATHLETE_ID,
        "event_type": "STRENGTH_SET",
        "workout_id": None,
        "exercise_id": None,
        "set_number": None,
        "reps": None,
        "weight_kg": None,
        "rpe": None,
        "rir": None,
        "activity_type": None,
        "distance_km": None,
        "duration_sec": None,
        "avg_hr": None,
        "tss": None,
        "session_load": session_load,
    }


def _produce_canonical(
    bootstrap_servers: str,
    topic: str,
    events: list[dict],
    schema_dict: dict,
    schema_id: int,
) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    def _on_err(err, _msg):
        if err is not None:
            raise RuntimeError(f"kafka produce failed: {err}")

    for event in events:
        payload = _encode_confluent_avro(schema_dict, event, schema_id)
        producer.produce(
            topic=topic,
            key=_ATHLETE_ID.encode("utf-8"),
            value=payload,
            callback=_on_err,
        )
    producer.flush(timeout=30)


# --- Kafka consume helper ---------------------------------------------------


def consume_exact(bootstrap_servers: str, topic: str, timeout: float):
    """Consume all messages up to ``timeout`` seconds (poll until quiet)."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"metrics-e2e-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
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
                        break
                continue
            if msg.error():
                continue
            messages.append(msg)
        return messages
    finally:
        consumer.close()


# --- The test ----------------------------------------------------------------


def test_metrics_job_e2e_acr_deload_late_dedup(redpanda_endpoints):
    """REAL end-to-end: acute/chronic/ACR computed per spec formulas, deload
    +1 after 3 consecutive ACR>1.3 days, late event -> DLQ, duplicate event_id
    deduped."""
    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    # --- Runtime probe: are the Kafka connector jars loadable? -------------
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaSink

        env_probe = StreamExecutionEnvironment.get_execution_environment()
        KafkaSink.builder()  # resolves the Java builder; raises if jar absent
        del env_probe
    except TypeError as exc:
        pytest.skip(
            "Kafka connector JARs not loadable by the pyflink gateway; "
            f"underlying error: {exc}"
        )

    # --- Isolated per-run topics -------------------------------------------
    run_id = uuid.uuid4().hex[:8]
    canonical_topic = f"canonical.training_event.e2e.{run_id}"
    metrics_topic = f"athlete.metrics.e2e.{run_id}"
    dlq_topic = f"dlq.canonical.training_event.e2e.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility

    set_compatibility(registry_url, subject, "BACKWARD")
    schema_dict = _training_event_producer_schema()
    schema_id = _register_schema(registry_url, subject, schema_dict)

    _create_topics(bootstrap_servers, [canonical_topic, metrics_topic, dlq_topic])

    # --- Produce canonical training events ---------------------------------
    # N_NORMAL_DAYS days (ascending event_time), one event per day, load=100.
    # With uniform 100/day: acute = 7d SUM, chronic = AVG -> ACR grows 1..7
    #   day 1: acute=100, chronic=100 -> ACR=1.0 (normal, first day chronic=0 ->
    #           ACR=None -> DELOAD_NORMAL; dynamic /n denominator ADR-16)
    #   day 2: acute=200, chronic=100 -> ACR=2.0 (>1.3 -> high breach)
    #   day 4: 3rd consecutive high -> deload +1
    #   day 7: acute=700, chronic=100 -> ACR=7.0, deload=+1
    events = []
    for day in range(1, _N_NORMAL_DAYS + 1):
        events.append(_canonical_event(f"evt-day-{day}", day, 100.0))
    # DUPLICATE event_id (same as day 1) -> must be deduped (day 1 stays 100).
    events.append(_canonical_event("evt-day-1", 1, 100.0))

    # Produce all in-window events (including the duplicate) FIRST.
    # The topic now contains events ordered by offset:
    #   offset 0..11 -> days 1-12 (ascending event_time BASE_MS+0..BASE_MS+11d)
    #   offset 12    -> duplicate day-1 (deduped by event_id)
    #
    # Then produce the late event at the END of the topic (C4 structural fix).
    # When the bounded job reads offsets 0..12 it advances the watermark to
    # roughly BASE_MS + 12d - 24h = BASE_MS + 11d. Day-1's window closes at
    # BASE_MS + 1d; day-1 + 24h allowed lateness = BASE_MS + 2d. The watermark
    # (BASE_MS + 11d) is well past BASE_MS + 2d, so the late event (event_time
    # = BASE_MS + 10h, i.e. day-1) arriving at offset 13 is genuinely past its
    # allowed lateness -> Flink routes it to side_output_late_data -> DLQ.
    _produce_canonical(bootstrap_servers, canonical_topic, events, schema_dict, schema_id)

    # Produce the late event AFTER the flush (= at a later topic offset).
    # Its event_time is day-1's timestamp, which is older than the window_end
    # of the day-1 TumblingEventTimeWindow + 24h allowed lateness by the time
    # the watermark has been driven forward by the day-12 events.
    late_event = _canonical_event("evt-day-1-late", 1, 100.0)
    _produce_canonical(
        bootstrap_servers, canonical_topic, [late_event], schema_dict, schema_id
    )

    # --- Run the REAL metrics job (bounded -> self-terminating) ------------
    from jobs.metrics.main import MetricsJobConfig, run

    checkpoint_dir = (
        Path(__file__).resolve().parent / f"_checkpoints_{run_id}"
    ).as_uri()
    config = MetricsJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"metrics-e2e-{run_id}",
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        metrics_output_topic=metrics_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        checkpoint_min_pause_ms=0,
        checkpoint_dir=checkpoint_dir,
        # RocksDB native cannot open its DB under the minicluster's long Windows
        # temp path; the metric math is backend-independent, so the bounded test
        # uses the default in-memory backend. Production keeps use_rocksdb=True.
        use_rocksdb=False,
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
            f"metrics job did not terminate within {_JOB_RUN_TIMEOUT_S}s "
            "(bounded Table source should drain + MAX_WATERMARK fire windows + "
            "env.execute() should return)"
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")

    # --- Consume the metrics output (take the LAST record per metric_date) --
    metrics_msgs = consume_exact(
        bootstrap_servers, metrics_topic, timeout=_CONSUME_TIMEOUT_S
    )
    assert metrics_msgs, "expected metrics output records, got none"

    by_date: dict[int, dict] = {}
    for m in metrics_msgs:
        rec = json.loads(m.value().decode("utf-8"))
        md = int(rec["metric_date"])
        by_date[md] = rec  # last write wins (final fire per day)

    def _day_index(metric_date_ms: int) -> int:
        return (metric_date_ms - _BASE_MS) // _MS_PER_DAY + 1

    def _get_day(day_index: int) -> dict | None:
        target = _BASE_MS + (day_index - 1) * _MS_PER_DAY
        return by_date.get(target)

    # --- ACR computation (spec formula) ------------------------------------
    # day 7: acute=700 (7*100), chronic_28d=100 (avg of 7 days), ACR=7.0.
    day7 = _get_day(7)
    assert day7 is not None, (
        f"missing metrics for day 7; got days={sorted(_day_index(d) for d in by_date)}"
    )
    assert abs(float(day7["acute_load"]) - 700.0) < 0.01, (
        f"day 7 acute_load != 700.0: {day7['acute_load']!r}"
    )
    assert abs(float(day7["chronic_load_28d"]) - 100.0) < 0.01, (
        f"day 7 chronic_load_28d != 100.0: {day7['chronic_load_28d']!r}"
    )
    assert abs(float(day7["chronic_load_42d"]) - 100.0) < 0.01, (
        f"day 7 chronic_load_42d != 100.0: {day7['chronic_load_42d']!r}"
    )
    assert abs(float(day7["acute_chronic_ratio"]) - 7.0) < 0.01, (
        f"day 7 ACR != 7.0 (acute/chronic_28d = 700/100): "
        f"{day7['acute_chronic_ratio']!r}"
    )

    # --- deload_flag triggers +1 after 3 consecutive ACR>1.3 days ----------
    # ACR>1.3 starts day 2; 3rd consecutive = day 4 -> deload +1.
    day4 = _get_day(4)
    assert day4 is not None, "missing metrics for day 4"
    assert int(day4["deload_flag"]) == 1, (
        f"day 4 deload_flag must be +1 (3 consecutive ACR>1.3): "
        f"{day4['deload_flag']!r}"
    )
    # day 1 is normal (ACR=1.0, first day -> no streak yet).
    day1 = _get_day(1)
    assert day1 is not None, "missing metrics for day 1"
    assert int(day1["deload_flag"]) == 0, (
        f"day 1 deload_flag must be 0 (ACR=1.0): {day1['deload_flag']!r}"
    )
    # day 7 stays +1 (streak continues).
    assert int(day7["deload_flag"]) == 1, (
        f"day 7 deload_flag must be +1 (streak continues): "
        f"{day7['deload_flag']!r}"
    )

    # --- Dedup proof: the duplicate evt-day-1 did NOT double day 1 load ----
    # If dedup failed, day 1 daily_load=200 -> acute at day 7 = 100*6+200 = 800.
    # acute=700 proves the duplicate was dropped.
    assert abs(float(day7["acute_load"]) - 700.0) < 0.01, (
        "dedup failed: duplicate evt-day-1 should have been dropped (acute at "
        "day 7 must remain 700, not 800)"
    )

    # --- Late data -> DLQ (LATE_DATA) --------------------------------------
    dlq_msgs = consume_exact(bootstrap_servers, dlq_topic, timeout=_CONSUME_TIMEOUT_S)
    assert dlq_msgs, "expected DLQ records (the late day-1 event), got none"
    late_seen = False
    for m in dlq_msgs:
        rec = json.loads(m.value().decode("utf-8"))
        if rec.get("error_type") == "LATE_DATA":
            late_seen = True
            break
    assert late_seen, (
        f"expected a LATE_DATA record in the DLQ (late day-1 event); got: "
        f"{[json.loads(m.value().decode('utf-8')) for m in dlq_msgs]}"
    )
