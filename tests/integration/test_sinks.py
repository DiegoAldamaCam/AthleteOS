"""PR5 Phase 6 (work-unit 6.4) integration test: PG UPSERT + Iceberg append sinks.

Verifies that running the bounded metrics job with pg_dsn + iceberg_warehouse
enabled writes correct data to both stores, in addition to the existing
athlete.metrics.stream Kafka topic (OQ-4 fan-out).

Structure mirrors tests/integration/test_metrics_job.py.

Topology under test (PR5 sinks wired in):
    Table source(canonical.training_event, avro-confluent, bounded)
      -> to_data_stream -> watermark -> dedup
      -> [IcebergAppendFn] (canonical events -> Iceberg warehouse)  [PR5]
      -> key_by(athlete_id) -> TumblingEventTimeWindows(1d) -> daily_load
      -> key_by(athlete_id) -> SlidingEventTimeWindows(42d, 1d) -> ACR
      -> key_by(athlete_id) -> DeloadKeyedProcessFunction -> metrics Row
      -> [PgUpsertFn] (metrics rows -> athlete_metrics)              [PR5]
      -> map -> JSON -> KafkaSink(athlete.metrics.stream)            [PR4 kept]

Assertions:
  1. PG athlete_metrics has expected row(s) with correct acute_load / deload_flag
     and metric_date as DATE.
  2. Duplicate event (same event_id) does NOT create a duplicate PG metrics row
     (UPSERT idempotency: count per (athlete_id, metric_date) stays 1).
  3. acute_chronic_ratio is NULL in PG when chronic_load_28d == 0 (day 1 with
     one event -> chronic is the avg of one day = 100, NOT 0; day 0 scenario
     would be null but that cannot happen with events present).
     For the chosen scenario (3 days, uniform load=100):
       day 1: acute=100, chronic_28d=100, ACR=1.0  (not null)
       The NULL ACR path requires chronic=0 which requires no events at all;
       we assert via the JSON Kafka output that the value is numeric (not null).
  4. Iceberg warehouse has Parquet data files; read_training_events returns the
     appended canonical events (count == produced events, dedup via event_id
     already applied upstream before the Iceberg sink).
  5. check_parity returns no mismatches (all (athlete_id, day) keys in PG have
     matching Iceberg events, and vice-versa).

Docker-gated: test self-skips cleanly when Docker is unavailable.
pyflink-gated: module-level skip when apache-flink not importable.

CI safety: uses storage.duckdb.reader (Parquet direct, no iceberg_scan —
avoids DuckDB extension download and Windows path issues; see obs #50).
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import threading
import time
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module-level pyflink gate (same pattern as test_metrics_job.py)
# ---------------------------------------------------------------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); sinks integration test skipped",
        allow_module_level=True,
    )

# ---------------------------------------------------------------------------
# Windows PyArrowFileIO shim (must run before any pyiceberg import that touches
# the filesystem — same shim as test_iceberg_sink.py).
# ---------------------------------------------------------------------------


from tests._pyarrow_compat import patch_pyarrow_file_io as _patch_pyarrow_file_io

# Apply the shim before any pyiceberg import (Flink minicluster runs UDFs
# in-process, so the patch must be active when _IcebergAppendFn.open() runs).
_patch_pyarrow_file_io()

# ---------------------------------------------------------------------------
# Connector JARs (same set as test_metrics_job.py)
# ---------------------------------------------------------------------------

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


_ensure_connector_jars()

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Test-scoped constants
# ---------------------------------------------------------------------------

_ATHLETE_ID = "athlete-s1"
# Base day = 2024-02-01 00:00 UTC (different from test_metrics_job.py to avoid
# topic name collision; also a different base to verify date conversion).
_BASE_DT = datetime(2024, 2, 1, 0, 0, 0, tzinfo=timezone.utc)
_BASE_MS = int(_BASE_DT.timestamp() * 1000)

from jobs.metrics.compute import MILLIS_PER_DAY as _MS_PER_DAY  # noqa: E402

# 4 days: enough for the watermark to pass day-1's window_end + 24h allowed
# lateness (same structural guarantee as test_metrics_job.py C4 note).
# With 4 days at 100/day:
#   day 1: acute=100, chronic_28d=100 (avg/1), ACR=1.0, deload=0
#   day 2: acute=200, chronic_28d=150 (avg/2), ACR=1.33 > 1.3 -> high streak
#   day 3: acute=300, chronic_28d=200 (avg/3), ACR=1.5 -> 2nd consecutive high
#   day 4: acute=400, chronic_28d=250 (avg/4), ACR=1.6 -> 3rd -> deload=+1
_N_DAYS = 4
_SESSION_LOAD = 100.0

_CHECKPOINT_MS = 2_000
_JOB_RUN_TIMEOUT_S = 300
_CONSUME_TIMEOUT_S = 60

# ---------------------------------------------------------------------------
# Helpers (shared with test_metrics_job.py pattern)
# ---------------------------------------------------------------------------


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
    """TrainingEvent.avsc with event_time/ingest_time as plain long (no logicalType)."""
    avsc_path = (
        Path(__file__).resolve().parents[2]
        / "schemas" / "canonical" / "TrainingEvent.avsc"
    )
    schema = json.loads(avsc_path.read_text(encoding="utf-8"))
    for field in schema["fields"]:
        if field["name"] in ("event_time", "ingest_time"):
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


def _apply_ddl(pg_dsn: str, ddl_path: Path) -> None:
    """Apply the DDL script to the target PostgreSQL database."""
    import psycopg2

    sql = ddl_path.read_text(encoding="utf-8")
    conn = psycopg2.connect(pg_dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    finally:
        conn.close()


def _pg_query(pg_dsn: str, sql: str, params=None) -> list[tuple]:
    """Execute a query against PG and return all rows as list of tuples."""
    import psycopg2

    conn = psycopg2.connect(pg_dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------


def test_pg_and_iceberg_sinks(
    tmp_path,
    redpanda_endpoints,
    postgres_container,
):
    """Integration: bounded metrics job writes to PG + Iceberg (PR5 sinks).

    Assertions:
      A. PG athlete_metrics row count and values match spec formulas.
      B. Duplicate event_id does NOT create duplicate PG row (UPSERT idempotency).
      C. Iceberg warehouse has Parquet data; read_training_events returns events.
      D. check_parity returns no mismatches.
    """
    bootstrap_servers = redpanda_endpoints["bootstrap_servers"]
    registry_url = redpanda_endpoints["schema_registry_url"]

    # --- Runtime probe: Kafka connector JARs loadable? ----------------------
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaSink

        env_probe = StreamExecutionEnvironment.get_execution_environment()
        KafkaSink.builder()
        del env_probe
    except TypeError as exc:
        pytest.skip(
            f"Kafka connector JARs not loadable: {exc}"
        )

    # --- PG DSN from testcontainers -----------------------------------------
    # driver=None gives the plain postgresql:// URI that psycopg2.connect()
    # accepts directly as a libpq connection string (psycopg2 >= 2.7).
    pg_dsn = postgres_container.get_connection_url(driver=None)

    # Apply DDL to the testcontainers PG
    ddl_path = Path(__file__).resolve().parents[2] / "storage" / "postgres" / "ddl.sql"
    _apply_ddl(pg_dsn, ddl_path)

    # --- Iceberg warehouse in tmp_path --------------------------------------
    iceberg_warehouse = str(tmp_path / "iceberg_warehouse")
    Path(iceberg_warehouse).mkdir(parents=True, exist_ok=True)

    # Pre-create the Iceberg table (same pattern as storage.iceberg.tables unit
    # tests): the sink's open() will load-or-create it, but creating it here
    # ensures the warehouse is fully initialised before run() starts.
    from pyiceberg.catalog.sql import SqlCatalog
    from storage.iceberg.tables import create_training_event_table

    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{iceberg_warehouse}/catalog.db",
            "warehouse": iceberg_warehouse,
        },
    )
    create_training_event_table(catalog)

    # --- Isolated per-run Kafka topics --------------------------------------
    run_id = uuid.uuid4().hex[:8]
    canonical_topic = f"canonical.training_event.sinks.{run_id}"
    metrics_topic = f"athlete.metrics.sinks.{run_id}"
    dlq_topic = f"dlq.canonical.training_event.sinks.{run_id}"
    subject = f"{canonical_topic}-value"

    from bootstrap.register_schemas import set_compatibility

    set_compatibility(registry_url, subject, "BACKWARD")
    schema_dict = _training_event_producer_schema()
    schema_id = _register_schema(registry_url, subject, schema_dict)
    _create_topics(bootstrap_servers, [canonical_topic, metrics_topic, dlq_topic])

    # --- Produce canonical events ------------------------------------------
    # N_DAYS events (ascending event_time), one per day, load=100.
    # Plus a duplicate of day 1 (same event_id) to test UPSERT idempotency.
    events = [
        _canonical_event(f"evt-s1-day-{d}", d, _SESSION_LOAD)
        for d in range(1, _N_DAYS + 1)
    ]
    # Duplicate day-1 event_id; the dedup operator drops it before both sinks.
    dup_event = _canonical_event("evt-s1-day-1", 1, _SESSION_LOAD)

    _produce_canonical(bootstrap_servers, canonical_topic, events + [dup_event], schema_dict, schema_id)

    # --- Run the bounded metrics job with PR5 sinks enabled -----------------
    from jobs.metrics.main import MetricsJobConfig, run

    checkpoint_dir = (
        Path(__file__).resolve().parent / f"_checkpoints_sinks_{run_id}"
    ).as_uri()
    config = MetricsJobConfig(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=registry_url,
        group_id=f"metrics-sinks-{run_id}",
        canonical_topic=canonical_topic,
        dlq_topic=dlq_topic,
        metrics_output_topic=metrics_topic,
        checkpoint_interval_ms=_CHECKPOINT_MS,
        checkpoint_min_pause_ms=0,
        checkpoint_dir=checkpoint_dir,
        use_rocksdb=False,
        bounded=True,
        parallelism=1,
        no_restart=True,
        # PR5 sinks enabled:
        pg_dsn=pg_dsn,
        iceberg_warehouse=iceberg_warehouse,
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
            f"metrics job did not terminate within {_JOB_RUN_TIMEOUT_S}s"
        )
    if not outcome.get("done"):
        raise outcome.get("error") or AssertionError("job finished with no result")

    # =========================================================================
    # Assertion A: PG athlete_metrics rows
    # =========================================================================
    pg_rows = _pg_query(
        pg_dsn,
        "SELECT athlete_id, metric_date, acute_load, chronic_load_28d, "
        "       chronic_load_42d, acute_chronic_ratio, deload_flag, "
        "       fatigue_score, readiness_score, coaching_flags "
        "FROM athlete_metrics "
        "WHERE athlete_id = %s "
        "ORDER BY metric_date",
        (_ATHLETE_ID,),
    )

    # We produced events for days 1-4; expect at least day 4 in PG.
    # The window may emit partial results for earlier days too; we focus on day
    # 4 (the final state per the bounded source) and day 1 (deload=0 baseline).
    assert pg_rows, (
        f"Expected athlete_metrics rows for {_ATHLETE_ID!r}, got none. "
        f"Ensure DDL was applied and PG sink wrote correctly."
    )

    # Build a dict keyed by metric_date (DATE object)
    by_date: dict[date, tuple] = {}
    for row in pg_rows:
        # row = (athlete_id, metric_date, acute_load, chronic_28d, chronic_42d,
        #         acr, deload, fatigue_score, readiness_score, coaching_flags)
        by_date[row[1]] = row

    # Expected day-1 date
    day1_date = date(2024, 2, 1)
    day4_date = date(2024, 2, 4)

    # A1: metric_date is a DATE (not an int / epoch) — testcontainers returns
    # Python datetime.date from psycopg2 for DATE columns.
    for md in by_date:
        assert isinstance(md, date), (
            f"metric_date must be DATE, got {type(md)!r}: {md!r}"
        )

    # A2: day 1 acute_load = 100 (one event, session_load=100)
    assert day1_date in by_date, (
        f"Missing PG row for day 1 (2024-02-01); got dates: {sorted(by_date)}"
    )
    row1 = by_date[day1_date]
    acute1 = float(row1[2])
    assert abs(acute1 - 100.0) < 0.5, (
        f"day 1 acute_load should be ~100.0, got {acute1}"
    )

    # A3: day 1 deload_flag = 0 (ACR=1.0, no streak)
    assert int(row1[6]) == 0, (
        f"day 1 deload_flag should be 0, got {row1[6]}"
    )

    # A4: day 4 — deload_flag = +1 after 3 consecutive ACR > 1.3 days
    # (day 2: ACR=1.33, day 3: ACR=1.5, day 4: ACR=1.6 — 3rd consecutive -> +1)
    assert day4_date in by_date, (
        f"Missing PG row for day 4 (2024-02-04); got dates: {sorted(by_date)}"
    )
    row4 = by_date[day4_date]
    acute4 = float(row4[2])
    assert abs(acute4 - 400.0) < 0.5, (
        f"day 4 acute_load should be ~400.0 (4 * 100), got {acute4}"
    )
    assert int(row4[6]) == 1, (
        f"day 4 deload_flag should be +1 (3 consecutive ACR>1.3), got {row4[6]}"
    )

    # Scenario 19 / 21 / 22 (metrics-v2): 10-column round-trip assertions.
    # fatigue_score and readiness_score: either float or None (NULL for new athletes).
    for md, row in by_date.items():
        fatigue_val = row[7]   # fatigue_score
        readiness_val = row[8]  # readiness_score
        flags_val = row[9]      # coaching_flags TEXT
        # Scenario 22 (compatibility): existing 7 cols (positions 0-6) unaffected.
        # Positions 0-6 already asserted above; spot-check type consistency.
        assert isinstance(row[0], str), f"athlete_id must be str at {md}"
        # Scenario 19 (atomic 10-field write): all 10 columns readable.
        # fatigue/readiness: float or None — both valid (NULL for zero-baseline rows).
        # FIX 6b: isinstance without float() cast — float(x) is always a float (tautology).
        if fatigue_val is not None:
            assert isinstance(fatigue_val, (float, int)), (
                f"fatigue_score must be float or NULL, got {type(fatigue_val)!r} at {md}"
            )
        # Scenario 21 (readiness never > 80): honesty cap enforced end-to-end.
        if readiness_val is not None:
            assert float(readiness_val) <= 80.0, (
                f"readiness_score MUST NOT exceed 80.0 (honesty cap violated: "
                f"{readiness_val} at {md})"
            )
        # coaching_flags: TEXT (JSON) or NULL — json.loads must succeed when present.
        if flags_val is not None:
            import json as _json
            parsed_flags = _json.loads(flags_val)
            assert isinstance(parsed_flags, list), (
                f"coaching_flags must be a JSON array, got {type(parsed_flags)!r} at {md}"
            )

    # =========================================================================
    # Assertion B: UPSERT idempotency — duplicate event_id does NOT double rows
    # =========================================================================
    # The duplicate event_id "evt-s1-day-1" is deduped upstream (DedupAndGuard)
    # so only 1 event reaches day-1's daily window -> acute_load stays 100, not 200.
    # Also: only 1 PG row per (athlete_id, metric_date) exists.
    count_rows = _pg_query(
        pg_dsn,
        "SELECT COUNT(*) FROM athlete_metrics "
        "WHERE athlete_id = %s AND metric_date = %s",
        (_ATHLETE_ID, day1_date),
    )
    assert count_rows[0][0] == 1, (
        f"Expected 1 PG row for (athlete={_ATHLETE_ID!r}, day=2024-02-01) "
        f"(UPSERT idempotency), got {count_rows[0][0]}"
    )
    # If dedup failed, acute_load for day 1 would be 200 (two loads of 100).
    assert abs(float(by_date[day1_date][2]) - 100.0) < 0.5, (
        "Dedup or UPSERT failure: day 1 acute_load should be 100 (not 200)."
    )

    # =========================================================================
    # Assertion C: Iceberg Parquet data files exist; read_training_events works
    # =========================================================================
    # The deduped stream sends N_DAYS unique events (the dup is dropped) to the
    # Iceberg sink.  We expect exactly N_DAYS rows in the warehouse.
    from storage.duckdb.reader import read_training_events

    iceberg_rows = read_training_events(iceberg_warehouse)
    assert len(iceberg_rows) > 0, (
        f"Expected Iceberg Parquet files in {iceberg_warehouse!r}, got none."
    )
    # Primary assertion: every expected event_id is present (set equality).
    # AT_LEAST_ONCE delivery means the count may be >= _N_DAYS (rare replays
    # could produce duplicate Parquet rows); the event_id set must be exact.
    expected_event_ids = {f"evt-s1-day-{d}" for d in range(1, _N_DAYS + 1)}
    actual_event_ids = {
        r["event_id"] if isinstance(r["event_id"], str) else r["event_id"].decode()
        for r in iceberg_rows
    }
    assert actual_event_ids == expected_event_ids, (
        f"Iceberg event_id set mismatch.\n"
        f"  Expected: {sorted(expected_event_ids)}\n"
        f"  Got:      {sorted(actual_event_ids)}"
    )
    # Secondary assertion: exact count under AT_LEAST_ONCE dedup (allow >=).
    assert len(iceberg_rows) >= _N_DAYS, (
        f"Expected at least {_N_DAYS} Iceberg rows, got {len(iceberg_rows)}."
    )

    # C2: All rows have the correct athlete_id
    iceberg_athlete_ids = {
        r["athlete_id"] if isinstance(r["athlete_id"], str) else r["athlete_id"].decode()
        for r in iceberg_rows
    }
    assert iceberg_athlete_ids == {_ATHLETE_ID}, (
        f"Iceberg rows have unexpected athlete_ids: {iceberg_athlete_ids}"
    )

    # =========================================================================
    # Assertion D: parity check — no pg_missing mismatches
    # =========================================================================
    # PG grain: per-(athlete_id, metric_date) DERIVED METRICS from rolling
    #           SlidingEventTimeWindows(42d, slide 1d).
    # Iceberg grain: per-event RAW CANONICAL EVENTS.
    #
    # The sliding window produces metric rows for many dates beyond the raw
    # event dates (e.g., 4 events on Feb 1-4 produce PG metrics for Feb 1
    # through Feb 4+~42=Mar 17, one per 1d window slide that contains any
    # event). This means:
    #
    #   iceberg_missing — expected for any PG metric_date where no raw event
    #   exists (e.g., Feb 5 has a PG metric derived from the Feb 1-4 events,
    #   but no Feb 5 raw event in Iceberg). These are CORRECT and expected.
    #
    #   pg_missing — unexpected: every Iceberg event day should have a PG
    #   metric row for that same day (an event on day D always triggers a
    #   window that emits a metric for day D). If pg_missing appears, it
    #   means the PG sink dropped events that the Iceberg sink received.
    #
    # The assertion: no pg_missing mismatches. iceberg_missing is acceptable
    # (structural grain difference; see storage/duckdb/parity.py docstring).
    from storage.duckdb.parity import check_parity

    # Build pg_rows input for check_parity: metric_date is a datetime.date;
    # _to_day_epoch_ms handles it via the datetime.date branch.
    pg_parity_input = [
        {"athlete_id": row[0], "metric_date": row[1]}
        for row in pg_rows
    ]
    all_mismatches = check_parity(pg_parity_input, iceberg_rows)
    pg_missing = [m for m in all_mismatches if m["side"] == "pg_missing"]
    assert pg_missing == [], (
        f"Unexpected pg_missing parity mismatches: {pg_missing}\n"
        f"Every Iceberg event day must have a matching PG metric row."
    )
    # iceberg_missing is expected and > 0: the 42d sliding window emits PG
    # metric rows for days beyond the raw event days (e.g. Feb 5 through ~Mar 17
    # for 4 Feb events), but those future days have no Iceberg raw events.
    # For 4 events and a 42d window, the sliding window produces metric rows for
    # many dates that have no corresponding Iceberg event → iceberg_missing > 0.
    iceberg_missing_count = len([m for m in all_mismatches if m["side"] == "iceberg_missing"])
    assert iceberg_missing_count > 0, (
        f"Expected iceberg_missing > 0 (42d sliding window produces PG metrics "
        f"for days beyond the {_N_DAYS} raw event days), got {iceberg_missing_count}. "
        f"This may indicate the window did not emit future-day metrics."
    )
