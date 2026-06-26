"""PyFlink metrics job wiring (PR4, Phase 5, tasks 5.1-5.5).

Import-isolation contract
=========================
apache-flink has no wheel for CPython 3.14. To keep the package importable and
``pytest --collect-only`` working, **all pyflink imports are LAZY** -- they live
inside ``run()``. The PURE metric math (daily_load, rolling acute/chronic, ACR,
deload state machine, NaN guard, DLQ envelope) lives in
:mod:`jobs.metrics.compute`, which imports WITHOUT pyflink and is fully
unit-tested under CPython 3.14 (tests/unit/test_metrics_compute.py).

The integration slice (tests/integration/test_metrics_job.py) exercises this
wiring end-to-end when apache-flink IS installed and Docker is up; it SKIPS
cleanly otherwise (never fakes a pass).

Topology (gate-passed design ADR-11: EVENT-TIME WINDOWS, NOT manual MapState)
============================================================================
canonical.training_event (Avro, Confluent Schema Registry) is consumed via the
Table API ``avro-confluent`` source (the ONLY real PyFlink 1.19 path to the
Confluent-Registry Avro serde -- the DataStream ConfluentRegistryAvro* schemas
are Java-only, same finding as PR3 ADR-15). The Table source is lifted into a
DataStream via ``to_data_stream``; the rest is DataStream event-time windowing:

    Table source(canonical.training_event, avro-confluent, bounded=latest-offset)
      -> to_data_stream -> DataStream[Row]
      -> assign_timestamps_and_watermarks(24h out-of-orderness, event_time epoch-ms)
      -> key_by(event_id) -> DedupKeyedProcessFunction
           ValueState<bool> per event_id, 7d TTL (LOCKED)
           NaN/Inf session_load -> DLQ side output (VALIDATION_FAILURE)
           main: yield Row (unchanged, record timestamp = event_time preserved)
      -> key_by(athlete_id)
      -> window(TumblingEventTimeWindows.of(1d))
           .trigger(ContinuousEventTimeTrigger.of(1d))   # emit-on-update (ADR-13)
           .allowed_lateness(24h)
           .side_output_late_data(late.daily)            # late events -> DLQ
           .aggregate(SumSessionLoadAgg, DailyLoadWindowFn)  # daily_load per (athlete, day)
      => daily_load stream: Row(athlete_id, day_start, daily_load)
      -> key_by(athlete_id)
      -> window(SlidingEventTimeWindows.of(42d, slide 1d))
           .allowed_lateness(24h)
           .side_output_late_data(late.rolling)
           .process(RollingMetricsWindowFn)
             # dedupes daily_loads by day (max -> final, handles
             # ContinuousEventTimeTrigger multi-emit), then computes:
             #   acute_load        = SUM  of daily_load for last 7 days
             #   chronic_load_28d  = AVG  of daily_load for last 28 days
             #   chronic_load_42d  = AVG  of daily_load for last 42 days
             #   acute_chronic_ratio = acute / chronic_28d (None if chronic=0)
      => acr stream: Row(athlete_id, metric_date, acute, chronic_28d, chronic_42d, acr)
      -> key_by(athlete_id) -> DeloadKeyedProcessFunction
           ValueState<(last_day, count, sign)>; pure update_deload_state();
           +1 if ACR>1.3 >=3 consecutive days, -1 if ACR<0.8 >=3 days, else 0;
           idempotent per day (skips re-fires for the same day from allowed
           lateness).
      => metrics stream: Row(athlete_id, metric_date, acute, chronic_28d,
                              chronic_42d, acr, deload_flag)
      -> map -> JSON string -> KafkaSink(metrics output topic, AT_LEAST_ONCE)
    DLQ (NaN + late) -> union -> KafkaSink(dlq.canonical.training_event, AT_LEAST_ONCE)

Design refinement (documented, not a deviation from intent)
------------------------------------------------------------
The design describes "three SlidingEventTimeWindows (7d/28d/42d)". Implementing
those as three separate window streams would require a stream-stream join to
align acute/chronic by (athlete, day) before computing ACR -- the riskiest
PyFlink API and the most fragile to terminate in a bounded test. Instead a
SINGLE SlidingEventTimeWindows.of(42d, 1d) with a ProcessWindowFunction
computes all three metrics from the window contents (filter the last 7 / 28 /
42 daily loads). This is STILL an event-time sliding window (honors ADR-11:
event-time windows, NOT manual MapState), uses allowed_lateness +
side_output_late_data natively, and computes the EXACT spec formulas. The
three-stream join is avoided. This is an equivalent realization of the
gate-passed design, recorded here for the verify phase.

PR4 scope: the output is a metrics DataStream sunk to a staging Kafka topic
for test assertion. The PostgreSQL + Iceberg sinks (exactly-once main path)
are PR5 (Phase 6), NOT this PR. The env checkpointing is EXACTLY_ONCE (task
5.1); the PR4 staging + DLQ sinks are AT_LEAST_ONCE (PG/Iceberg add
exactly-once in PR5).

Refs #1.
"""

from __future__ import annotations

import json
import math
import os
import sys
from typing import Any

# Constants are import-safe (no pyflink); they describe the topology.
CANONICAL_TOPIC = "canonical.training_event"
DLQ_TOPIC = "dlq.canonical.training_event"
METRICS_OUTPUT_TOPIC = "athlete.metrics.stream"
SOURCE_NAME = "metrics-training-event-source"

# Dedup ValueState<bool> per event_id, 7d TTL (LOCKED design).
DEDUP_TTL_DAYS = 7
# Batch-ish canonical topic: bounded out-of-orderness = 24h (design).
WATERMARK_OUT_OF_ORDER_HOURS = 24
# Allowed lateness on every event-time window = 24h (design ADR-11).
ALLOWED_LATENESS_MS = 24 * 60 * 60 * 1000


# Characters that must never appear in DDL-interpolated config values.
# A single quote closes an SQL string literal; double quote, newline (LF/CR),
# and null byte enable multi-line or parser-confusion injection into the
# Flink Table DDL f-string (source_ddl). This is an allowlist-based guard:
# if the value contains ANY of these characters it is unconditionally rejected
# with a ValueError before it can reach tbl_env.execute_sql. (RISK F1)
_DDL_FORBIDDEN_CHARS: str = "'\"\n\r\x00"


def _validate_ddl_config_field(field_name: str, value: str) -> None:
    """Reject values that contain SQL/DDL injection characters.

    Raises ValueError with the field name so the caller can identify which
    config parameter is problematic. Called in MetricsJobConfig.__init__ for
    every field that is interpolated into source_ddl. (RISK F1)
    """
    for ch in _DDL_FORBIDDEN_CHARS:
        if ch in value:
            raise ValueError(
                f"MetricsJobConfig.{field_name} contains a character that is "
                f"forbidden in DDL interpolation (char ord={ord(ch):#04x}). "
                f"Accepted characters: printable ASCII excluding '\"\\n\\r\\x00. "
                f"Received value (first 80 chars): {value[:80]!r}"
            )


class MetricsJobConfig:
    """Plain configuration container (no pyflink). Import-safe.

    Allows tests / orchestrators to construct and inspect a config without
    pulling in pyflink; ``run()`` consumes it from inside the lazy-import scope.

    Validation (RISK F1 — DDL injection guard): bootstrap_servers,
    schema_registry_url, group_id, and canonical_topic are interpolated raw
    into a Flink Table DDL f-string (tbl_env.execute_sql). Any value containing
    a quote, newline, or null byte could inject arbitrary connector properties.
    These four fields are validated at construction time; invalid values raise
    ValueError immediately, before any DDL is executed.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id: str = "metrics-training-event",
        canonical_topic: str = CANONICAL_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
        metrics_output_topic: str = METRICS_OUTPUT_TOPIC,
        checkpoint_interval_ms: int = 60_000,
        checkpoint_min_pause_ms: int = 30_000,
        checkpoint_dir: str = "file:///tmp/athleteos-metrics-checkpoints",
        use_rocksdb: bool = True,
        bounded: bool = False,
        parallelism: int | None = None,
        no_restart: bool = False,
    ) -> None:
        # Validate all fields that are interpolated into the DDL f-string.
        # Must run BEFORE assignment so a bad value never reaches run(). (RISK F1)
        _validate_ddl_config_field("bootstrap_servers", bootstrap_servers)
        _validate_ddl_config_field("schema_registry_url", schema_registry_url)
        _validate_ddl_config_field("group_id", group_id)
        _validate_ddl_config_field("canonical_topic", canonical_topic)
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.group_id = group_id
        self.canonical_topic = canonical_topic
        self.dlq_topic = dlq_topic
        self.metrics_output_topic = metrics_output_topic
        self.checkpoint_interval_ms = checkpoint_interval_ms
        self.checkpoint_min_pause_ms = checkpoint_min_pause_ms
        self.checkpoint_dir = checkpoint_dir
        # EmbeddedRocksDBStateBackend is the production state backend (task 5.1,
        # design "Keyed state (RocksDB)"). It is gated behind this flag because
        # the RocksDB native library cannot open its DB under the Flink
        # minicluster's long TaskManager temp path on Windows
        # ("Failed to create a NewWriteableFile: ...\\db\\MANIFEST-000001: The
        # system cannot find the path specified" -- the minicluster auto-
        # generated tmp dir + operator-id UUID exceed what RocksDB's Windows
        # file handle can create). The metric math (windowing, ACR, deload,
        # dedup, late routing) is state-backend independent, so the bounded
        # integration test runs with the default in-memory (HashMap) backend to
        # verify correctness; production keeps ``use_rocksdb=True``. Checkpoint
        # EXACTLY_ONCE / externalized / RETAIN_ON_CANCELLATION / tolerable=3
        # apply to BOTH backends (task 5.1).
        self.use_rocksdb = use_rocksdb
        # When True the Table Kafka source is BOUNDED (scan.bounded.mode =
        # latest-offset): reads earliest -> latest-at-startup, then finishes ->
        # MAX_WATERMARK fires all windows -> env.execute() returns. Production
        # stays unbounded (False).
        self.bounded = bounded
        self.parallelism = parallelism
        self.no_restart = no_restart


# ---------------------------------------------------------------------------
# Job wiring (PYFLINK-DEPENDENT). Imported lazily. Do not call at import time.
# ---------------------------------------------------------------------------


def run(config: MetricsJobConfig) -> None:  # pragma: no cover - flink runtime
    """Build and execute the metrics job against a live broker + Registry.

    All pyflink imports are INSIDE this function so the module imports cleanly
    on interpreters without apache-flink. Executed only on a flink-capable
    runtime (apache-flink 1.19 on CPython 3.8-3.11).
    """
    # --- pyflink imports (deferred) -----------------------------------------
    from pyflink.common import (
        Duration,
        Row,
        Time,
        Types,
        WatermarkStrategy,
    )
    from pyflink.common.watermark_strategy import TimestampAssigner
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import (
        CheckpointingMode,
        ExternalizedCheckpointCleanup,
        OutputTag,
        RuntimeExecutionMode,
        StreamExecutionEnvironment,
    )
    from pyflink.datastream.connectors.kafka import (
        DeliveryGuarantee,
        KafkaRecordSerializationSchema,
        KafkaSink,
    )
    from pyflink.datastream.functions import (
        AggregateFunction,
        KeyedProcessFunction,
        ProcessWindowFunction,
    )
    from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
    from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
    from pyflink.datastream.window import (
        ContinuousEventTimeTrigger,
        SlidingEventTimeWindows,
        TumblingEventTimeWindows,
    )
    from pyflink.table import (
        EnvironmentSettings,
        StreamTableEnvironment,
    )

    from jobs.metrics.compute import (
        LATE_DATA,
        MILLIS_PER_DAY,
        VALIDATION_FAILURE,
        acute_chronic_ratio,
        build_metrics_dlq_envelope,
        epoch_ms_now,
        is_finite_load,
        update_deload_state,
    )

    # --- environment --------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if config.parallelism is not None:
        env.set_parallelism(config.parallelism)
    if config.no_restart:
        from pyflink.common import RestartStrategies

        env.set_restart_strategy(RestartStrategies.no_restart())

    # RocksDB state backend (task 5.1, production) + EXACTLY_ONCE checkpointing.
    # The state backend is gated by config.use_rocksdb (see MetricsJobConfig):
    # the bounded integration test uses the default in-memory backend because
    # RocksDB native cannot open its DB under the minicluster's long Windows temp
    # path. EXACTLY_ONCE / externalized / RETAIN_ON_CANCELLATION / tolerable=3
    # apply to both backends.
    if config.use_rocksdb:
        env.set_state_backend(EmbeddedRocksDBStateBackend())
    env.enable_checkpointing(config.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().enable_externalized_checkpoints(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    env.get_checkpoint_config().set_min_pause_between_checkpoints(
        config.checkpoint_min_pause_ms
    )
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(3)
    env.get_checkpoint_config().set_checkpoint_storage_dir(config.checkpoint_dir)

    table_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=table_settings)

    # --- Row types ----------------------------------------------------------
    # Canonical TrainingEvent field layout (order matches the canonicalize sink
    # DDL / TrainingEvent.avsc; accessed by INDEX downstream).
    canonical_field_names = (
        "event_id", "event_time", "ingest_time", "source", "schema_version",
        "athlete_id", "event_type", "workout_id", "exercise_id", "set_number",
        "reps", "weight_kg", "rpe", "rir", "activity_type", "distance_km",
        "duration_sec", "avg_hr", "tss", "session_load",
    )

    def _canonical_row_type() -> Any:
        # A FRESH instance with FRESH field-type instances each call.
        # PyFlink's TypeInformation.get_java_type_info() MUTATES the instance
        # (caches a Java object holding an RLock) AND resolves its field types'
        # Java types, mutating the field-type instances too. Types.ROW_NAMED
        # shares field-type instances across rows built from the same list, so
        # reusing a shared ``_canonical_field_types`` list would let one
        # operator's ``output_type`` resolution corrupt another's OutputTag
        # type -> "cannot pickle '_thread.RLock' object" on the window op's
        # cloudpickle. Building fresh field types inline per call keeps every
        # OutputTag type independent from any output_type. (WindowedStream
        # .get_input_type() also reconstructs a fresh type from the Java stream
        # type, so the 42d ListStateDescriptor is safe.)
        return Types.ROW_NAMED(
            list(canonical_field_names),
            [
                Types.STRING(), Types.LONG(), Types.LONG(), Types.STRING(), Types.INT(),
                Types.STRING(), Types.STRING(),
                Types.STRING(), Types.STRING(), Types.INT(),
                Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
                Types.STRING(), Types.FLOAT(), Types.INT(), Types.INT(), Types.FLOAT(),
                Types.FLOAT(),
            ],
        )

    def _daily_load_row_type() -> Any:
        # Fresh field-type instances each call (see _canonical_row_type note).
        return Types.ROW_NAMED(
            ["athlete_id", "day_start", "daily_load"],
            [Types.STRING(), Types.LONG(), Types.FLOAT()],
        )

    # Types used as operator output_type (these get mutated by get_java_type_info
    # -> keep them separate from the OutputTag types below).
    canonical_row_type = _canonical_row_type()
    daily_load_row_type = _daily_load_row_type()
    acr_row_type = Types.ROW_NAMED(
        ["athlete_id", "metric_date", "acute_load", "chronic_load_28d",
         "chronic_load_42d", "acute_chronic_ratio"],
        [Types.STRING(), Types.LONG(), Types.FLOAT(), Types.FLOAT(),
         Types.FLOAT(), Types.FLOAT()],
    )
    metrics_row_type = Types.ROW_NAMED(
        ["athlete_id", "metric_date", "acute_load", "chronic_load_28d",
         "chronic_load_42d", "acute_chronic_ratio", "deload_flag"],
        [Types.STRING(), Types.LONG(), Types.FLOAT(), Types.FLOAT(),
         Types.FLOAT(), Types.FLOAT(), Types.INT()],
    )

    # Indices into the canonical Row (source of truth: the DDL column order).
    IDX_EVENT_ID = 0
    IDX_EVENT_TIME = 1
    IDX_ATHLETE_ID = 5
    IDX_SESSION_LOAD = 19

    # Indices into the daily_load Row(athlete_id, day_start, daily_load).
    IDX_DL_ATHLETE_ID = 0
    IDX_DL_DAY_START = 1
    IDX_DL_DAILY_LOAD = 2

    # Indices into the acr Row(athlete_id, metric_date, acute_load,
    #   chronic_load_28d, chronic_load_42d, acute_chronic_ratio).
    IDX_ACR_ATHLETE_ID = 0
    IDX_ACR_METRIC_DATE = 1
    IDX_ACR_ACUTE = 2
    IDX_ACR_CHRONIC_28D = 3
    IDX_ACR_CHRONIC_42D = 4
    IDX_ACR = 5

    # Indices into the metrics Row (adds deload_flag at position 6).
    IDX_METRICS_DELOAD_FLAG = 6

    # --- DLQ / late side-output tags ----------------------------------------
    # NaN guard emits JSON strings; late side outputs carry the late elements.
    # Each OutputTag gets a FRESH TypeInformation instance (with fresh field
    # types) so the WindowOperationDescriptor cloudpickle is not broken by a
    # get_java_type_info()-mutated shared type (see _canonical_row_type note).
    dlq_nan_tag = OutputTag("dlq.metrics.nan", Types.STRING())
    late_daily_tag = OutputTag("late.metrics.daily", _canonical_row_type())
    late_rolling_tag = OutputTag("late.metrics.rolling", _daily_load_row_type())

    # --- source: canonical.training_event as Avro via Confluent Registry ----
    # The avro-confluent Table format is the ONLY PyFlink 1.19 path to the
    # Confluent-Registry Avro serde (DataStream ConfluentRegistryAvro* schemas
    # are Java-only -- PR3 ADR-15 finding). The DDL column types infer the Avro
    # READER schema; the writer schema (registered by the canonicalize sink, or
    # by the test's AvroSerializer) is resolved by embedded schema id.
    # event_time is BIGINT (the canonicalize sink emits plain-long event_time;
    # the metrics job reads it as epoch-ms). The watermark is assigned on the
    # DataStream side (to_data_stream + assign_timestamps_and_watermarks) which
    # mirrors the PR3 source-side assigner pattern and is robust regardless of
    # Table<->DataStream rowtime propagation quirks.
    bounded_options = (
        "'scan.bounded.mode' = 'latest-offset',\n"
        if config.bounded
        else ""
    )
    source_ddl = f"""
CREATE TABLE canonical_training_event_source (
  `event_id` STRING,
  `event_time` BIGINT,
  `ingest_time` BIGINT,
  `source` STRING,
  `schema_version` INT,
  `athlete_id` STRING,
  `event_type` STRING,
  `workout_id` STRING,
  `exercise_id` STRING,
  `set_number` INT,
  `reps` INT,
  `weight_kg` FLOAT,
  `rpe` FLOAT,
  `rir` FLOAT,
  `activity_type` STRING,
  `distance_km` FLOAT,
  `duration_sec` INT,
  `avg_hr` INT,
  `tss` FLOAT,
  `session_load` FLOAT
) WITH (
  'connector' = 'kafka',
  'topic' = '{config.canonical_topic}',
  'properties.bootstrap.servers' = '{config.bootstrap_servers}',
  'properties.group.id' = '{config.group_id}',
  'scan.startup.mode' = 'earliest-offset',
  {bounded_options}'key.format' = 'raw',
  'key.fields' = 'athlete_id',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = '{config.schema_registry_url}'
)
"""
    tbl_env.execute_sql(source_ddl)
    source_table = tbl_env.from_path("canonical_training_event_source")
    canonical_stream = tbl_env.to_data_stream(source_table)

    # --- event-time watermark on the DataStream (24h out-of-orderness) ------
    class _EventTimeAssigner(TimestampAssigner):  # type: ignore[misc]
        def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
            # value = canonical Row; event_time at IDX_EVENT_TIME (epoch-ms).
            try:
                et = value[IDX_EVENT_TIME]
                if isinstance(et, (int, float)) and not isinstance(et, bool):
                    return int(et)
            except Exception:
                pass
            return record_timestamp

    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_hours(WATERMARK_OUT_OF_ORDER_HOURS)
        ).with_timestamp_assigner(_EventTimeAssigner())
    )
    watermarked = canonical_stream.assign_timestamps_and_watermarks(watermark)

    # --- dedup (ValueState<bool> per event_id, 7d TTL) + NaN guard ----------
    class DedupAndGuardFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Dedup by event_id (LOCKED: 7d TTL) + route NaN/Inf session_load to DLQ.

        Keyed by event_id. First-seen -> yield the canonical Row unchanged (its
        record timestamp = event_time is preserved for downstream event-time
        windows). Duplicate -> dropped. NaN/Inf session_load -> DLQ side output
        as VALIDATION_FAILURE (spec DLQ scenario: session_load = NaN).
        """

        def open(self, runtime_context: Any) -> None:
            ttl = (
                StateTtlConfig.new_builder(Time.days(DEDUP_TTL_DAYS))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            self._seen = runtime_context.get_state(
                ValueStateDescriptor("seen-event-id", Types.BOOLEAN())
            )
            self._seen.enable_time_to_live(ttl)

        def process_element(self, value: Any, ctx: Any) -> None:
            if bool(self._seen.value()):
                return  # duplicate inside the 7d re-delivery window -> dropped
            session_load = value[IDX_SESSION_LOAD]
            if not is_finite_load(session_load):
                # NaN/Inf guard (task 5.4): route to DLQ, mark seen, drop.
                self._seen.update(True)
                yield dlq_nan_tag, json.dumps(
                    build_metrics_dlq_envelope(
                        original_key=value[IDX_ATHLETE_ID],
                        original_value=json.dumps(
                            {"event_id": value[IDX_EVENT_ID],
                             "session_load": session_load}
                        ),
                        error_type=VALIDATION_FAILURE,
                        error_message=f"session_load is not finite: {session_load!r}",
                        timestamp=epoch_ms_now(),
                    )
                )
                return
            self._seen.update(True)
            yield value  # unchanged; record timestamp preserved

    deduped = (
        watermarked
        .key_by(lambda row: row[IDX_EVENT_ID])
        .process(DedupAndGuardFunction(), output_type=canonical_row_type)
    )

    # --- daily pre-agg: TumblingEventTimeWindows(1d) summing session_load ---
    # AggregateFunction (incremental sum) + ProcessWindowFunction (attach
    # day_start = window.start). ContinuousEventTimeTrigger emits on update
    # (ADR-13 serving freshness); the rolling window dedupes by day downstream.
    class SumSessionLoadAgg(AggregateFunction):  # type: ignore[misc]
        """Incremental SUM of session_load over a daily window (accumulator = float)."""

        def create_accumulator(self) -> float:
            return 0.0

        def add(self, value: Any, accumulator: float) -> float:
            return float(accumulator) + float(value[IDX_SESSION_LOAD])

        def get_result(self, accumulator: float) -> float:
            return float(accumulator)

        def merge(self, a: float, b: float) -> float:
            return float(a) + float(b)

    class DailyLoadWindowFn(ProcessWindowFunction):  # type: ignore[misc]
        """Attach day_start (window.start) to the daily sum -> daily_load Row."""

        def process(
            self,
            key: Any,
            context: "ProcessWindowFunction.Context",
            sums: Any,
        ) -> Any:
            daily_sum = next(iter(sums))
            yield Row(key, context.window().start, float(daily_sum))

    daily_stream = (
        deduped
        .key_by(lambda row: row[IDX_ATHLETE_ID])
        .window(TumblingEventTimeWindows.of(Time.days(1)))
        .trigger(ContinuousEventTimeTrigger.of(Time.days(1)))
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .side_output_late_data(late_daily_tag)
        .aggregate(
            SumSessionLoadAgg(),
            window_function=DailyLoadWindowFn(),
            accumulator_type=Types.FLOAT(),
            output_type=daily_load_row_type,
        )
    )

    # --- rolling metrics: SlidingEventTimeWindows(42d, slide 1d) ------------
    # A single 42d sliding window whose ProcessWindowFunction computes acute
    # (last 7d SUM), chronic_28d (last 28d AVG), chronic_42d (all 42d AVG) and
    # ACR from the daily_loads in the window. daily_loads are deduped by day
    # (max -> final) to absorb the daily window's ContinuousEventTimeTrigger
    # multi-emit. See the module docstring for the design refinement note.
    class RollingMetricsWindowFn(ProcessWindowFunction):  # type: ignore[misc]
        """Compute acute/chronic_28d/chronic_42d/ACR from the 42d daily_loads."""

        def process(
            self,
            key: Any,
            context: "ProcessWindowFunction.Context",
            daily_loads: Any,
        ) -> Any:
            window_end = context.window().end  # epoch-ms, exclusive, day-aligned
            metric_date = window_end - MILLIS_PER_DAY  # last full day in window
            # Dedupe by day_start keeping the max daily_load (the final running
            # sum, since session_load >= 0 -> daily sum is monotonic within a
            # day). Absorbs ContinuousEventTimeTrigger partial + final emits.
            by_day: dict[int, float] = {}
            for dl in daily_loads:
                day = dl[IDX_DL_DAY_START]
                load = float(dl[IDX_DL_DAILY_LOAD])
                if day not in by_day or load > by_day[day]:
                    by_day[day] = load
            # acute_load = SUM of daily_load for the last 7 days [t-6, t].
            acute = sum(
                v for day, v in by_day.items()
                if window_end - 7 * MILLIS_PER_DAY <= day <= metric_date
            )
            # chronic_load_28d = AVG of daily_load for the last 28 days.
            c28 = [
                v for day, v in by_day.items()
                if window_end - 28 * MILLIS_PER_DAY <= day <= metric_date
            ]
            chronic_28d = (sum(c28) / len(c28)) if c28 else 0.0
            # chronic_load_42d = AVG of daily_load for the last 42 days.
            c42 = [
                v for day, v in by_day.items()
                if window_end - 42 * MILLIS_PER_DAY <= day <= metric_date
            ]
            chronic_42d = (sum(c42) / len(c42)) if c42 else 0.0
            acr_val = acute_chronic_ratio(acute, chronic_28d)  # None if chronic=0
            # ACR None (chronic==0) cannot cross a FLOAT Row field boundary
            # safely in PyFlink 1.19 — FLOAT TypeInfo is non-nullable at the
            # Java level. Encode None as float('nan') in the Row so the FLOAT
            # type is satisfied while preserving the "no ratio" semantic.
            # DeloadKeyedProcessFunction detects math.isnan(acr) to treat it
            # identically to None (streak RESET, never DELOAD_LOW). (C3+F2)
            acr_wire = float("nan") if acr_val is None else float(acr_val)
            yield Row(key, metric_date, float(acute), float(chronic_28d),
                      float(chronic_42d), acr_wire)

    acr_stream = (
        daily_stream
        .key_by(lambda row: row[IDX_DL_ATHLETE_ID])  # athlete_id
        .window(SlidingEventTimeWindows.of(Time.days(42), Time.days(1)))
        .allowed_lateness(ALLOWED_LATENESS_MS)
        .side_output_late_data(late_rolling_tag)
        .process(RollingMetricsWindowFn(), output_type=acr_row_type)
    )

    # --- deload flag: KeyedProcessFunction over the daily ACR stream --------
    # ValueState<(last_day, count, sign)>; pure update_deload_state() advances
    # the consecutive-day counter. Idempotent per day: skips re-fires for the
    # same day (the 42d window may re-emit at allowed-lateness close).
    # Deload state TTL: 56 days (>= 42d window horizon + 14d safety margin).
    # Rationale (RESILIENCE F1 fix): without TTL the deload ValueState retains
    # one entry per athlete_id forever in RocksDB, growing unboundedly in a
    # long-running job. 56d covers the full 42d chronic window + the 14d grace
    # period of the longest raw-topic retention (raw.recovery/planning = 14d),
    # so no in-window metric computation is evicted. An athlete inactive for
    # longer than 56d has no chronic baseline anyway; state expiry is correct.
    DELOAD_STATE_TTL_DAYS = 56

    class DeloadKeyedProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """deload_flag consecutive-day rule over the ordered daily ACR stream."""

        def open(self, runtime_context: Any) -> None:
            # StateTtlConfig: same pattern as the dedup ValueState (7d TTL).
            # 56d TTL >= 42d window horizon (RESILIENCE F1). (C4+F1)
            ttl = (
                StateTtlConfig.new_builder(Time.days(DELOAD_STATE_TTL_DAYS))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            desc = ValueStateDescriptor(
                "deload-state",
                Types.TUPLE([Types.LONG(), Types.INT(), Types.INT()]),
            )
            self._state = runtime_context.get_state(desc)
            self._state.enable_time_to_live(ttl)

        def process_element(self, value: Any, ctx: Any) -> None:
            # value = acr Row(athlete_id, metric_date, acute, chronic_28d,
            #                 chronic_42d, acr)
            metric_date = value[1]
            acr_raw = value[IDX_ACR]
            # ACR is encoded as float('nan') when chronic_28d==0 (C3+F2).
            # Treat NaN identically to None: no streak assertion possible.
            acr = None if (acr_raw is None or (isinstance(acr_raw, float) and math.isnan(acr_raw))) else acr_raw
            st = self._state.value()
            if st is None:
                last_day, count, sign = -1, 0, 0
            else:
                last_day, count, sign = st
            if metric_date == last_day:
                return  # idempotent: skip re-fire for the same day
            if metric_date < last_day:
                return  # out of order (event-time windows keep order; safety)
            new_count, new_sign, flag = update_deload_state(count, sign, acr)
            self._state.update((int(metric_date), int(new_count), int(new_sign)))
            yield Row(
                value[IDX_ACR_ATHLETE_ID],
                value[IDX_ACR_METRIC_DATE],
                value[IDX_ACR_ACUTE],
                value[IDX_ACR_CHRONIC_28D],
                value[IDX_ACR_CHRONIC_42D],
                value[IDX_ACR],
                int(flag),
            )

    metrics_stream = (
        acr_stream
        .key_by(lambda row: row[IDX_ACR_ATHLETE_ID])  # athlete_id
        .process(DeloadKeyedProcessFunction(), output_type=metrics_row_type)
    )

    # --- metrics output sink (staging Kafka topic for PR4 assertion) --------
    # PR4 outputs a metrics DataStream to a staging Kafka topic (JSON,
    # AT_LEAST_ONCE). The PostgreSQL + Iceberg exactly-once sinks are PR5.
    def _metrics_row_to_json(row: Any) -> str:
        acr_v = row[IDX_ACR]
        # NaN ACR (chronic==0) serializes as JSON null (C3+F2).
        acr_json = None if (acr_v is None or (isinstance(acr_v, float) and math.isnan(acr_v))) else float(acr_v)
        return json.dumps({
            "athlete_id": row[IDX_ACR_ATHLETE_ID],
            "metric_date": row[IDX_ACR_METRIC_DATE],
            "acute_load": row[IDX_ACR_ACUTE],
            "chronic_load_28d": row[IDX_ACR_CHRONIC_28D],
            "chronic_load_42d": row[IDX_ACR_CHRONIC_42D],
            "acute_chronic_ratio": acr_json,
            "deload_flag": row[IDX_METRICS_DELOAD_FLAG],
        })

    metrics_json_stream = metrics_stream.map(
        _metrics_row_to_json, output_type=Types.STRING()
    )

    metrics_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.metrics_output_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    metrics_json_stream.sink_to(metrics_sink)

    # --- DLQ sink: NaN guard + late side outputs -> dlq.canonical.training_event
    # (JSON, AT_LEAST_ONCE per design ADR-12).
    nan_dlq = deduped.get_side_output(dlq_nan_tag)  # DataStream[STRING]

    def _late_canonical_to_json(row: Any) -> str:
        return json.dumps(
            build_metrics_dlq_envelope(
                original_key=row[IDX_ATHLETE_ID],
                original_value=json.dumps(
                    {"event_id": row[IDX_EVENT_ID], "event_time": row[IDX_EVENT_TIME]}
                ),
                error_type=LATE_DATA,
                error_message="event arrived past daily window-end + 24h allowed lateness",
                timestamp=epoch_ms_now(),
            )
        )

    def _late_dailyload_to_json(row: Any) -> str:
        return json.dumps(
            build_metrics_dlq_envelope(
                original_key=row[IDX_DL_ATHLETE_ID],
                original_value=json.dumps(
                    {"day_start": row[IDX_DL_DAY_START], "daily_load": row[IDX_DL_DAILY_LOAD]}
                ),
                error_type=LATE_DATA,
                error_message="daily_load arrived past rolling window-end + 24h allowed lateness",
                timestamp=epoch_ms_now(),
            )
        )

    late_daily_json = (
        daily_stream.get_side_output(late_daily_tag)
        .map(_late_canonical_to_json, output_type=Types.STRING())
    )
    late_rolling_json = (
        acr_stream.get_side_output(late_rolling_tag)
        .map(_late_dailyload_to_json, output_type=Types.STRING())
    )

    dlq_combined = nan_dlq.union(late_daily_json).union(late_rolling_json)

    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.dlq_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    dlq_combined.sink_to(dlq_sink)

    # Submit the full DAG (Table avro-confluent source + DataStream windowing +
    # metrics output sink + DLQ sink) as a single Flink job.
    env.execute("athleteos-metrics-job")


def main() -> int:  # pragma: no cover - entrypoint
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    group = os.environ.get("METRICS_GROUP_ID", "metrics-training-event")
    config = MetricsJobConfig(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        group_id=group,
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
