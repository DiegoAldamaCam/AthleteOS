"""PyFlink planning canonicalize job wiring (PR-PL2b).

Import-isolation contract
=========================
apache-flink has no wheel for CPython 3.14 (grpcio-tools / apache-beam build
fails). To keep the package importable and ``pytest --collect-only`` working,
**all pyflink imports are LAZY** — they live inside ``run()`` on a flink-capable
runtime. The PURE mapping/validation/DLQ logic lives in
:mod:`jobs.planning_canonicalize.transform`, which imports WITHOUT pyflink and is
fully unit-tested (tests/unit/test_planning_transform.py).

The integration slice (tests/integration/test_planning_canonicalize_job.py)
exercises this wiring end-to-end when apache-flink IS installed and Docker is
up; it SKIPS cleanly otherwise (never fakes a pass).

Job topology (design ADR-20)
============================
raw.planning → canonical.planning_block (Avro) + dlq.canonical.planning_block:

    KafkaSource(raw.planning, SimpleStringSchema-JSON)
      .assign_watermark(WatermarkStrategy.for_bounded_out_of_orderness(24h)
                        .with_timestamp_assigner(_EventTimeAssigner()))
        # _EventTimeAssigner reads epoch-ms event_time directly from the
        # planning envelope (the P1 producer emits epoch-ms longs at UTC
        # midnight of start_date). Falls back to record_timestamp on parse error.
      -> key_by(athlete_id)         # ADR-4 co-partitioning (NOT event_id)
      -> .process(PlanningCanonicalizeProcessFunction, output_type=...)
        # MapState<event_id, bool> seen_events (7d TTL per entry, OnCreateAndWrite
        # + NeverReturnExpired). ADR-20: NO block_id dedup — repeat block_id is a
        # NEW plan revision, never dropped. Emits via ``yield``:
        #   -> main:  yield Row(... canonical PlanningBlock ...)
        #   -> side:  yield dlq_tag, json.dumps(build_dlq_envelope(...))
      [main] -> StreamTableEnvironment.from_data_stream(canonical_table)
              -> avro-confluent Table DDL sink → canonical.planning_block
                 (all primitive types; no enum concern — PlanningBlock.avsc uses
                 primitives only, EXACTLY_ONCE)
      [side] -> KafkaSink(dlq.canonical.planning_block, JSON, AT_LEAST_ONCE)

ADR-20: Block identity = VERSIONING, not dedup-by-key
======================================================
Planning keys the stream by athlete_id (ADR-4) so all events for the same
athlete co-locate in one Flink partition. The ProcessFunction deduplicates ONLY
by event_id using MapState<event_id, bool> to guarantee idempotent reprocessing.

CRITICAL — why MapState, not ValueState:
  The operator key is athlete_id. A ValueState<bool> has ONE cell per operator
  key, i.e. one boolean per athlete. After event_A is processed and
  state.update(True) is called, the NEXT event for the same athlete (regardless
  of its event_id) would see state.value() == True and be silently dropped.
  MapState<event_id, bool> provides one cell PER event_id within the athlete
  partition — exactly the per-event_id dedup semantics required.

The ProcessFunction MUST NOT carry any MapState keyed on block_id — dropping a
repeat block_id would discard a plan revision (the exact anti-goal). The PG PK
(athlete_id, block_id, ingest_time) absorbs multiple revisions without conflict
(ADR-21 DO NOTHING).
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

# Topic and group constants (import-safe, no pyflink).
RAW_TOPIC = "raw.planning"
CANONICAL_TOPIC = "canonical.planning_block"
DLQ_TOPIC = "dlq.canonical.planning_block"
SOURCE_NAME = "planning-canonicalize-source"

DEDUP_TTL_DAYS = 7
WATERMARK_OUT_OF_ORDER_HOURS = 24

_DEFAULT_SCHEMA_VERSION_FALLBACK = 1

_DDL_FORBIDDEN_CHARS: str = "'\"\n\r\x00"


def _validate_ddl_config_field(field_name: str, value: str) -> None:
    for ch in _DDL_FORBIDDEN_CHARS:
        if ch in value:
            raise ValueError(
                f"PlanningCanonicalizeJobConfig.{field_name} contains a character that is "
                f"forbidden in DDL interpolation (char ord={ord(ch):#04x}). "
                f"Accepted characters: printable ASCII excluding '\"\\n\\r\\x00. "
                f"Received value (first 80 chars): {value[:80]!r}"
            )


def _epoch_ms_now() -> int:
    return int(time.time() * 1000)


class PlanningCanonicalizeJobConfig:
    """Plain configuration container (no pyflink). Import-safe.

    Mirrors WellnessCanonicalizeJobConfig from jobs/wellness_canonicalize/main.py
    exactly, with planning-specific topic defaults.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id: str = "canonicalize-planning",
        raw_topic: str = RAW_TOPIC,
        canonical_topic: str = CANONICAL_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
        checkpoint_interval_ms: int = 60_000,
        schema_version: int | None = None,
        bounded: bool = False,
        parallelism: int | None = None,
        no_restart: bool = False,
    ) -> None:
        _validate_ddl_config_field("bootstrap_servers", bootstrap_servers)
        _validate_ddl_config_field("schema_registry_url", schema_registry_url)
        _validate_ddl_config_field("canonical_topic", canonical_topic)
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.group_id = group_id
        self.raw_topic = raw_topic
        self.canonical_topic = canonical_topic
        self.dlq_topic = dlq_topic
        self.checkpoint_interval_ms = checkpoint_interval_ms
        self.schema_version = schema_version
        # bounded=True: read from earliest to LATEST offset captured at startup,
        # then send MAX_WATERMARK and finish (integration-test mode).
        self.bounded = bounded
        self.parallelism = parallelism
        # no_restart=True: disable restart strategy so crashes surface
        # immediately (integration-test mode; production keeps default).
        self.no_restart = no_restart

    def effective_schema_version(self) -> int:
        return (
            self.schema_version
            if self.schema_version is not None
            else _DEFAULT_SCHEMA_VERSION_FALLBACK
        )


# ---------------------------------------------------------------------------
# Job wiring (PYFLINK-DEPENDENT). Imported lazily. Do not call at import time.
# ---------------------------------------------------------------------------


def run(config: PlanningCanonicalizeJobConfig) -> None:  # pragma: no cover - flink runtime
    """Build and execute the planning canonicalize job.

    All pyflink imports are INSIDE this function so the module imports cleanly
    on interpreters without apache-flink.
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
        OutputTag,
        StreamExecutionEnvironment,
        RuntimeExecutionMode,
    )
    from pyflink.datastream.connectors.kafka import (
        DeliveryGuarantee,
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )
    from pyflink.datastream.functions import KeyedProcessFunction
    from pyflink.datastream.state import MapStateDescriptor, StateTtlConfig
    from pyflink.table import (
        EnvironmentSettings,
        StreamTableEnvironment,
    )

    from jobs.planning_canonicalize.transform import (
        build_dlq_envelope,
        select_dlq_error_type,
        transform_planning_to_canonical,
        validate_planning_block,
        TransformError,
        ValidationError,
    )

    # --- environment --------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    if config.parallelism is not None:
        env.set_parallelism(config.parallelism)
    if config.no_restart:
        from pyflink.common import RestartStrategies
        env.set_restart_strategy(RestartStrategies.no_restart())
    env.enable_checkpointing(config.checkpoint_interval_ms)

    table_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=table_settings)

    schema_version = config.effective_schema_version()

    # DLQ side-output tag: JSON string payloads.
    dlq_tag = OutputTag("dlq", Types.STRING())

    # --- source: raw.planning as JSON string --------------------------------
    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.raw_topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if config.bounded:
        source_builder = source_builder.set_bounded(
            KafkaOffsetsInitializer.latest()
        )
    source = source_builder.build()

    # Event-time assigner: reads epoch-ms ``event_time`` directly from the
    # raw.planning JSON envelope. The P1 producer emits epoch-ms longs
    # (UTC midnight of start_date — W1-5 pattern for planning). Falls back
    # to record_timestamp on parse error; the ProcessFunction downstream
    # catches and routes bad records to the DLQ.
    class _EventTimeAssigner(TimestampAssigner):  # type: ignore[misc]
        def extract_timestamp(self, value: str, record_timestamp: int) -> int:
            try:
                envelope = json.loads(value)
                ts = (
                    envelope.get("event_time")
                    if isinstance(envelope, dict)
                    else None
                )
            except (TypeError, ValueError):
                return record_timestamp
            if isinstance(ts, int):
                return ts
            if isinstance(ts, float) and ts.is_integer():
                return int(ts)
            return record_timestamp

    watermark = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_hours(WATERMARK_OUT_OF_ORDER_HOURS)
        ).with_timestamp_assigner(_EventTimeAssigner())
    )

    raw_stream = env.from_source(
        source=source,
        watermark_strategy=watermark,
        source_name=SOURCE_NAME,
    )

    # Canonical Row field layout (order matches schemas/canonical/PlanningBlock.avsc).
    # All 12 fields; all primitive types (no enum — PlanningBlock.avsc has no enum).
    canonical_field_names = (
        "event_id", "event_time", "ingest_time", "source", "schema_version",
        "athlete_id", "block_id", "goal",
        "start_date", "end_date",
        "planned_sessions_per_week",
        "weekly_volume_targets",
    )
    canonical_row_type = Types.ROW_NAMED(
        list(canonical_field_names),
        [
            Types.STRING(), Types.LONG(), Types.LONG(), Types.STRING(), Types.INT(),
            Types.STRING(), Types.STRING(), Types.STRING(),  # event_id..goal
            Types.LONG(), Types.LONG(),                      # start_date, end_date (epoch-ms)
            Types.INT(),                                     # planned_sessions_per_week
            Types.STRING(),                                  # weekly_volume_targets (JSON str)
        ],
    )

    class PlanningCanonicalizeProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Dedup (MapState<event_id, bool> per athlete, 7d TTL per entry) + validate + transform.

        ADR-20: Keyed by ``athlete_id`` (ADR-4 co-partitioning). Dedup is
        event_id ONLY via MapState<str, bool> — each event_id gets its own
        map entry within the per-athlete state partition. NO block_id state —
        repeat block_id = new plan revision (kept, not dropped). First-seen
        event_id → validate + transform + yield canonical Row; duplicate
        event_id → silently dropped (PL2-2). ValidationError / TransformError
        → DLQ side output (PL2-3/PL2-6/PL2-7/PL2-8).

        WHY MapState, not ValueState:
          The operator key is athlete_id. ValueState<bool> would provide ONE
          cell per athlete — after the first event is processed, all subsequent
          events for the same athlete would be silently dropped regardless of
          their event_id. MapState<event_id, bool> gives one cell PER event_id
          within the athlete partition, which is the correct dedup granularity.

        PyFlink KeyedProcessFunction emits via ``yield``; Java Collector does
        NOT exist on the Python side.
        """

        def __init__(self, field_names: tuple, schema_version: int) -> None:
            self._field_names = field_names
            self._schema_version = schema_version

        def open(self, runtime_context: Any) -> None:
            ttl = (
                StateTtlConfig.new_builder(Time.days(DEDUP_TTL_DAYS))
                .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build()
            )
            # ADR-20: event_id dedup via MapState<event_id, bool>.
            # Each map entry corresponds to one distinct event_id seen for the
            # current athlete partition. The descriptor name makes ADR-20
            # compliance auditable via source inspection.
            # TTL applies per-entry: entries expire 7d after last write.
            self._seen_events = runtime_context.get_map_state(
                MapStateDescriptor(
                    "seen-planning-event-ids",
                    Types.STRING(),
                    Types.BOOLEAN(),
                )
            )
            self._seen_events.enable_time_to_live(ttl)

        def process_element(self, value: str, ctx: Any) -> None:
            # Parse the raw JSON envelope first to extract event_id for dedup.
            original_value = value
            try:
                raw = json.loads(value)
            except (TypeError, ValueError) as json_exc:
                err = TransformError(f"raw value is not valid JSON: {json_exc}")
                yield dlq_tag, json.dumps(
                    build_dlq_envelope(
                        original_topic=config.raw_topic,
                        original_key=None,
                        original_value=original_value,
                        error_type=select_dlq_error_type(err),
                        error_message=str(err),
                        timestamp=_epoch_ms_now(),
                    )
                )
                return

            athlete_id = raw.get("athlete_id") if isinstance(raw, dict) else None
            event_id = raw.get("event_id") if isinstance(raw, dict) else None

            # Dedup: MapState<event_id, bool> per athlete (7d TTL per entry).
            # self._seen_events.contains(event_id) returns True only if this
            # specific event_id has been processed before for this athlete.
            # Different event_ids for the same athlete each get their own entry.
            if event_id and self._seen_events.contains(event_id):
                return  # duplicate within 7d re-delivery window → silently dropped (PL2-2)

            try:
                validate_planning_block(raw)
                canonical = transform_planning_to_canonical(raw, self._schema_version)
            except (ValidationError, TransformError) as exc:
                if event_id:
                    self._seen_events.put(event_id, True)  # avoid re-routing the same bad event
                yield dlq_tag, json.dumps(
                    build_dlq_envelope(
                        original_topic=config.raw_topic,
                        original_key=athlete_id,
                        original_value=original_value,
                        error_type=select_dlq_error_type(exc),
                        error_message=str(exc),
                        timestamp=_epoch_ms_now(),
                    )
                )
                return

            # Mark event_id seen and emit canonical Row.
            if event_id:
                self._seen_events.put(event_id, True)
            yield Row(*[canonical[f] for f in self._field_names])

    # --- transform pipeline -------------------------------------------------
    # ADR-4: key_by athlete_id (co-partitioning). Dedup is event_id-based
    # inside the ProcessFunction via MapState<event_id, bool>; the operator
    # key is athlete_id so all events for one athlete land in one partition.
    transformed = (
        raw_stream
        .key_by(lambda raw_str: json.loads(raw_str).get("athlete_id") or "")
        .process(
            PlanningCanonicalizeProcessFunction(canonical_field_names, schema_version),
            output_type=canonical_row_type,
        )
    )

    # DLQ side output → JSON KafkaSink (AT_LEAST_ONCE per design).
    dlq_stream = transformed.get_side_output(dlq_tag)

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
    dlq_stream.sink_to(dlq_sink)

    # --- canonical main stream: avro-confluent Table DDL sink ---------------
    # PlanningBlock.avsc uses primitive types only (no enum concern).
    # STRING maps to Avro {"type":"string"}; LONG maps to {"type":"long"} with
    # timestamp-millis logical type auto-detected from the registered schema.
    # TopicNameStrategy (ADR-10): subject = canonical.planning_block-value.
    sink_ddl = f"""
CREATE TABLE canonical_planning_block_sink (
  `event_id` STRING,
  `event_time` BIGINT,
  `ingest_time` BIGINT,
  `source` STRING,
  `schema_version` INT,
  `athlete_id` STRING,
  `block_id` STRING,
  `goal` STRING,
  `start_date` BIGINT,
  `end_date` BIGINT,
  `planned_sessions_per_week` INT,
  `weekly_volume_targets` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '{config.canonical_topic}',
  'properties.bootstrap.servers' = '{config.bootstrap_servers}',
  'key.format' = 'raw',
  'key.fields' = 'athlete_id',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = '{config.schema_registry_url}',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'athleteos-canonicalize-planning-block'
)
"""
    tbl_env.execute_sql(sink_ddl)

    # Lift canonical Row DataStream into a Table and INSERT into the
    # avro-confluent sink via StatementSet + attach_as_datastream() so the
    # DLQ DataStream sink and the canonical Table sink run as ONE Flink job.
    canonical_table = tbl_env.from_data_stream(transformed)
    statement_set = tbl_env.create_statement_set()
    statement_set.add_insert("canonical_planning_block_sink", canonical_table)
    statement_set.attach_as_datastream()

    env.execute("athleteos-planning-canonicalize-job")


def main() -> int:  # pragma: no cover - entrypoint
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    group = os.environ.get("PLANNING_CANONICALIZE_GROUP_ID", "canonicalize-planning")
    sv_env = os.environ.get("SCHEMA_VERSION_OVERRIDE")
    config = PlanningCanonicalizeJobConfig(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        group_id=group,
        schema_version=int(sv_env) if sv_env else None,
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
