"""PyFlink wellness canonicalize job wiring (PR-W2).

Import-isolation contract
=========================
apache-flink has no wheel for CPython 3.14 (grpcio-tools / apache-beam build
fails). To keep the package importable and ``pytest --collect-only`` working,
**all pyflink imports are LAZY** — they live inside ``run()`` on a flink-capable
runtime. The PURE mapping/validation/DLQ logic lives in
:mod:`jobs.wellness_canonicalize.transform`, which imports WITHOUT pyflink and is
fully unit-tested (tests/unit/test_wellness_transform.py).

The integration slice (tests/integration/test_wellness_canonicalize_job.py)
exercises this wiring end-to-end when apache-flink IS installed and Docker is
up; it SKIPS cleanly otherwise (never fakes a pass).

Job topology (design #133)
==========================
raw.wellness → canonical.wellness_event, wellness-sourced:

    KafkaSource(raw.wellness, SimpleStringSchema-JSON)
      .assign_watermark(WatermarkStrategy.for_bounded_out_of_orderness(24h)
                        .with_timestamp_assigner(_EventTimeAssigner()))
        # _EventTimeAssigner reads epoch-ms event_time directly from the
        # wellness envelope (no ISO→epoch conversion needed; the W1 producer
        # emits epoch-ms longs). Falls back to record_timestamp on parse error.
      -> key_by(event_id)           # dedup keyed by event_id (LOCKED)
      -> .process(WellnessCanonicalizeProcessFunction, output_type=...)
        # ValueState<bool> seen(event_id), StateTtlConfig 7d (OnCreateAndWrite
        # + NeverReturnExpired). Emits via ``yield``:
        #   -> main:  yield Row(... canonical WellnessEvent ...)
        #   -> side:  yield dlq_tag, json.dumps(build_dlq_envelope(...))
      [main] -> StreamTableEnvironment.from_data_stream(canonical_table)
              -> avro-confluent Table DDL sink (event_type STRING — ADR-16,
                 RAW athlete_id key, EXACTLY_ONCE)
      [side] -> KafkaSink(dlq.canonical.wellness_event, JSON, AT_LEAST_ONCE)

ADR-16: event_type STRING in the Table DDL sink
========================================================
Flink 1.19 avro-confluent Table DDL has no Avro enum type. Declaring
``event_type STRING`` in the DDL emits Avro ``{"type":"string"}`` on the wire,
which matches the migrated WellnessEvent.avsc (enum→string per ADR-16). The
former enum's semantic guarantee is enforced at the application layer by
validate_wellness_event() in transform.py.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

# Topic and group constants (import-safe, no pyflink).
RAW_TOPIC = "raw.wellness"
CANONICAL_TOPIC = "canonical.wellness_event"
DLQ_TOPIC = "dlq.canonical.wellness_event"
SOURCE_NAME = "wellness-canonicalize-source"

DEDUP_TTL_DAYS = 7
WATERMARK_OUT_OF_ORDER_HOURS = 24

_DEFAULT_SCHEMA_VERSION_FALLBACK = 1


def _epoch_ms_now() -> int:
    return int(time.time() * 1000)


class WellnessCanonicalizeJobConfig:
    """Plain configuration container (no pyflink). Import-safe.

    Mirrors CanonicalizeJobConfig from jobs/canonicalize/main.py exactly, with
    wellness-specific topic defaults.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id: str = "canonicalize-wellness",
        raw_topic: str = RAW_TOPIC,
        canonical_topic: str = CANONICAL_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
        checkpoint_interval_ms: int = 60_000,
        schema_version: int | None = None,
        bounded: bool = False,
        parallelism: int | None = None,
        no_restart: bool = False,
    ) -> None:
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


def run(config: WellnessCanonicalizeJobConfig) -> None:  # pragma: no cover - flink runtime
    """Build and execute the wellness canonicalize job.

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
    from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
    from pyflink.table import (
        EnvironmentSettings,
        StreamTableEnvironment,
    )

    from jobs.wellness_canonicalize.transform import (
        build_dlq_envelope,
        select_dlq_error_type,
        transform_wellness_to_canonical,
        validate_wellness_event,
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

    # --- source: raw.wellness as JSON string --------------------------------
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
    # raw.wellness JSON envelope. The wellness producer already emits epoch-ms
    # longs (not ISO strings — W1-5 intentional divergence from strength).
    # Falls back to record_timestamp on parse error; the ProcessFunction
    # downstream catches and routes bad records to the DLQ.
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

    # Canonical Row field layout (order matches schemas/canonical/WellnessEvent.avsc).
    canonical_field_names = (
        "event_id", "event_time", "ingest_time", "source", "schema_version",
        "athlete_id", "event_type",
        "sleep_hours", "resting_hr", "hrv", "steps", "body_weight_kg",
        "calories", "protein_g", "carbs_g", "fat_g", "nutrition_adherence",
        "energy", "soreness", "mood", "stress", "perceived_recovery",
    )
    canonical_row_type = Types.ROW_NAMED(
        list(canonical_field_names),
        [
            Types.STRING(), Types.LONG(), Types.LONG(), Types.STRING(), Types.INT(),
            Types.STRING(), Types.STRING(),  # event_type STRING (ADR-16)
            Types.FLOAT(), Types.INT(), Types.FLOAT(), Types.INT(), Types.FLOAT(),
            Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
            Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(),
        ],
    )

    class WellnessCanonicalizeProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Dedup (ValueState<bool> per event_id, 7d TTL) + transform + validate.

        Keyed by ``event_id`` (LOCKED). First-seen → transform+validate+yield
        canonical Row; duplicate → silently dropped (W2-6). ValidationError /
        TransformError → DLQ side output (W2-5).

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
            self._seen = runtime_context.get_state(
                ValueStateDescriptor("seen-wellness-event-id", Types.BOOLEAN())
            )
            self._seen.enable_time_to_live(ttl)

        def process_element(self, value: str, ctx: Any) -> None:
            # Dedup: ValueState<bool> keyed by event_id (7d TTL)
            if bool(self._seen.value()):
                return  # duplicate within 7d re-delivery window → silently dropped (W2-6)

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
            try:
                canonical = transform_wellness_to_canonical(raw, self._schema_version)
                validate_wellness_event(canonical)
            except (ValidationError, TransformError) as exc:
                self._seen.update(True)  # mark to avoid re-routing the same bad event
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

            # Mark seen and emit canonical Row.
            self._seen.update(True)
            yield Row(*[canonical[f] for f in self._field_names])

    # --- transform pipeline -------------------------------------------------
    transformed = (
        raw_stream
        .key_by(lambda raw: json.loads(raw).get("event_id") or "")
        .process(
            WellnessCanonicalizeProcessFunction(canonical_field_names, schema_version),
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
    # ADR-16: event_type is declared as STRING in the DDL (not enum). The Flink
    # 1.19 avro-confluent format infers the Avro writer schema from DDL column
    # types; STRING maps to Avro {"type":"string"}, which matches the migrated
    # WellnessEvent.avsc. This avoids the HTTP 409 enum↔string mismatch at the
    # Schema Registry (identical fix to ADR-15 for TrainingEvent).
    sink_ddl = f"""
CREATE TABLE canonical_wellness_event_sink (
  `event_id` STRING,
  `event_time` BIGINT,
  `ingest_time` BIGINT,
  `source` STRING,
  `schema_version` INT,
  `athlete_id` STRING,
  `event_type` STRING,
  `sleep_hours` FLOAT,
  `resting_hr` INT,
  `hrv` FLOAT,
  `steps` INT,
  `body_weight_kg` FLOAT,
  `calories` INT,
  `protein_g` FLOAT,
  `carbs_g` FLOAT,
  `fat_g` FLOAT,
  `nutrition_adherence` FLOAT,
  `energy` INT,
  `soreness` INT,
  `mood` INT,
  `stress` INT,
  `perceived_recovery` INT
) WITH (
  'connector' = 'kafka',
  'topic' = '{config.canonical_topic}',
  'properties.bootstrap.servers' = '{config.bootstrap_servers}',
  'key.format' = 'raw',
  'key.fields' = 'athlete_id',
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = '{config.schema_registry_url}',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'athleteos-canonicalize-wellness-event'
)
"""
    tbl_env.execute_sql(sink_ddl)

    # Lift canonical Row DataStream into a Table and INSERT into the
    # avro-confluent sink via StatementSet + attach_as_datastream() so the
    # DLQ DataStream sink and the canonical Table sink run as ONE Flink job.
    canonical_table = tbl_env.from_data_stream(transformed)
    statement_set = tbl_env.create_statement_set()
    statement_set.add_insert("canonical_wellness_event_sink", canonical_table)
    statement_set.attach_as_datastream()

    env.execute("athleteos-wellness-canonicalize-job")


def main() -> int:  # pragma: no cover - entrypoint
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    group = os.environ.get("WELLNESS_CANONICALIZE_GROUP_ID", "canonicalize-wellness")
    sv_env = os.environ.get("SCHEMA_VERSION_OVERRIDE")
    config = WellnessCanonicalizeJobConfig(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        group_id=group,
        schema_version=int(sv_env) if sv_env else None,
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
