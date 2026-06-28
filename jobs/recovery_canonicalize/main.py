"""PyFlink recovery canonicalize job wiring (PR-R2).

Import-isolation contract
=========================
apache-flink has no wheel for CPython 3.14 (grpcio-tools / apache-beam build
fails). To keep the package importable and ``pytest --collect-only`` working,
**all pyflink imports are LAZY** — they live inside ``run()`` on a flink-capable
runtime. The PURE mapping/validation/DLQ logic lives in
:mod:`jobs.recovery_canonicalize.transform`, which imports WITHOUT pyflink and is
fully unit-tested (tests/unit/test_recovery_transform.py).

The integration slice (tests/integration/test_recovery_canonicalize_job.py)
exercises this wiring end-to-end when apache-flink IS installed and Docker is
up; it SKIPS cleanly otherwise (never fakes a pass).

Job topology (design #225)
==========================
raw.recovery → canonical.wellness_event (RECOVERY_SNAPSHOT events):

    KafkaSource(raw.recovery, SimpleStringSchema-JSON)
      .assign_watermark(WatermarkStrategy.for_bounded_out_of_orderness(24h)
                        .with_timestamp_assigner(_EventTimeAssigner()))
        # _EventTimeAssigner reads epoch-ms event_time directly from the
        # raw.recovery envelope (W1-5 compliant: producer emits epoch-ms longs).
      -> key_by(event_id)           # dedup keyed by event_id (LOCKED)
      -> .process(RecoveryCanonicalizeProcessFunction, output_type=...)
        # ValueState<bool> seen(event_id), StateTtlConfig 7d (OnCreateAndWrite
        # + NeverReturnExpired). Emits via ``yield``:
        #   -> main:  yield Row(... canonical WellnessEvent ...)
        #   -> side:  yield dlq_tag, json.dumps(build_dlq_envelope(...))
      [main] -> StreamTableEnvironment.from_data_stream(canonical_table)
              -> avro-confluent Table DDL sink (existing WellnessEvent schema,
                 22-col, RAW athlete_id key, EXACTLY_ONCE)
      [side] -> KafkaSink(dlq.canonical.wellness_event, JSON, AT_LEAST_ONCE)
                (original_topic="raw.recovery" disambiguates from wellness DLQ)

ADR-R2: transactional_id_prefix = "athleteos-canonicalize-recovery-wellness-event"
=========================================================================
Disjoint from:
  - strength:  "athleteos-canonicalize-training-event"
  - wellness:  "athleteos-canonicalize-wellness-event"
  - planning:  "athleteos-canonicalize-planning-block"
  - cardio:    "athleteos-canonicalize-cardio-training-event"
Kafka EXACTLY_ONCE is per-transactional-ID; disjoint prefixes → no
ProducerFencedException between recovery and wellness jobs writing the same
canonical.wellness_event topic (sc-23).

ADR-R1: event_type = RECOVERY_SNAPSHOT (hardcoded in transform.py, not read
from payload). Verified at the unit layer by TRANSACTIONAL_ID_PREFIX test (sc-23)
and the transform unit tests (sc-15, sc-16).
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

# Constants are import-safe (no pyflink). They describe the topology.
RAW_TOPIC = "raw.recovery"
CANONICAL_TOPIC = "canonical.wellness_event"
DLQ_TOPIC = "dlq.canonical.wellness_event"
SOURCE_NAME = "recovery-canonicalize-source"

# ADR-R2: distinct transactional_id_prefix — disjoint from strength/wellness/planning/cardio.
# sc-23 asserts this EXACT literal string; do NOT change without updating the test.
TRANSACTIONAL_ID_PREFIX = "athleteos-canonicalize-recovery-wellness-event"

# Dedup ValueState<bool> per event_id, 7d TTL (LOCKED design, mirrors wellness/cardio).
DEDUP_TTL_DAYS = 7
# Bounded out-of-orderness: 24h (recovery uploads land late, same as wellness/cardio).
WATERMARK_OUT_OF_ORDER_HOURS = 24

_DEFAULT_SCHEMA_VERSION_FALLBACK = 1


def _epoch_ms_now() -> int:
    """Wall-clock now as epoch-ms long (used for DLQ envelope timestamp)."""
    return int(time.time() * 1000)


class RecoveryCanonicalizeJobConfig:
    """Plain configuration container (no pyflink). Import-safe.

    Mirrors WellnessCanonicalizeJobConfig / CardioCanonicalizeJobConfig from the
    sibling jobs, with recovery-specific topic defaults.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id: str = "canonicalize-recovery",
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


def run(config: RecoveryCanonicalizeJobConfig) -> None:  # pragma: no cover - flink runtime
    """Build and execute the recovery canonicalize job.

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

    from jobs.recovery_canonicalize.transform import (
        build_dlq_envelope,
        select_dlq_error_type,
        transform_recovery_to_canonical,
        validate_recovery_event,
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

    # --- source: raw.recovery as JSON string --------------------------------
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
    # raw.recovery JSON envelope. The recovery producer emits epoch-ms longs (W1-5).
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

    # Canonical Row field layout (order matches schemas/canonical/WellnessEvent.avsc,
    # 22 columns — identical to the wellness canonicalize job DDL).
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
            Types.STRING(), Types.STRING(),  # event_type STRING
            Types.FLOAT(), Types.INT(), Types.FLOAT(), Types.INT(), Types.FLOAT(),
            Types.INT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
            Types.INT(), Types.INT(), Types.INT(), Types.INT(), Types.INT(),
        ],
    )

    class RecoveryCanonicalizeProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Dedup (ValueState<bool> per event_id, 7d TTL) + transform + DLQ routing.

        Keyed by ``event_id`` (LOCKED). First-seen → transform → yield canonical
        Row; duplicate → silently dropped (sc-21). ValidationError / TransformError
        → DLQ side output (sc-19, sc-20). event_type is ALWAYS RECOVERY_SNAPSHOT
        (hardcoded in transform.py — ADR-R1).

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
                ValueStateDescriptor("seen-recovery-event-id", Types.BOOLEAN())
            )
            self._seen.enable_time_to_live(ttl)

        def process_element(self, value: str, ctx: Any) -> None:
            # Dedup: ValueState<bool> keyed by event_id (7d TTL)
            if bool(self._seen.value()):
                return  # duplicate within 7d re-delivery window → silently dropped (sc-21)

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
                canonical = transform_recovery_to_canonical(raw, self._schema_version)
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
            RecoveryCanonicalizeProcessFunction(canonical_field_names, schema_version),
            output_type=canonical_row_type,
        )
    )

    # DLQ side output → JSON KafkaSink (AT_LEAST_ONCE per design).
    # original_topic="raw.recovery" in the DLQ envelope disambiguates from
    # the wellness job's DLQ records (original_topic="raw.wellness") at triage time.
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
    # Uses the EXISTING WellnessEvent schema (22 columns, no new registration).
    # ADR-R2: transactional_id_prefix is DISTINCT from wellness job to prevent
    # ProducerFencedException when both jobs write to canonical.wellness_event
    # with EXACTLY_ONCE semantics (sc-23).
    # ADR-16 (wellness): event_type is declared as STRING (not enum); the
    # recovery job inherits this DDL convention from the wellness DDL.
    sink_ddl = f"""
CREATE TABLE canonical_recovery_wellness_event_sink (
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
  'sink.transactional-id-prefix' = '{TRANSACTIONAL_ID_PREFIX}'
)
"""
    tbl_env.execute_sql(sink_ddl)

    # Lift canonical Row DataStream into a Table and INSERT into the
    # avro-confluent sink via StatementSet + attach_as_datastream() so the
    # DLQ DataStream sink and the canonical Table sink run as ONE Flink job.
    canonical_table = tbl_env.from_data_stream(transformed)
    statement_set = tbl_env.create_statement_set()
    statement_set.add_insert("canonical_recovery_wellness_event_sink", canonical_table)
    statement_set.attach_as_datastream()

    env.execute("athleteos-recovery-canonicalize-job")


def main() -> int:  # pragma: no cover - entrypoint
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    group = os.environ.get("RECOVERY_CANONICALIZE_GROUP_ID", "canonicalize-recovery")
    sv_env = os.environ.get("SCHEMA_VERSION_OVERRIDE")
    config = RecoveryCanonicalizeJobConfig(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        group_id=group,
        schema_version=int(sv_env) if sv_env else None,
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
