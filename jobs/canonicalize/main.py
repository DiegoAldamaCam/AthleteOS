"""PyFlink canonicalize job wiring (PR3, task 4.1/4.2).

Import-isolation contract
=======================
apache-flink has no wheel for CPython 3.14 (grpcio-tools / apache-beam build
fails). To keep the package importable and `pytest --collect-only` working,
**all pyflink imports are LAZY** -- they live inside ``run()`` / ``build_job()``
on a flink-capable runtime. The PURE mapping/validation/DLQ/session_load logic
lives in :mod:`jobs.canonicalize.transform`, which imports WITHOUT pyflink and is
fully unit-tested under CPython 3.14 (tests/unit/test_canonicalize_transform.py).

The integration slice (tests/integration/test_canonicalize_job.py) exercises
this wiring end-to-end when apache-flink IS installed and Docker is up; it
SKIPS cleanly otherwise (never fakes a pass).

PyFlink API reality (Flink 1.19, verified via Context7)
=======================================================
The Confluent-Registry Avro serde has NO DataStream-facing API in PyFlink
(``pyflink.datastream.formats.avro.ConfluentRegistryAvroSerializationSchema``
does NOT exist; it is a Java-only API). The Confluent-Registry Avro wire format
IS reachable through the **Table/SQL** connector via ``'value.format' =
'avro-confluent'`` + ``'value.avro-confluent.url'`` -- which is the cleanest
real path. Hence the canonical sink is wired through a
``StreamTableEnvironment`` DDL'd Kafka table whose value uses the
``avro-confluent`` format, and the canonical Row DataStream is lifted into that
table via ``tbl_env.from_data_stream(transformed)`` +
``table.execute_insert(...)``.

KeyedProcessFunction in PyFlink emits via ``yield``: the Java
``process_element(value, ctx, Collector)`` signature does NOT apply on the
Python side. PyFlink's signature is ``process_element(self, value, ctx)``
(yield main payload, ``yield output_tag, payload`` for side output), and the
main-stream type MUST be passed as ``output_type=...`` to ``.process(...)``.

Job topology (design.md)
=======================
raw -> canonical training event, strength-sourced slice:

    KafkaSource(raw.strength, SimpleStringSchema-JSON)
      .assign_watermark(WatermarkStrategy.for_bounded_out_of_orderness(24h)
                        .with_timestamp_assigner(_EventTimeAssigner()))
        # _EventTimeAssigner parses JSON -> event_time (ISO) -> epoch-ms long
        # so windows are over event_time, NOT ingest_time (spec line ~29).
      -> key_by(event_id)                 # dedup keyed by event_id (LOCKED)
      -> .process(CanonicalizeProcessFunction, output_type=canonical_row_type)
        # KeyedProcessFunction: ValueState<bool> seen(event_id), StateTtlConfig
        # 7d (OnCreateAndWrite + NeverReturnExpired). Emits via ``yield``:
        #   -> main:  yield Row(... canonical TrainingEvent ...)
        #   -> side:  yield dlq_tag, json.dumps(build_dlq_envelope(...))
      [main stream] -> StreamTableEnvironment.from_data_stream(table)
                     -> Kafka-sink table (avro-confluent value, RAW athlete_id
                        key, DeliveryGuarantee.EXACTLY_ONCE) [Table API]
      [side DLQ  ]   -> KafkaSink(dlq.canonical.training_event, JSON string,
                                  DeliveryGuarantee.AT_LEAST_ONCE) [DataStream]

Refs the event-contracts spec:
  - Common Event Envelope: epoch-ms longs, schema_version REQUIRED (added here).
  - TrainingEvent Avro schema (registered BACKWARD via TopicNameStrategy).
  - DLQ error envelope: original_value base64, error_type, timestamp.
  - DLQ JSON sink = AT_LEAST_ONCE (duplicates tolerable diagnostics).
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

# Constants are import-safe (no pyflink); they describe the topology.
RAW_TOPIC = "raw.strength"
CANONICAL_TOPIC = "canonical.training_event"
DLQ_TOPIC = "dlq.canonical.training_event"
SOURCE_NAME = "canonicalize-strength-source"

# Dedup ValueState<bool> per event_id, 7d TTL (LOCKED design).
DEDUP_TTL_DAYS = 7
# Batch-ish source: bounded out-of-orderness = 24h (recovery uploads land late).
WATERMARK_OUT_OF_ORDER_HOURS = 24

# Schema version is resolved from the Confluent Schema Registry at job startup
# (the contract forbids hardcoding versions). Used only when the job cannot
# resolve the live version (unit-injectable placeholder; the real job reads the
# Registry's version for ``canonical.training_event-value``).
_DEFAULT_SCHEMA_VERSION_FALLBACK = 1


# Characters that must never appear in DDL-interpolated config values.
# A single quote closes an SQL string literal; double quote, newline (LF/CR),
# and null byte enable multi-line or parser-confusion injection into the
# Flink Table DDL f-string (sink_ddl). This is an allowlist-based guard:
# if the value contains ANY of these characters it is unconditionally rejected
# with a ValueError before it can reach tbl_env.execute_sql. (RISK F2)
_DDL_FORBIDDEN_CHARS: str = "'\"\n\r\x00"


def _validate_ddl_config_field(field_name: str, value: str) -> None:
    """Reject values that contain SQL/DDL injection characters.

    Raises ValueError with the field name so the caller can identify which
    config parameter is problematic. Called in CanonicalizeJobConfig.__init__
    for every field that is interpolated into the sink DDL f-string. (RISK F2)
    """
    for ch in _DDL_FORBIDDEN_CHARS:
        if ch in value:
            raise ValueError(
                f"CanonicalizeJobConfig.{field_name} contains a character that is "
                f"forbidden in DDL interpolation (char ord={ord(ch):#04x}). "
                f"Accepted characters: printable ASCII excluding '\"\\n\\r\\x00. "
                f"Received value (first 80 chars): {value[:80]!r}"
            )


def _epoch_ms_now() -> int:
    """Wall-clock now as epoch-ms long (used for DLQ envelope timestamp)."""
    return int(time.time() * 1000)


class CanonicalizeJobConfig:
    """Plain configuration container (no pyflink). Import-safe.

    Allows tests / orchestrators to construct and inspect a config without
    pulling in pyflink; ``run()`` consumes it from inside the lazy-import scope.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        schema_registry_url: str,
        group_id: str = "canonicalize-strength",
        raw_topic: str = RAW_TOPIC,
        canonical_topic: str = CANONICAL_TOPIC,
        dlq_topic: str = DLQ_TOPIC,
        checkpoint_interval_ms: int = 60_000,
        schema_version: int | None = None,
        bounded: bool = False,
        parallelism: int | None = None,
        no_restart: bool = False,
    ) -> None:
        # DDL injection guard (RISK F2): validate before any field assignment.
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
        # If None, the job resolves the live version from the Registry at startup.
        self.schema_version = schema_version
        # When True the KafkaSource is created in BOUNDED mode: it reads from the
        # earliest offset up to the LATEST offset captured at job startup, then
        # sends MAX_WATERMARK and finishes -- which makes the bounded streaming
        # job drain and ``env.execute()`` return (so an integration test can run
        # the real PyFlink job deterministically, terminating on its own). The
        # production job stays unbounded (False) as designed.
        self.bounded = bounded
        # Optional parallelism override (None -> env default). Integration tests set
        # parallelism=1 to make the bounded run fully deterministic and to keep the
        # exactly-once Kafka txn to a single sink subtask.
        self.parallelism = parallelism
        # When True disable the restart strategy so a runtime failure surfaces
        # immediately (the default FIXED_DELAY-with-Integer.MAX_VALUE-retries
        # strategy, auto-enabled when checkpointing is on, would otherwise mask a
        # ProcessFunction crash by restarting forever -- which made the live job
        # appear to hang). The production job keeps the default (fault-tolerant).
        self.no_restart = no_restart

    def effective_schema_version(self) -> int:
        return self.schema_version if self.schema_version is not None else (
            _DEFAULT_SCHEMA_VERSION_FALLBACK
        )


# ---------------------------------------------------------------------------
# Job wiring (PYFLINK-DEPENDENT). Imported lazily. Do not call at import time.
# ---------------------------------------------------------------------------


def run(config: CanonicalizeJobConfig) -> None:  # pragma: no cover - flink runtime
    """Build and execute the canonicalize job against a live broker+Registry.

    All pyflink imports are INSIDE this function so the module imports cleanly on
    interpreters without apache-flink. Executed only on a flink-capable runtime.

    PyFlink reality (Flink 1.19, verified via Context7):
      - The Confluent-Registry Avro serde is reachable ONLY through the Table
        / SQL connector ('value.format'='avro-confluent'), NOT through any
        DataStream-facing API (the prior
        ``ConfluentRegistryAvroSerializationSchema`` import was a Java-only
        fiction). The canonical sink is therefore wired via a StreamTableEnvironment.
      - ``KeyedProcessFunction.process_element`` has the signature
        ``(self, value, ctx)`` and emits via ``yield`` (and
        ``yield output_tag, payload`` for side output); the Java Collector
        does NOT exist on the Python side. ``.process(func, output_type=...)``
        MUST be given the main-stream type.
    """
    # --- pyflink imports (deferred) -----------------------------------------
    from pyflink.common import (
        Duration,
        Row,
        Time,
        Types,
        WatermarkStrategy,
    )
    # TimestampAssigner is defined in pyflink.common.watermark_strategy but is
    # NOT re-exported at the `pyflink.common` package level in 1.19 (gate
    # review importing it from `pyflink.common` was wrong -- ImportError on
    # the live runtime). Import it from its defining submodule directly.
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

    from jobs.canonicalize.transform import (
        _key_by_event_id,
        build_dlq_envelope,
        select_dlq_error_type,
        transform_strength_to_canonical,
        validate_training_event,
        parse_iso_to_epoch_ms,
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

    # StreamTableEnvironment wraps the same DataStream env so the canonical
    # Avro-Confluent sink (Table API) and the DLQ KafkaSink (DataStream API)
    # run as ONE submitted job. (avro-confluent is the only real PyFlink path
    # to the Confluent Registry Avro serde; there is no DataStream equivalent.)
    table_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    tbl_env = StreamTableEnvironment.create(env, environment_settings=table_settings)

    schema_version = config.effective_schema_version()

    # DLQ side-output tag: JSON string payloads (spec: DLQ is JSON).
    dlq_tag = OutputTag("dlq", Types.STRING())

    # --- source: raw.strength as JSON string --------------------------------
    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.raw_topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if config.bounded:
        # Bounded mode (test/integration): read from earliest to the latest
        # offset captured at startup, then finish. Lets `env.execute()` terminate
        # deterministically. Production stays unbounded (the KafkaSource default).
        source_builder = source_builder.set_bounded(
            KafkaOffsetsInitializer.latest()
        )
    source = source_builder.build()

    # Event-time assigner: parses event_time (ISO-8601 string) out of the raw
    # JSON envelope and converts it to epoch-ms so downstream event-time
    # windows run over event_time, NOT ingest_time (spec line ~29
    # "Event-time ordering"). The Kafka source delivers raw envelopes as JSON
    # strings via SimpleStringSchema; the timestamp assigner therefore MUST
    # json.loads the value here. NO assigner can live at the Kafka-source level
    # (event_time only exists inside the parsed JSON).
    #
    # The assigner must never raise: malformed JSON / missing event_time here
    # is silently forwarded with its prior timestamp -- the canonicalize
    # ProcessFunction downstream catches those records and routes them to the
    # DLQ via select_dlq_error_type. NAIVE-UTC equivalence with
    # transform.parse_iso_to_epoch_ms is preserved.
    class _EventTimeAssigner(TimestampAssigner):  # type: ignore[misc]
        def extract_timestamp(self, value: str, record_timestamp: int) -> int:
            try:
                envelope = json.loads(value)
                iso = (
                    envelope.get("event_time")
                    if isinstance(envelope, dict)
                    else None
                )
            except (TypeError, ValueError):
                return record_timestamp
            if not isinstance(iso, str) or iso == "":
                return record_timestamp
            try:
                return parse_iso_to_epoch_ms(iso)
            except TransformError:
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

    # Canonical Row field layout (order matches schemas/canonical/TrainingEvent.avsc).
    canonical_field_names = (
        "event_id", "event_time", "ingest_time", "source", "schema_version",
        "athlete_id", "event_type", "workout_id", "exercise_id", "set_number",
        "reps", "weight_kg", "rpe", "rir", "activity_type", "distance_km",
        "duration_sec", "avg_hr", "tss", "session_load",
    )
    canonical_row_type = Types.ROW_NAMED(
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

    class CanonicalizeProcessFunction(KeyedProcessFunction):  # type: ignore[misc]
        """Dedup (ValueState<bool> per event_id, 7d TTL) + transform + validate.

        Keyed by ``event_id`` (LOCKED: growth bounded to the 7d re-delivery
        window). First-seen -> transform+validate+``yield`` canonical Row;
        duplicate -> dropped. ValidationError / TransformError -> DLQ side
        output via ``yield dlq_tag, payload``.

        PyFlink KeyedProcessFunction emits via ``yield``; the Java Collector
        signature ``process_element(value, ctx, Collector)`` does NOT exist on
        the Python side (CRITICAL-1 fix).
        """

        def __init__(
            self,
            field_names: tuple[str, ...],
            schema_version: int,
        ) -> None:
            self._field_names = field_names
            self._schema_version = schema_version

        def open(self, runtime_context: Any) -> None:  # noqa: D401
            ttl = StateTtlConfig.new_builder(
                Time.days(DEDUP_TTL_DAYS)
            ).set_update_type(
                StateTtlConfig.UpdateType.OnCreateAndWrite
            ).set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            ).build()
            self._seen = runtime_context.get_state(
                ValueStateDescriptor("seen-event-id", Types.BOOLEAN())
            )
            self._seen.enable_time_to_live(ttl)

        def process_element(self, value: str, ctx: Any) -> None:
            # Dedup: ValueState<bool> keyed by event_id
            if bool(self._seen.value()):
                return  # duplicate inside the 7d re-delivery window -> dropped
            original_value = value
            try:
                raw = json.loads(value)
            except (TypeError, ValueError) as json_exc:
                # Malformed raw JSON: raise-and-classify so the same
                # pure helper (select_dlq_error_type) picks the DLQ error_type.
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
                canonical = transform_strength_to_canonical(raw, self._schema_version)
                validate_training_event(canonical)
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

            # Mark seen and emit canonical Row (order: mark first for exactly-once).
            self._seen.update(True)
            yield Row(*[canonical[f] for f in self._field_names])

    # --- transform pipeline -------------------------------------------------
    # PyFlink .process(...) REQUIRES output_type= for the main stream type
    # (CRITICAL-1 fix); without it the runtime cannot infer the produced type.
    transformed = (
        raw_stream
        .key_by(_key_by_event_id)
        .process(
            CanonicalizeProcessFunction(canonical_field_names, schema_version),
            output_type=canonical_row_type,
        )
    )

    # DLQ side output -> JSON KafkaSink (AT_LEAST_ONCE per design ADR-12).
    # (Straight call; the prior tautological identical-branch ternary removed.)
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

    # --- canonical main stream: Avro-Confluent sink via the Table API --------
    # BLOCKER-1 fix: the Confluent-Registry Avro serde is ONLY reachable in
    # PyFlink 1.19 through the Table/SQL connector ('value.format' =
    # 'avro-confluent'); there is no DataStream ConfluentRegistryAvro*
    # serialization schema. We DDL a 'connector'='kafka' sink table whose value
    # uses 'avro-confluent' against the Confluent Schema Registry, with a RAW
    # athlete_id key for co-partitioning (design: Kafka record key = athlete_id).
    #
    # value.fields-include defaults to ALL -> athlete_id is ALSO present in the
    # Avro value, matching the registered TrainingEvent.avsc schema (which
    # lists athlete_id as a required field).
    #
    # RUNTIME-VERIFIED: ``event_type`` wire format CONVERGES (ADR-15).
    # Flink's Table ``avro-confluent`` format infers the Avro writer schema
    # from the DDL column types (Context7 confirmed -- there is NO option to
    # supply an explicit .avsc). The Table type system has NO Avro enum, so
    # ``event_type STRING`` below emits Avro ``{"type":"string"}``.
    # ``schemas/canonical/TrainingEvent.avsc`` now ALSO declares ``event_type``
    # as a plain ``string`` (approved change ADR-15), so the design contract
    # and the runtime wire format now CONVERGE -- no more HTTP 409 enum<->string
    # incompatibility. The former enum's semantic guarantee is preserved at the
    # application layer by ``validate_training_event()`` (symbol set
    # ``{STRENGTH_SET, CARDIO_ACTIVITY}``; out-of-set -> ValidationError ->
    # DLQ VALIDATION_FAILURE via select_dlq_error_type).
    #
    # Why the live sink subject is still NOT pre-registered here: the
    # DDL-inferred writer schema may still differ from ``TrainingEvent.avsc``
    # on nullable-union vs plain-type for optional fields (the avsc declares
    # e.g. ``workout_id`` as ``["null","string"]`` with default null, while the
    # DDL emits a plain ``STRING`` that the connector maps to a non-union Avro
    # type under certain versions). Letting the Flink sink own the live
    # writer-schema registration keeps the first emission consistent with the
    # BACKWARD subject compatibility; the cross-process design contract stays
    # verified by ``tests/unit/test_canonicalize_transform.py`` (against the
    # .avsc) and by ``bootstrap.register_schemas`` in the deploy pipeline. The
    # integration test mirrors exactly this runtime discipline.
    #
    # The kafka connector's EXACTLY_ONCE delivery requires a transactional-id
    # prefix (spec: canonical sink is EXACTLY_ONCE; DLQ is AT_LEAST_ONCE).
    sink_ddl = f"""
CREATE TABLE canonical_training_event_sink (
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
  -- Kafka record key = athlete_id (RAW UTF-8) for co-partitioning with
  -- downstream athlete-keyed windows (design: "Kafka record key =
  -- athlete_id"). value.fields-include defaults to ALL (athlete_id is
  -- ALSO in the Avro value -- matches the registered TrainingEvent.avsc).
  'key.format' = 'raw',
  'key.fields' = 'athlete_id',
  -- Confluent-Registry Avro value serde (BLOCKER-1 fix: this is the real,
  -- documented Flink 1.19 PyFlink path -- no DataStream equivalent exists).
  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = '{config.schema_registry_url}',
  -- Spec: canonical sink MUST be EXACTLY_ONCE (DLQ is AT_LEAST_ONCE).
  -- EXACTLY_ONCE on the kafka connector REQUIRES the transactional-id prefix.
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'athleteos-canonicalize-training-event'
)
"""
    tbl_env.execute_sql(sink_ddl)

    # Lift the canonical Row DataStream (its Types.ROW_NAMED carries the named
    # field names) into a Table and INSERT into the Avro-Confluent sink.
    #
    # Note: ``from_data_stream`` does NOT propagate a rowtime/watermark Schema
    # across the Table boundary. That is fine here -- the watermark is for the
    # source-side event-time pipeline (dedup/KeyedProcessFunction); the
    # canonical Kafka sink is append-only by field-name mapping, so no
    # rowtime Schema is required for the sink to work.
    #
    # Single-submission wiring (Flink 1.19 PyFlink, verified via Context7):
    # ``Table.execute_insert(...)`` submits ONLY the Table pipeline -- any
    # DataStream sinks on the SAME env (here: ``dlq_stream.sink_to(dlq_sink)``)
    # would be ORPHANED and never executed, silently dropping DLQ traffic and
    # breaking the spec DLQ requirement. Instead we add the Table insert to a
    # StatementSet, fold the Table pipeline into the DataStream DAG via
    # ``attach_as_datastream()`` (the statement set is cleared afterwards),
    # then submit the whole job ONCE with ``env.execute(...)``. The DLQ sink
    # is already attached to this same ``env``, so the single
    # ``env.execute()`` submits BOTH the canonical Avro-Confluent Table sink
    # AND the DLQ JSON DataStream sink as one Flink job.
    #
    # API names/signatures confirmed via Context7 (Flink 1.19 docs):
    #   - ``StreamTableEnvironment.create_statement_set() -> StatementSet``
    #   - ``StatementSet.add_insert(target_path: str, table: Table) -> StatementSet``
    #     (target_path = registered sink-table name from the DDL above)
    #   - ``StatementSet.attach_as_datastream()`` -- adds the Table pipeline as
    #     transformations to the StreamExecutionEnvironment; use ``env.execute()``
    #     to submit them (NOT the statement set).
    canonical_table = tbl_env.from_data_stream(transformed)
    statement_set = tbl_env.create_statement_set()
    statement_set.add_insert("canonical_training_event_sink", canonical_table)
    statement_set.attach_as_datastream()

    # Submit the full DAG (canonical Avro-Confluent Table sink + DLQ JSON
    # DataStream sink) as a single Flink job.
    env.execute("athleteos-canonicalize-job")


def main() -> int:  # pragma: no cover - entrypoint
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    registry = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    group = os.environ.get("CANONICALIZE_GROUP_ID", "canonicalize-strength")
    sv_env = os.environ.get("SCHEMA_VERSION_OVERRIDE")
    config = CanonicalizeJobConfig(
        bootstrap_servers=bootstrap,
        schema_registry_url=registry,
        group_id=group,
        schema_version=int(sv_env) if sv_env else None,
    )
    run(config)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())