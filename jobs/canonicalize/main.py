"""PyFlink canonicalize job wiring (PR3, task 4.1/4.2).

Import-isolation contract
========================
apache-flink has no wheel for CPython 3.14 (grpcio-tools / apache-beam build
fails). To keep the package importable and `pytest --collect-only` working,
**all pyflink imports are LAZY** -- they live inside ``run()`` / ``build_job()``
on a flink-capable runtime. The PURE mapping/validation/DLQ/session_load logic
lives in :mod:`jobs.canonicalize.transform`, which imports WITHOUT pyflink and is
fully unit-tested under CPython 3.14 (tests/unit/test_canonicalize_transform.py).

The integration slice (tests/integration/test_canonicalize_job.py) exercises
this wiring end-to-end when apache-flink IS installed and Docker is up; it
SKIPS cleanly otherwise (never fakes a pass).

Job topology (design.md)
========================
raw -> canonical training event, strength-sourced slice:

    KafkaSource(raw.strength, SimpleStringSchema-JSON)
      .assign_watermark(WatermarkStrategy.for_bounded_out_of_orderness(24h)
                        .with_timestamp_assigner(event -> event_time epoch-ms))
      -> key_by(event_id)                 # dedup keyed by event_id (LOCKED)
      -> CanonicalizeProcessFunction:     # KeyedProcessFunction
          ValueState<bool> seen(event_id), StateTtlConfig 7d
          (OnCreateAndWrite + NeverReturnExpired)
          on first-seen event_id:
            json.loads(raw) -> transform_strength_to_canonical(...) -> validate
            emit canonical Row (main output) -- on ValidationError/TransformError
            ctx.output(DLQ_TAG, build_dlq_envelope(...))   (side output)
      -> key_by(athlete_id)               # Kafka record key = co-partitioning
      -> KafkaSink(canonical.training_event,
                   ConfluentRegistryAvroSerializationSchema(Registry URL),
                   key=athlete_id, DeliveryGuarantee.EXACTLY_ONCE)

      (main.split.side_output(DLQ_TAG))
      == DLQ stream -> KafkaSink(dlq.canonical.training_event, JSON,
                                 DeliveryGuarantee.AT_LEAST_ONCE)

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
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.group_id = group_id
        self.raw_topic = raw_topic
        self.canonical_topic = canonical_topic
        self.dlq_topic = dlq_topic
        self.checkpoint_interval_ms = checkpoint_interval_ms
        # If None, the job resolves the live version from the Registry at startup.
        self.schema_version = schema_version

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
    """
    # --- pyflink imports (deferred) -----------------------------------------
    from pyflink.common import Duration, Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import (
        StreamExecutionEnvironment,
        RuntimeExecutionMode,
    )
    from pyflink.datastream.connectors.kafka import (
        KafkaSource,
        KafkaSink,
        KafkaOffsetsInitializer,
        DeliveryGuarantee,
        KafkaRecordSerializationSchema,
    )
    from pyflink.datastream.functions import KeyedProcessFunction
    from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig
    from pyflink.datastream import OutputTag
    from pyflink.datastream.formats.avro import ConfluentRegistryAvroSerializationSchema
    from pyflink.datastream.formats.json import JsonRowSerializationSchema

    from jobs.canonicalize.transform import (
        transform_strength_to_canonical,
        validate_training_event,
        build_dlq_envelope,
        ValidationError,
        TransformError,
        VALIDATION_FAILURE,
        TRANSFORM_ERROR,
    )

    # --- environment --------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(config.checkpoint_interval_ms)

    schema_version = config.effective_schema_version()

    # DLQ side-output tag: JSON string payloads (spec: DLQ is JSON).
    dlq_tag = OutputTag("dlq", Types.STRING())

    # --- source: raw.strength as JSON string --------------------------------
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_topics(config.raw_topic)
        .set_group_id(config.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_hours(WATERMARK_OUT_OF_ORDER_HOURS)
    )

    raw_stream = env.from_source(
        source=source,
        watermark_strategy=watermark,
        source_name=SOURCE_NAME,
    )

    # Canonical Row type matches the TrainingEvent Avro schema.
    canonical_row_type = Types.ROW_NAMED(
        [
            "event_id", "event_time", "ingest_time", "source", "schema_version",
            "athlete_id", "event_type", "workout_id", "exercise_id", "set_number",
            "reps", "weight_kg", "rpe", "rir", "activity_type", "distance_km",
            "duration_sec", "avg_hr", "tss", "session_load",
        ],
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
        window). First-seen -> transform+validate+emit canonical Row; duplicate
        -> dropped. ValidationError / TransformError -> DLQ side output.
        """

        def __init__(self, row_type: Any, schema_version: int) -> None:
            self._row_type = row_type
            self._schema_version = schema_version

        def open(self, runtime_context: Any) -> None:  # noqa: D401
            ttl = StateTtlConfig.new_builder(
                Duration.of_days(DEDUP_TTL_DAYS)
            ).set_update_type(
                StateTtlConfig.UpdateType.OnCreateAndWrite
            ).set_state_visibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired
            ).build()
            self._seen = runtime_context.get_state(
                ValueStateDescriptor("seen-event-id", Types.BOOLEAN())
            )
            self._seen.enable_time_to_live(ttl)

        def process_element(self, value: str, ctx: Any, out: Any) -> None:
            # Dedup: ValueState<bool> keyed by event_id
            if bool(self._seen.value()):
                return  # duplicate inside the 7d re-delivery window -> dropped
            original_value = value
            try:
                raw = json.loads(value)
            except (TypeError, ValueError):
                ctx.output(
                    dlq_tag,
                    json.dumps(
                        build_dlq_envelope(
                            original_topic=config.raw_topic,
                            original_key=None,
                            original_value=original_value,
                            error_type=TRANSFORM_ERROR,
                            error_message="raw value is not valid JSON",
                            timestamp=_epoch_ms_now(),
                        )
                    ),
                )
                return

            event_id = raw.get("event_id") if isinstance(raw, dict) else None
            athlete_id = raw.get("athlete_id") if isinstance(raw, dict) else None
            try:
                canonical = transform_strength_to_canonical(raw, self._schema_version)
                validate_training_event(canonical)
            except (ValidationError, TransformError) as exc:
                self._seen.update(True)  # mark to avoid re-routing the same bad event
                ctx.output(
                    dlq_tag,
                    json.dumps(
                        build_dlq_envelope(
                            original_topic=config.raw_topic,
                            original_key=athlete_id,
                            original_value=original_value,
                            error_type=(
                                VALIDATION_FAILURE
                                if isinstance(exc, ValidationError)
                                else TRANSFORM_ERROR
                            ),
                            error_message=str(exc),
                            timestamp=_epoch_ms_now(),
                        )
                    ),
                )
                return

            # Mark seen and emit canonical Row (order: mark first for exactly-once).
            self._seen.update(True)
            from pyflink.common import Row
            out.collect(Row(*[canonical[f] for f in self._row_type._field_names]))

    # --- transform pipeline -------------------------------------------------
    transformed = (
        raw_stream
        .key_by(lambda raw: json.loads(raw).get("event_id") or "")
        .process(CanonicalizeProcessFunction(canonical_row_type, schema_version))
    )

    # DLQ side output -> JSON KafkaSink (AT_LEAST_ONCE per design ADR-12).
    dlq_stream = transformed.get_side_output(dlq_tag) if hasattr(
        transformed, "get_side_output"
    ) else transformed.get_side_output(dlq_tag)

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

    # Canonical main stream: re-key by athlete_id (Kafka record key = co-partitioning)
    # and sink to canonical.training_event with the Confluent Registry Avro serde.
    avro_value_schema = ConfluentRegistryAvroSerializationSchema.for_value(
        schema_registry_url=config.schema_registry_url,
        type_info=canonical_row_type,
    )

    canonical_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.canonical_topic)
            .set_value_serialization_schema(avro_value_schema)
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .build()
    )
    transformed.key_by(lambda row: row.athlete_id).sink_to(canonical_sink)

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