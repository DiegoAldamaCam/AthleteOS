# Design: AthleteOS Foundation — Implementation-Ready Technical Design

This design makes the four source-of-truth specs (event-contracts, architecture, serving-store, analytical-store) buildable. It fixes the PyFlink job topology, Schema Registry operations, DLQ wiring, storage DDL/config, and the local runtime so the apply phase can scaffold the repo and the first vertical slice (strength end-to-end) without re-deciding architecture. All LOCKED decisions are honored, not re-opened.

## Technical Approach

Two PyFlink jobs per the Event-Staged layers: a **canonicalize** job (raw→canonical, Layer 2) and a **metrics** job (canonical→PG+Iceberg, Layer 3). Both run on a single jobmanager/taskmanager pair locally, exactly-once checkpointing every 60s (main PG+Iceberg path; DLQ sink is at-least-once) aligned with the serving-store micro-batch sink. RocksDB holds keyed state. Schema Registry governs canonical Avro with BACKWARD compatibility and `TopicNameStrategy` subjects. Invalid events route to DLQ via PyFlink side outputs. PostgreSQL serves latest metrics via UPSERT; Iceberg (Hadoop catalog, Parquet) stores history; DuckDB reads Iceberg read-only.

## Stream Processing Design (PyFlink)

### Job topology

| Job | Source | Key | State / Windows | Sinks |
|-----|--------|-----|-----------------|-------|
| **canonicalize** | `raw.*` (KafkaSource, JSON) | `athlete_id` | dedup `ValueState<bool>` per `event_id` (7d TTL) | `canonical.*` (Avro, Registry) + `dlq.canonical.*` side output |
| **metrics** | `canonical.training_event` (+ `canonical.wellness_event`, `canonical.planning_block` post-MVP) | `athlete_id` | event-time windows: daily pre-agg → sliding 7/28/42d load; deload counter (`KeyedProcessFunction`) over daily stream; dedup | PG `athlete_metrics` (UPSERT) + Iceberg `training_event`/`wellness_event`/`planning_block` (append) + DLQ + late side outputs |

MVP vertical slice activates only `raw.strength → canonical.training_event → metrics → PG + Iceberg`. The wellness/planning sources are wired structurally but their canonicalize branches and SHOULD-metric joins are post-MVP.

### Event-time + watermark

- `WatermarkStrategy.for_bounded_out_of_orderness(Duration)` on `event_time` (epoch-ms), assigned at the KafkaSource.
- Batch-ish sources (strength, recovery, nutrition, planning): bounded out-of-orderness = **24h** (recovery uploads land days late).
- Streaming/NRT sources (cardio, wellness): **1h**.
- Per-source watermark is set at the raw consumer; the metrics job inherits canonical `event_time`. Since one topic carries mixed-latency sources, the metrics job uses the **conservative 24h** bound.
- Rolling load is computed with **event-time windows** (see below), so late data is handled by the window's native `allowed_lateness` + `side_output_late_data`, NOT by manual operator config.

### Rolling-load windowing (event-time windows)

Rolling 7/28/42d load is a **sliding event-time window** aggregation — the natural and spec-aligned shape (the architecture watermark scenario and the event-contracts event-time-ordering scenario both describe event-time windows + allowed lateness). Topology on the keyed (`athlete_id`) canonical stream:

1. **Daily pre-aggregation**: `TumblingEventTimeWindows.of(Time.days(1))` summing `session_load` per `(athlete_id, day)`. Emits one daily-load record per athlete per day.
2. **Rolling windows**: three `SlidingEventTimeWindows` (size 7d/28d/42d, slide 1d) over the daily-load stream produce acute (7d), chronic (28d) and 42d load; ACR = 7d/28d.
3. **allowed lateness**: each window declares `.allowed_lateness(Time.hours(24))` matching the 24h out-of-orderness bound, so a late strength upload still updates already-fired windows within 24h of window end.
4. **Late side output**: `.side_output_late_data(OutputTag("late.metrics", ...))`; data later than window-end + 24h is captured (not dropped) and routed to a DLQ-style late log. Retrieved via `result.get_side_output(tag)`.
5. **Emit-on-update**: the daily pre-agg window uses a `ContinuousEventTimeTrigger` (early firing) so the serving store sees within-day updates instead of waiting for day close; final correction fires at window close. This is what keeps PG fresh under a 24h watermark (see ADR-13).

Precise PyFlink chain (confirmed against Flink 1.19 docs): `stream.key_by(athlete_id).window(<assigner>).trigger(ContinuousEventTimeTrigger...).allowed_lateness(Time.hours(24)).side_output_late_data(tag).aggregate(load_agg)`; `result.get_side_output(tag)` for late events. `allowed_lateness`/`side_output_late_data` are `WindowedStream` methods — valid here because we call `.window(...)`.

### Keyed state (RocksDB)

| Operator | State | Type | TTL |
|----------|-------|------|-----|
| canonicalize dedup | seen `event_id` | `ValueState<bool>` keyed by event_id | **7d** (`OnCreateAndWrite`, `NeverReturnExpired`) |
| metrics dedup | seen `event_id` | same | 7d |
| rolling load | window contents (daily pre-agg + 7/28/42d sliding) | Flink-managed window state (RocksDB) | bounded by window size + 24h allowed lateness |
| deload counter | consecutive ACR-breach day count + sign | `ValueState<(int,int)>` in a `KeyedProcessFunction` over the daily-load stream | none (small, per-athlete) |

The deload rule runs as a `KeyedProcessFunction` over the **already-windowed** daily-load/ACR stream — it consumes ordered, window-finalized daily values, so it needs no late-data handling of its own (lateness is absorbed upstream by the windows). State backend: `EmbeddedRocksDBStateBackend` with incremental checkpoints enabled. Dedup keyed by `event_id` (not `athlete_id`) so TTL bounds growth to the 7d raw-retention re-delivery window (LOCKED).

### Checkpointing

- `env.enable_checkpointing(60_000)` — 60s, matches serving-store sink micro-batch.
- Mode: **EXACTLY_ONCE** on the main path. PG sink is idempotent UPSERT and Iceberg sink commits per checkpoint, so exactly-once end-to-end holds for the **PG + Iceberg** path (ADR-12).
- **DLQ sink is at-least-once** (`DeliveryGuarantee.AT_LEAST_ONCE` on the DLQ `KafkaSink`). DLQ records are diagnostic; duplicates are tolerable and avoid the transactional-coordinator overhead of EXACTLY_ONCE on a non-critical path. The exactly-once guarantee is NOT claimed for the DLQ path.
- `EXTERNALIZED, RETAIN_ON_CANCELLATION`; min-pause 30s; tolerable failures 3.
- Dedup `StateTtlConfig` = 7d (`new_builder(Duration.ofDays(7))`, `OnCreateAndWrite`, `NeverReturnExpired`).

### Idempotency + replay

- `event_id` dedup with 7d TTL: re-delivered events inside the 7d window are dropped. For the MVP sources (`raw.strength`, `raw.cardio`, `raw.nutrition`, `raw.wellness`) retention = 7d = dedup TTL, so a re-delivery cannot outlive its dedup key — idempotency is exact.
- **Known gap (post-MVP)**: `raw.recovery` and `raw.planning` retain 14d but dedup TTL is 7d (both LOCKED). Between day 7 and day 14 a re-delivered event's dedup key has expired, so it could be reprocessed. Accepted post-MVP limitation, not a contradiction: those sources are not in the MVP slice, their downstream sinks are idempotent (PG UPSERT collapses duplicate `(athlete_id, metric_date)`), and re-delivery in that band is rare for batch recovery/planning uploads. If exact dedup is later required for them, raise their dedup TTL to 14d to match retention (no change to the LOCKED 7d default for the other topics).
- Replay from raw → re-canonicalize → same `event_id` → deduped at metrics. Replay from canonical → metrics dedup catches duplicates.
- PG: `INSERT ... ON CONFLICT (athlete_id, metric_date) DO UPDATE` — reprocessing a day overwrites, never duplicates.
- Iceberg: checkpoint-boundary append; replay within the same checkpoint is atomic (uncommitted files discarded on restore).

## Schema Registry Operational Design

| Concern | Decision |
|---------|----------|
| Subject naming | **TopicNameStrategy** (`canonical.training_event-value`). One schema per topic; matches WIDE-RECORD (one record type per topic). (ADR-10.) |
| Compatibility | **BACKWARD** per subject, set at bootstrap via Registry config API. |
| Schema fetch | Flink Kafka consumer/producer use `ConfluentRegistryAvroDeserializationSchema`/`...SerializationSchema` pointed at the Registry URL; schemas fetched + cached by ID at job startup and on cache miss. |
| Python producers (raw→nothing; canonical writes via Flink) | Ingestion producers write JSON to raw (no Registry). Avro (de)serialization to canonical is done inside Flink via the Confluent Registry serde; standalone Python tooling/tests use `confluent-kafka` + `fastavro` against the same Registry. |
| Version negotiation | Producers register schema → Registry returns ID (auto-increment, never hardcoded). Consumers resolve writer schema by embedded ID, read into reader schema (BACKWARD-safe). |

`.avsc` files live in `schemas/canonical/` and are registered at bootstrap by a `register_schemas.py` step.

## DLQ Handling Design

- **Where**: validation/transform failures in canonicalize (transform/validation errors) and metrics (deserialization, NaN guards). Each `ProcessFunction` emits failures to an `OutputTag`.
- **Pattern**: `OutputTag("dlq", Types.STRING())`; `main.get_side_output(tag)` → JSON KafkaSink to `dlq.canonical.<entity>`.
- **DLQ producer**: plain JSON `KafkaSink` (no Registry — original bytes may be unparseable), `DeliveryGuarantee.AT_LEAST_ONCE` (duplicates tolerable for diagnostics).
- **Late side output** (rolling-load windows): events past window-end + 24h allowed lateness are captured via `side_output_late_data` and written to the same DLQ-style late log rather than dropped, satisfying the architecture watermark scenario's "dropped to side output" (captured, not silently lost).
- **Error envelope** (per spec): `{original_topic, original_key, original_value(base64), error_type, error_message, error_stack, timestamp}`. `error_type` ∈ {VALIDATION_FAILURE, SCHEMA_INCOMPATIBILITY, DESERIALIZATION_ERROR, TRANSFORM_ERROR}.

## Storage Design

### PostgreSQL

DDL carried verbatim from serving-store spec (`athlete_metrics`, PK `(athlete_id, metric_date)`). Indexes: PK B-tree + `idx_athlete_metrics_athlete (athlete_id)`. Sink: JDBC UPSERT batched per checkpoint, HikariCP pool, exponential-backoff retry → DLQ on exhaustion.

### Iceberg

Three tables (`training_event`, `wellness_event`, `planning_block`) per analytical-store spec. Partitioning: `(athlete_id, days(event_time))` for training/wellness; `athlete_id` for planning. Parquet. **Hadoop catalog** at `./warehouse` (dev) / `s3://athleteos-lake/warehouse` (prod-shape). Flink Iceberg sink commits per checkpoint. **Compaction**: scheduled `iceberg-python rewrite_data_files` when a partition has >10 files <128MB; orphan-file cleanup past retention.

### DuckDB

Read-only via `iceberg_scan('./warehouse/<table>')`; same warehouse path; used in local dev + CI parity checks against PG.

## Local Runtime Design (Docker Compose)

| Service | Profile |
|---------|---------|
| kafka, schema-registry | `core` |
| flink-jobmanager, flink-taskmanager | `core` |
| postgres | `core` |
| ingestion (file watcher/producers) | `ingest` |
| fastapi | `serve` |
| streamlit | `serve` |
| schema-bootstrap (one-shot register + topic create) | `bootstrap` |

Profiles mitigate service-count friction: `core` for processing, `serve` for API/UI, `bootstrap` runs once. **Topics created at bootstrap** (all 8 partitions, `athlete_id` key): `raw.{strength,cardio,recovery,nutrition,wellness,planning}`, `canonical.{training_event,wellness_event,planning_block}`, `dlq.canonical.{training_event,wellness_event,planning_block}`. Canonical topics: compacted + time window (training/wellness 30d, planning 90d). Raw: time-bound only (7–14d).

## Architecture Decisions (new design-level ADRs)

| ADR | Decision | Alternatives | Rationale |
|-----|----------|--------------|-----------|
| ADR-10 | Subject naming = **TopicNameStrategy** | RecordNameStrategy, TopicRecordName | One record type per canonical topic (WIDE-RECORD); simplest subject mapping; BACKWARD per topic |
| ADR-11 | Rolling 7/28/42d load = **event-time sliding windows** (daily tumbling pre-agg → sliding windows) with `allowed_lateness(24h)` + `side_output_late_data`; 24h batch / 1h streaming out-of-orderness | (A) chosen: event-time windows; (B) manual `KeyedProcessFunction` + `MapState` with hand-rolled timers/late routing | Windowing is the spec-described shape (event-contracts event-time-ordering + architecture watermark scenarios) and the ONLY place `allowed_lateness`/`side_output_late_data` legitimately exist in PyFlink (they are `WindowedStream` methods). Sliding windows express rolling daily load directly; lateness/late-output are native, not hand-rolled. Rejected (B): manual state would force hand-rolled timer + watermark-comparison logic and an `OutputTag` route from inside the process function to emulate what windows give for free — more code, more bugs, no benefit for fixed-size rolling windows. |
| ADR-12 | **Exactly-once** checkpointing for the **PG + Iceberg** path; **at-least-once** for the DLQ sink | At-least-once everywhere; exactly-once everywhere | PG UPSERT + Iceberg checkpoint-commit are idempotent, so exactly-once on the main path adds no extra cost and yields the strongest guarantee. DLQ records are diagnostic — duplicates are harmless, so at-least-once avoids needless transactional overhead. Exactly-once is scoped to the main path, not DLQ. |
| ADR-13 | Checkpoint interval = **60s** (governs Iceberg commit cadence / file size); serving freshness = **emit-on-update** via `ContinuousEventTimeTrigger` early firing | Shorter interval (more small Iceberg files), longer (larger files); emit-on-window-close only (stale serving under 24h watermark) | Two distinct mechanisms, not one knob: (1) the 60s checkpoint interval governs Iceberg commit cadence and file size; (2) serving freshness is governed by the window trigger — `ContinuousEventTimeTrigger` early-fires intra-window so PG updates without waiting for the 24h-held window to close. Window finalization (correctness) and serving freshness (latency) are decoupled; the 60s interval does NOT control serving freshness, the trigger does. |
| ADR-14 | Two jobs (canonicalize + metrics), not one fat job | Single job, three jobs | Layer isolation (raw→canonical vs metric compute) + independent restart/scaling; avoids over-splitting at MVP |
| ADR-15 | Canonical `event_type` is **Avro `string`** with **application-layer symbol-set validation**, not an Avro enum | (A) keep Avro enum + hand-roll a DataStream Avro serializer (`ConfluentRegistryAvroSerializationSchema` for SpecificRecord in Java) to emit the enum; (B) emit enum on the wire via a custom DataStream Avro serde | The Flink 1.19 `avro-confluent` Table sink derives the Avro writer schema from the Table column types (Context7-confirmed: there is no option to supply an explicit `.avsc`) and the Table type system has **no Avro enum type**, so it emits `event_type` as `{"type":"string"}`. Avro forbids enum↔string promotion under BACKWARD, so the sink's inferred schema is incompatible with an enum-typed `.avsc` and the Schema Registry returns HTTP 409 on the first emission. Rather than hand-roll a Java-only DataStream Avro serializer to preserve the enum (option A — large added complexity for a single symbol field, and `ConfluentRegistryAvroSerializationSchema` has no PyFlink binding), we **relax the contract to `string`** and **enforce the symbol set `{STRENGTH_SET, CARDIO_ACTIVITY}` in `validate_training_event()`** (out-of-set → `ValidationError` → DLQ `VALIDATION_FAILURE`). This is intelligent simplicity: the runtime and the design contract converge, and the semantic guarantee is preserved. **Trade-off:** consumers no longer get enum-level schema enforcement from the Registry; they get application-level validation + DLQ routing instead. The relaxation is recorded in the `.avsc`, the event-contracts spec, and `validate_training_event()`. |
| ADR-16 | `chronic_load` uses a **dynamic denominator /n** (days present in window), NOT the fixed window size (/28 or /42) | (A) dynamic /n (chosen); (B) fixed /28 or /42 | **Sports-science rationale**: dividing by the fixed window size when fewer training days exist produces an artificially low chronic baseline for new athletes (e.g., 3 days @100 → chronic=300/28≈10.7 → ACR=700/10.7≈65 → false DELOAD_HIGH on day 1). The dynamic /n denominator reflects the athlete's actual average load over the days present, making ACR meaningful from the first recorded day. Fixed /28 is the textbook EWMA-smoothing interpretation; /n is the correct implementation for an athlete with a sparse history. The spec formula table previously said `/28` and `/42` — this was an error in the spec; the code (`compute.py: chronic_load()`) was already correct with `/n`. The spec has been updated to `/n` (see serving-store spec "Metric Formulas"). Partial-window behavior is now tested explicitly (`TestChronicLoad::test_partial_*_dynamic_denominator`). |

## File Changes (scaffolding targets for apply)

| Path | Action | Purpose |
|------|--------|---------|
| `docker-compose.yml` | Create | Services + profiles |
| `schemas/canonical/*.avsc` | Create | 3 canonical schemas |
| `bootstrap/register_schemas.py`, `bootstrap/create_topics.py` | Create | One-shot bootstrap |
| `jobs/canonicalize/` | Create | Raw→canonical PyFlink job |
| `jobs/metrics/` | Create | Metric PyFlink job |
| `storage/postgres/ddl.sql` | Create | `athlete_metrics` DDL |
| `storage/iceberg/tables.py` | Create | Iceberg table creation + compaction |
| `ingestion/strength/` | Create | Strong CSV watcher → `raw.strength` |
| `api/`, `ui/` | Create | FastAPI + Streamlit |

## Testing Strategy

| Layer | What | Approach |
|-------|------|----------|
| Unit | session_load formulas, ACR, deload rule, envelope mapping | pytest, table-driven from spec scenarios |
| Integration | canonicalize + metrics jobs | Flink mini-cluster / testcontainers Kafka + Registry |
| E2E | strength CSV → PG + Iceberg | Compose up `core`; assert PG row + DuckDB parity |

## Migration / Rollout

No migration — greenfield. Vertical slice (strength) first; wellness/planning branches and SHOULD metrics added incrementally behind the same topology.

## Open Questions

- [ ] None blocking. New trade-offs surfaced (subject strategy, exactly-once scope, event-time-window late-data handling, 24h conservative watermark on the mixed-latency canonical topic) are recorded as ADR-10..14. Late data is handled by native window `allowed_lateness` + `side_output_late_data` (ADR-11), serving freshness by an early-firing trigger (ADR-13) — both internally consistent with the LOCKED 24h watermark and wide-record decisions.
