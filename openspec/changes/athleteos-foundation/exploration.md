# Exploration: athleteos-foundation

## Phase Mapping

This single SDD change `athleteos-foundation` covers TWO conceptual phases:
- **PHASE 1 — Technical Blueprint**: End-to-end layered architecture, component responsibilities, data flow from each source to serving + historical, component classification (batch-ish / streaming / near-real-time).
- **PHASE 2 — Data Model & Contracts**: Canonical event model, Kafka topics with justified partitioning, Avro/JSON schemas per event type, PostgreSQL + Iceberg tables, schema evolution strategy, DLQ for invalid events.

## Current State

**Greenfield project.** No code, no infrastructure, no data model. Only a project brief (`prompt.md`) defining 6 data sources, 8 athlete metrics, a core technology stack (Kafka, Flink, Iceberg, PostgreSQL, FastAPI, Streamlit), and non-negotiable design principles.

The prompt explicitly:
- Rejects classic medallion (bronze/silver/gold) as the protagonist layer model
- Mandates event-time-first, keyed state, replay, idempotency, schema evolution
- Requires realistic ingestion patterns (CSV exports, Strava API, Apple Health XML — not perfect real-time APIs)
- Binds every component to problem/inputs/outputs/technology/trade-offs
- Separates MVP (6-8 weeks) from later phases
- Restricts "no tools just because" — justify every choice

## Dependent Patterns & Reference Architecture

### Data Platform Patterns Identified

| Pattern | Applies? | Why |
|---------|----------|-----|
| **Event-Staged Architecture** | YES | Raw → Canonical → Processed topics, not medallion tables. Kafka backbone with Flink stream processing. |
| **Kappa Architecture** | PARTIAL | Single processing pipeline for real-time and historical. Flink unifies batch/replay. But Iceberg layer adds analytical serving that goes beyond Kappa. |
| **CQRS** | YES | Write path (Kafka → Flink → PostgreSQL) vs Read path (FastAPI → PostgreSQL for operational, Trino/DuckDB → Iceberg for analytical). Separate models. |
| **Event Sourcing** | PARTIAL | Kafka topics as event log, but we do not rebuild entire state from events — PostgreSQL is source of truth for current metrics. |
| **Medallion (rejected)** | NO | Rejected by brief as protagonist. Bronze/Silver/Gold is table-centric, not stream-centric. We use processing stages, not quality tiers. |

### Why Not Classic Medallion

The brief explicitly rejects classic medallion (bronze/silver/gold) as the protagonist. Reasoning:
1. **Medallion is table-centric** — designed for Spark batch ETL into a data lake. Bronze=raw, silver=cleaned, gold=aggregated. The abstraction is a DATALAKE TABLE.
2. **This platform is stream-centric** — Kafka topics are the backbone, Flink is the processing engine, tables are materialized views from streams. The abstraction is an EVENT STREAM.
3. **Medallion conflates data quality with processing stage** — not every raw event is "bronze quality"; the same canonical event may be consumed for both operational serving and analytical queries simultaneously.
4. **Stream-native layers** (raw ingestion → canonical → processing → serving → analytical) are organized around EVENT FLOW AND PROCESSING SEMANTICS, not data quality tiers. This is the right model for Kafka+Flink.

**Alternative Layering Model — Event-Staged Architecture:**

```
┌─────────────────────────────────────────────────────────────────┐
│  RAW INGESTION LAYER                                           │
│  Source connectors, CSV parsers, API pollers, validation       │
│  Output: per-source raw Kafka topics                           │
│  Technology: Python scripts, Kafka producers, file watchers    │
├─────────────────────────────────────────────────────────────────┤
│  CANONICAL MODEL LAYER                                         │
│  Deserialize raw → validate → map to canonical event schema    │
│  Output: canonical Kafka topics (unified event model)          │
│  Technology: Flink (or lightweight Python transform)           │
│  Schema: Avro with Schema Registry, validated on produce       │
├─────────────────────────────────────────────────────────────────┤
│  STREAM PROCESSING LAYER                                       │
│  Keyed state, event-time windows, joins across event types     │
│  Output: computed metrics → PostgreSQL (operational)           │
│  Side: Iceberg via Flink Iceberg sink (analytical)             │
│  Technology: Flink, with keyed state (ValueState, MapState)    │
├─────────────────────────────────────────────────────────────────┤
│  SERVING LAYER (Operational)                                   │
│  Latest metrics per athlete, current scores, flags             │
│  Technology: PostgreSQL, optimized for point lookups           │
│  Accessed by: FastAPI REST, Streamlit UI                       │
├─────────────────────────────────────────────────────────────────┤
│  ANALYTICAL LAYER (Historical)                                 │
│  Full event history, trend analysis, cohort queries            │
│  Technology: Iceberg (columnar, partitioned, time-travel)      │
│  Optional engines: DuckDB (local/CI), Trino (distributed)      │
├─────────────────────────────────────────────────────────────────┤
│  API / UI LAYER                                                │
│  REST endpoints (FastAPI), dashboards (Streamlit)              │
│  Queries PostgreSQL for operational data, Iceberg for analytics │
└─────────────────────────────────────────────────────────────────┘
```

## Key Architectural Decisions (with Trade-offs)

### ADR-1: Layering Model — Event-Staged Architecture over Medallion

**Decision**: Adopt an event-staged layered architecture organized around stream processing stages, not table quality tiers.

**Trade-offs**:
- PRO: Natural fit for Kafka+Flink — topics are the abstraction, not tables
- PRO: Each layer has a clear processing concern (ingest → canonicalize → compute → serve → analyze)
- PRO: Enables composing canonical events from multiple raw sources without "bronze contamination" semantics
- CON: Less familiar to data engineers trained on Spark/databricks medallion — documentation needs to justify it
- CON: Without strong naming conventions, layers can blur into one another
- CON: Some batch-oriented consumers may prefer the familiarity of Bronze/Silver/Gold table names

### ADR-2: Event Contract — Avro + Schema Registry over JSON Schema

**Decision**: Use Avro serialization with Confluent Schema Registry for canonical event topics.

**Trade-offs**:
- PRO: Native Kafka+Schema Registry integration, first-class citizen
- PRO: Compact binary format (~10x smaller than JSON for numeric-heavy athlete data)
- PRO: Mature schema evolution model (BACKWARD, FORWARD, FULL, NONE compatibility settings)
- PRO: Stronger portfolio signal — demonstrates understanding of binary serialization and schema governance
- PRO: Avro schema can generate code or be used with generic record parsing (fastavro in Python)
- CON: Python toolchain is more painful — `fastavro` is excellent but `confluent_kafka.avro` has rough edges
- CON: Not human-readable in Kafka messages — debugging requires Avro deserialization
- CON: Schema Registry adds operational complexity (another stateful service to manage)
- CON: Avro type system can be limiting (no union of complex types without nesting)

**Open question**: Should raw topics also use Avro, or accept JSON for simplicity since raw formats vary so widely (CSV, API JSON, XML)?

### ADR-3: Topic Design — Two-Tier (Raw + Canonical)

**Decision**: Maintain both per-source raw topics AND canonical unified topics.

**Raw topics** (`raw.strength`, `raw.cardio`, `raw.recovery`, `raw.nutrition`, `raw.wellness`, `raw.planning`):
- Accept events in near-source format (parsed from CSV/API/XML but not fully normalized)
- Enable replay from source without re-fetching external APIs
- Serve as source-of-truth for data quality auditing
- Retention: time-bound (7-14 days) with compaction for replay safety

**Canonical topics** (`canonical.training_event`, `canonical.wellness_event`, `canonical.planning_block`, `canonical.athlete_metric`):
- Unified schema per domain, Avro-encoded with Schema Registry
- Consumers (Flink jobs) read ONLY from canonical topics — never raw
- Enable adding new data sources without changing downstream processing
- Retention: compacted + time-bound as needed

**Trade-offs**:
- PRO: Clean separation of concerns — ingestion owns raw, processing owns canonical
- PRO: Future data sources map to canonical model without affecting consumers
- PRO: Can replay from canonical (with schema evolution) without touching raw
- CON: Doubles topic count and storage
- CON: Ingestion layer must maintain ETL mapping from raw → canonical (a "canonicalization" step)
- CON: Adds latency: raw → canonical transform → processing

### ADR-4: Partitioning Key — athlete_id

**Decision**: Partition ALL canonical topics by `athlete_id`.

**Justification**:
- All metrics are per-athlete: acute/chronic load, readiness, recovery, fatigue, adherence
- Flink keyed state (`keyBy(athlete_id)`) maps 1:1 with Kafka partitioning, enabling efficient stateful processing
- Joins across topics (training + recovery + wellness) require co-partitioning — same key, same partition count

**Trade-offs**:
- PRO: All events for an athlete are ordered within a partition — critical for correct time-series computation
- PRO: Co-partitioning enables efficient stream-stream joins without reshuffling
- CON: **Skew risk** — a professional athlete training 4h/day produces more events than a casual athlete
- CON: Partition count is fixed (or requires repartitioning which is expensive)
- CON: If one athlete produces orders of magnitude more data, that partition becomes a bottleneck

**Skew mitigation strategies**:
1. Over-partition (e.g., 16 partitions for a single-broker dev setup, target 5-10 athletes per partition)
2. Accept skew for MVP — document it as a known limitation with rebalancing as future work
3. Composite key (athlete_id + source_type) — breaks ordering guarantee within a single ID

**Recommendation for MVP**: Over-partition and monitor. Skew is manageable for portfolio-scale data (100s-1000s of athletes, not millions). Document the skew analysis as a talking point for interviews.

### ADR-5: Ingestion Layer — Realistic Patterns for Non-Streaming Sources

**Critical insight**: 5 of 6 sources are fundamentally batch-ish (CSV exports, XML exports, YAML files). Only Strava API can be polled in near-real-time. The architecture must acknowledge this and design accordingly.

**Proposed ingestion patterns**:

| Source | Format | Ingestion Pattern | Realism |
|--------|--------|-------------------|---------|
| Strength | Strong CSV export | File watcher → CSV parser → Kafka producer. Micro-batch every N minutes or on file creation. | High. CSV export is the real Strong behavior. |
| Cardio | Strava API | Polling every 5-15 min via cron/scheduler. Strava webhooks possible but require registered app. | High. Polling is standard for non-premium APIs. |
| Recovery | Apple Health XML export | Batch export (user uploads). Parse XML → Kafka. | High. Apple Health does not stream. |
| Nutrition | Manual CSV | File upload or scheduled import. | High. Manual nutrition tracking is batch. |
| Wellness | Manual/form | Direct input via API → Kafka. Near-real-time possible. | Medium. Can be built as form → API → Kafka. |
| Planning | YAML/JSON/CSV | Pre-season upload. Batch. | High. Planning is inherently batch. |

**Architectural implication**: The "event backbone" receives events in micro-batches, not as a continuous low-latency stream. Flink handles this transparently via event-time — it doesn't care if events arrive 5 minutes apart or 5 hours apart, as long as watermarks are set correctly.

**Watermark strategy**: Use periodic watermarks with generous allowed lateness (e.g., 1 hour for near-real-time events, 24 hours for batch-uploaded events). Configure `allowedLateness` on window operators to handle late CSV uploads.

**Idempotency for replay**: Each event carries a deduplication ID (event_id UUID). Flink `idempotent` sink to PostgreSQL uses UPSERT on (athlete_id, metric_name, date). Iceberg sink with `append` is inherently idempotent for replay within the same checkpoint boundary.

### ADR-6: Processing Language — PyFlink vs Java Flink

**Decision**: MVP uses PyFlink for stream processing jobs. Java Flink is a future optimization.

**Trade-offs**:
- PRO: Python-only codebase (simplifies hiring signal for a portfolio — a single-language project is more accessible to reviewers)
- PRO: PyFlink supports the full DataStream API with Python UDFs
- PRO: Can share data validation logic between ingestion (Python) and processing (PyFlink)
- CON: PyFlink has higher per-event overhead (Python serialization boundary between Java operator and Python UDF)
- CON: State access from Python UDFs has latency — high-throughput operations may bottleneck
- CON: Portability — if the portfolio needs to show Flink expertise, Java/Scala is the industry standard
- CON: Some advanced features (asynchronous I/O, custom state) are harder or unavailable in PyFlink

**Risk for MVP**: For MVP data volumes (synthetic/small-scale), PyFlink overhead is negligible. Document this as a conscious trade-off with a threshold for Java migration when throughput exceeds X events/sec.

### ADR-7: Operational vs Analytical Persistence Split

**PostgreSQL — Operational Serving**:
- Stores latest computed metrics per athlete per day
- Optimized for: point lookups (`SELECT * FROM athlete_metrics WHERE athlete_id=? AND date=?`)
- Data model: wide-ish table with columns per metric (acute_load, chronic_load, acr, readiness, etc.)
- Updated UPSERT-style by Flink sink (idempotent)
- Accessed by: FastAPI endpoints, Streamlit dashboard

**Iceberg — Analytical Layer**:
- Stores full event history, raw events, canonical events, metric computation snapshots
- Optimized for: range scans, historical trends, cohort analysis, time-travel queries
- Partitioned by (athlete_id, date_hour) or (athlete_id, date)
- Format: Parquet (columnar, compressed)
- Writes: Flink Iceberg sink (streaming or checkpoint-based)
- Reads: via DuckDB (local dev/CI) or Trino (production analytics)

**DuckDB evaluation**: Include in MVP. It's zero-operational-overhead (embedded), reads Iceberg/Parquet directly, and enables ad-hoc SQL analysis with zero infrastructure. Essential for local development and CI testing.

**dbt-core evaluation**: DEFER past MVP. dbt shines when transformations are SQL-based and need documentation/testing lineage. But in a streaming architecture, transformations happen in Flink (Python/Java), not in SQL batch. dbt for the Iceberg layer would be for analytical models (aggregations, cohort tables) — valuable but not MVP.

**Trino evaluation**: DEFER. Trino adds a distributed query engine for Iceberg. Useful when multi-engine access or federated queries are needed. For an MVP with DuckDB, Trino is operational overhead without proportional benefit.

### ADR-8: Metric Computation Placement — Streaming vs Batch-ish Classification

| Metric | Classification | Where | Why |
|--------|---------------|-------|-----|
| acute_load (7d) | **Streaming** | Flink, sliding window (7 days), event-time | Rolling computation over training events. Natural window. |
| chronic_load (28d/42d) | **Streaming** | Flink, sliding window (28d/42d), event-time | Same pattern as acute, longer window. |
| acute_chronic_ratio | **Streaming** | Flink, join acute+chronic streams | Simple division triggered on both sides updating. |
| readiness_score | **Near-real-time** | Flink, combines load + recovery + wellness | Depends on training events + sleep/HRV + wellness input. Can stream but recovery is daily. |
| recovery_score | **Near-real-time** | Flink, keyed state over recovery events | Updated when new sleep/HRV data arrives. |
| fatigue_score | **Near-real-time** | Flink, composite from load + recovery + wellness | Multiple contributing factors, recompute on any input change. |
| adherence_score | **Batch-ish** | Flink with session window or daily tumbling | Requires joining planning (batch, weekly) with actual training (streaming). |
| deload_flag / coaching_flags | **Near-real-time** | Flink, threshold check on ACR | Simple rule: if ACR > 1.3 or < 0.8 for N consecutive days, flag. |

**Key insight**: All metrics can be computed in Flink with keyed state and event-time windows. None require a batch system. The "batch-ish" classification for adherence_score is about input timing (planning data arrives weekly), not processing paradigm.

### ADR-9: DLQ and Data Quality

**DLQ Design**:
- One DLQ topic per canonical topic (e.g., `dlq.canonical.training_event`)
- Invalid events are JSON-with-error-detail (not Avro — schema itself may be unparseable)
- Fields: original_topic, original_key, original_value (as bytes), error_type, error_message, timestamp
- Consumers: DLQ dashboard (Streamlit) for monitoring, automated retry handler

**Schema evolution DLQ handling**:
- On schema incompatibility (BACKWARD mismatch), the Schema Registry rejects the producer
- On incompatible reader (consumer cannot handle new schema), the consumer routes to DLQ
- Flink jobs should validate schema compatibility at startup via Schema Registry client

## Component Classification Summary

| Component | Classification | Rationale |
|-----------|---------------|-----------|
| Strong CSV File Watcher | **Batch-ish** | File arrival triggers processing. No streaming API. |
| Strava API Poller | **Near-real-time** | Polls every 5-15 min. Can approach streaming with webhooks. |
| Apple Health XML Parser | **Batch-ish** | User-initiated export upload. |
| Nutrition CSV Parser | **Batch-ish** | File upload or manual entry. |
| Wellness Input Service | **Near-real-time** | Can accept events as they come via API. |
| Planning Ingestion | **Batch-ish** | Pre-season file upload. Infrequent. |
| Raw→Canonical Transform (Flink) | **Streaming** | Consumes raw topics, emits canonical. Continuous. |
| Acute/Chronic Load Computation | **Streaming** | Flink sliding windows over canonical training events. |
| Readiness/Recovery/Fatigue | **Near-real-time** | Composite metrics, recomputed on new relevant events. |
| Adherence Score | **Batch-ish** | Joins planning (batch) with training (stream). Daily compute. |
| PostgreSQL Sink (Flink) | **Streaming** | Each window/trigger outputs to PostgreSQL via UPSERT. |
| Iceberg Sink (Flink) | **Micro-batch** | Flink Iceberg connector writes on checkpoint boundaries. |
| FastAPI Serving | **Near-real-time** | Reads current state from PostgreSQL. |
| Streamlit UI | **Near-real-time** | Renders from FastAPI or direct PostgreSQL reads. |
| DuckDB Queries (analytical) | **Batch/Ad-hoc** | Interactive SQL. Not continuous. |
| DLQ Consumer/Monitor | **Batch-ish** | Periodic review. Could be alert-driven. |

## Open Questions for User Decision (STOP AND ASK)

The following trade-offs need user input before the propose phase. The orchestrator MUST surface these in interactive mode:

### Q1: Avro + Schema Registry vs Hybrid (JSON raw + Avro canonical)
Avro gives stronger portfolio signal but adds Python dev friction. **Do you want Avro on canonical topics only (raw remains JSON) OR Avro everywhere?** The raw topic performance impact is minimal for portfolio scale. JSON on raw simplifies inspection and debugging.

### Q2: PyFlink vs Java Flink for stream processing
PyFlink keeps the codebase Python-only (simpler portfolio) but limits Flink job performance and advanced features. Java Flink is the industry standard for a Data Engineering portfolio. **Do you accept PyFlink for MVP with a documented threshold to migrate?**

### Q3: Raw topics retention policy
Raw topics duplicate storage but protect replay safety. **Time-bound retention (7-14 days, delete old raw events) or compacted retention (keep latest per key for replay)?** Compacted raw topics enable replay of the latest state but not event history. If replay matters, time-bound + full event log is better.

### Q4: DuckDB — include in MVP or defer?
DuckDB adds a powerful local query engine for Iceberg with zero ops cost. It makes local development and CI testing much richer. **Include DuckDB in MVP (it's tiny, zero infrastructure) or defer to Phase 6/7?**

### Q5: Partition count and scaling strategy
Partitioning by athlete_id is the right call, but how many partitions for MVP? **Single-broker with small partition count (4-8) and document the scaling strategy, or start with 16+ partitions to demonstrate understanding of over-partitioning as skew mitigation?**

### Q6: Flink checkpoints and state backend
For MVP with Docker Compose, the state backend matters. **Filesystem-based state (simple, ephemeral) or RocksDB (heavier but more realistic for a portfolio)?** RocksDB shows senior knowledge but adds complexity for local dev.

### Q7: Iceberg catalog choice for development
**Use the built-in Hadoop catalog (file-based, simple) or set up a Polaris REST catalog (production-grade, but more infrastructure)?** Hadoop catalog is fine for MVP. Polaris adds portfolio signal but is additional ops.

### Q8: MVP scope boundary
The brief defines 6-8 weeks for MVP. **Which metrics are truly MVP vs deferred?**
- MUST have: acute_load, chronic_load, acute_chronic_ratio, deload_flag
- SHOULD have: readiness_score, fatigue_score, adherence_score
- COULD defer: coaching_flags beyond deload, advanced analytics dashboards

## Risks

1. **Skew risk on athlete_id partitioning** — if not acknowledged and monitored, can cause partition imbalance and backpressure
2. **PyFlink performance ceiling** — if MVP scales beyond synthetic data, Python overhead becomes a bottleneck requiring redesign
3. **Schema Registry as single point of failure** — if Schema Registry is down, all producers/consumers fail. Must be in the critical recovery plan
4. **DLQ accumulation without monitoring** — if DLQ is not monitored, invalid events accumulate silently and data quality erodes
5. **PostgreSQL write pressure** — Flink sink writing per-event UPSERTs can overwhelm a small PostgreSQL instance. Micro-batch the sink writes.
6. **Iceberg small file problem** — streaming writes to Iceberg generate many small Parquet files. Must plan compaction maintenance.
7. **Complexity creep** — the stack is Kafka+Flink+PostgreSQL+Iceberg+FastAPI+Streamlit+Prometheus+Grafana+DuckDB for MVP. This is a LOT of services in Docker Compose. Each add increases dev friction.

## Ready for Proposal

**YES** — but ONLY after the user answers the 8 questions above. The answers will materially change the proposal direction.

The exploration has identified clear architectural patterns, justified trade-offs, and mapped all 6 data sources to realistic ingestion patterns. The event-staged architecture (not medallion) is correctly scoped, the two-tier topic design is justified, and the metric split between streaming/batch-ish is classified.

Make sure the orchestrator reads `SDD PHASE COMMON Section E (Review Workload Guard)` — this change covers two conceptual phases (blueprint + data model/contracts) in one SDD change. The proposal and spec phases need to be substantial enough to cover both, and the task breakdown must respect the 400-line review budget.
