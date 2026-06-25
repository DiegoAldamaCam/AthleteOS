# Proposal: AthleteOS Foundation — Blueprint & Data Contracts

## Intent

Build the architectural foundation for AthleteOS: a stream-centric data platform that centralizes 6 heterogeneous athlete data sources into a unified event model, computes training-load metrics in real time, and serves them via operational and analytical paths. Portfolio project targeting Data Engineer reviewers — every choice signals pragmatic seniority, not tool worship.

**Problem**: Greenfield. No code, no infrastructure, no data model. The challenge is designing a coherent event-driven architecture from scratch that handles batch-ish sources (CSV, XML, YAML) alongside near-real-time APIs, with justified technology choices and explicit trade-offs.

## Scope

### In Scope (Phases 1–2 of this change)

- Event-Staged Architecture: 6 layers, component responsibilities, data flow from all 6 sources
- Two-tier Kafka topic design (`raw.*` + `canonical.*`) with partitioning strategy
- Canonical event model with Avro schemas (canonical) and JSON (raw)
- Schema evolution governance (BACKWARD / FORWARD compatibility)
- DLQ design for invalid events
- PostgreSQL operational schema + Iceberg analytical tables
- 9 consolidated ADRs (all LOCKED from exploration)
- MVP vertical-slice definition (strength → end-to-end)

### Out of Scope (Phases 3–7, future changes)

- Flink job implementation (Phase 3)
- Repository scaffolding, Docker Compose (Phase 4)
- Source connector implementation (Phase 5)
- FastAPI endpoints, Streamlit views (Phase 6)
- Observability, tests, data quality checks (Phase 7)

## Capabilities

> Contract between proposal and spec phases. Each becomes `openspec/specs/<name>/spec.md`.

### New Capabilities

- `architecture`: Event-staged layered architecture — 6 layers, component responsibilities, data flow from all 6 sources to serving + analytical, batch/streaming/NRT classification per component
- `event-contracts`: Canonical event model, Kafka topic design (raw + canonical), Avro/JSON schemas per event type, partitioning by athlete_id (8 partitions), schema evolution with Schema Registry, DLQ routing
- `serving-store`: PostgreSQL schema for operational metric serving — latest computed metrics per athlete per day, UPSERT semantics from Flink sink
- `analytical-store`: Iceberg table schema for historical/analytical queries — full event history, partitioned by (athlete_id, date), Parquet format, Hadoop file-based catalog

### Modified Capabilities

None (greenfield project — no existing specs).

## Approach

### Architecture: Event-Staged (NOT Medallion)

| Layer | Problem Solved | Inputs | Outputs | Technology | Key Trade-off |
|-------|---------------|--------|---------|------------|---------------|
| **Raw Ingestion** | Accept heterogeneous formats (CSV, XML, API JSON, YAML) | Source files, API responses | `raw.*` Kafka topics (JSON) | Python producers, file watchers, API pollers | Source-faithful & replayable vs. doubles topic count |
| **Canonical Model** | Unify events across sources into governed contracts | `raw.*` topics | `canonical.*` topics (Avro) | Flink/Python transform, Confluent Schema Registry | Add sources without changing consumers vs. canonicalization latency |
| **Stream Processing** | Compute metrics with event-time semantics, keyed state | `canonical.*` topics | Metric updates → PG; events → Iceberg | PyFlink, RocksDB state backend | Unified batch/stream vs. PyFlink perf ceiling |
| **Serving** | Point lookups for latest athlete metrics | Flink sink output | REST API responses | PostgreSQL (UPSERT) | Simple & proven vs. write pressure from Flink |
| **Analytical** | Historical queries, trend analysis, time-travel | Flink Iceberg sink | Ad-hoc SQL results | Iceberg (Parquet), DuckDB | Zero-ops analytics vs. small-file problem |
| **API / UI** | Human and programmatic access | PG + Iceberg | Dashboards, JSON | FastAPI, Streamlit | Rapid prototyping vs. Streamlit not production-grade |

### Consolidated ADRs (LOCKED — decided, not open)

| # | Decision | Rationale |
|---|----------|-----------|
| ADR-1 | Event-Staged over Medallion | Stream-centric. Layers by processing semantics, not quality tiers |
| ADR-2 | Hybrid serialization | JSON on raw (debuggable, heterogeneous). Avro + Schema Registry on canonical (governed evolution, compact binary) |
| ADR-3 | Two-tier topics (raw + canonical) | Ingestion owns raw, processing owns canonical. New sources map without affecting consumers |
| ADR-4 | Partition by athlete_id, 8 partitions | Natural key for per-athlete metrics. Co-partitioning enables joins. Skew documented as ADR |
| ADR-5 | Realistic ingestion patterns | 5/6 sources are batch-ish. Architecture designed for micro-batch, not continuous stream |
| ADR-6 | PyFlink for MVP | Python-only repo, shared validation. Documented events/sec threshold for Java migration |
| ADR-7 | RocksDB state backend | Production-grade, handles large keyed state for 28/42d windows |
| ADR-8 | Operational/Analytical split | PostgreSQL for point lookups. Iceberg for historical. DuckDB for local/CI |
| ADR-9 | Hadoop Iceberg catalog | File-based, simple. Defer Polaris REST catalog |

### MVP Scope (6–8 weeks) — Vertical Slice First

**MUST** (vertical slice: strength training end-to-end):

1. Strong CSV → file watcher → `raw.strength` (JSON)
2. Raw → canonical → `canonical.training_event` (Avro + Schema Registry)
3. Flink: acute_load(7d), chronic_load(28/42d), acute_chronic_ratio, deload_flag
4. PostgreSQL sink (UPSERT `athlete_metrics`)
5. Iceberg sink (`training_event` history)
6. FastAPI: `GET /athletes/{id}/metrics`
7. Streamlit: single metric dashboard view
8. DuckDB: ad-hoc query on Iceberg
9. Schema Registry validation + schema evolution
10. DLQ for invalid canonical events

**SHOULD** (if time after vertical slice):
- readiness_score, fatigue_score, adherence_score
- Additional source connectors (Strava, Apple Health)

**DEFER** (Phases 3–7):
- Advanced coaching_flags, full observability stack, dbt-core, Trino, multi-athlete scaling tests

### CV Narrative — Why Each Choice Signals Seniority

| Choice | Seniority Signal |
|--------|-----------------|
| Event-Staged over Medallion | Understands stream-native architecture, not just Spark batch patterns |
| Avro + Schema Registry | Schema governance, binary serialization, compatibility modes |
| DLQ for invalid events | Data quality thinking beyond the happy path |
| RocksDB state backend | Knows Flink internals — state management, checkpointing, large state |
| Hybrid JSON/Avro | Pragmatic: debuggability where it matters, governance where it counts |
| Iceberg + DuckDB | Modern analytical stack, zero-ops local development |
| Vertical slice MVP | Depth over breadth — end-to-end thinking |
| Partition skew awareness | Understands distributed systems failure modes |

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `openspec/specs/architecture/` | New | Event-staged architecture spec with layer definitions |
| `openspec/specs/event-contracts/` | New | Canonical event model, topic design, Avro/JSON schemas |
| `openspec/specs/serving-store/` | New | PostgreSQL operational schema spec |
| `openspec/specs/analytical-store/` | New | Iceberg analytical table schema spec |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| athlete_id partition skew | Med | 8 partitions for dev, document skew-analysis ADR, monitor partition lag |
| PyFlink performance ceiling | Low (MVP) | Document events/sec threshold for Java migration; MVP volumes are small |
| Schema Registry as SPOF | Med | Docker Compose health checks; document recovery procedure |
| DLQ accumulation without monitoring | Med | Streamlit DLQ dashboard, alerting on DLQ depth |
| PostgreSQL write pressure | Med | Micro-batch Flink sink (checkpoint-aligned), not per-event |
| Iceberg small files | High | Compaction maintenance job; checkpoint-aligned writes reduce count |
| Docker Compose service count | Med | Compose profiles to start only needed services per workflow |

## Rollback Plan

Greenfield — no existing system to roll back to. If architecture proves unsound during Phases 3–7:
- Layers can be collapsed (merge raw + canonical) without full redesign
- Avro schemas can migrate to JSON if Schema Registry overhead is unjustified
- Iceberg can be replaced with plain Parquet if catalog complexity blocks progress
- Each layer is independently replaceable — modular by design

## Dependencies

- Docker + Docker Compose (local development runtime)
- Confluent Platform images (Kafka, Schema Registry) or Apache Kafka + Apicurio Registry alternative
- Python 3.12+ (PyFlink, ingestion scripts, FastAPI)
- Java 11+ (Flink runtime, even with PyFlink UDFs)

## Success Criteria

- [ ] Architecture spec documents all 6 layers with responsibilities, inputs, outputs, technology, trade-offs
- [ ] Event contract spec defines canonical events for all 6 source domains with Avro schemas
- [ ] Topic design spec covers raw + canonical topics, partitioning (8 by athlete_id), retention policies
- [ ] Schema evolution spec covers BACKWARD/FORWARD compatibility, DLQ routing, Registry integration
- [ ] PostgreSQL schema spec covers operational metric tables with UPSERT semantics
- [ ] Iceberg schema spec covers analytical tables with partitioning strategy (Hadoop catalog)
- [ ] All 9 ADRs documented with trade-offs and rationale
- [ ] MVP vertical slice (strength → end-to-end) is demonstrable
- [ ] Every technology choice justified with trade-offs — no "tools just because"
