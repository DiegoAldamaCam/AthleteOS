# Architecture Specification

## Purpose

Defines the Event-Staged Architecture for AthleteOS: a 6-layer stream-centric platform where layers are organized by processing semantics and event flow, NOT by data quality tiers (explicitly rejecting classic Medallion). Each layer has bounded responsibility, explicit inputs/outputs, and a declared processing classification.

## Requirements

### Requirement: Six-Layer Event-Staged Architecture

The system MUST implement 6 layers organized by event processing stage. Layers communicate via Kafka topics or direct sink writes. No layer MAY skip an intermediate layer.

| # | Layer | Responsibility | Inputs | Outputs | Technology | Classification |
|---|-------|---------------|--------|---------|------------|----------------|
| 1 | **Raw Ingestion** | Accept heterogeneous source formats (CSV, XML, API JSON, YAML), produce source-faithful events | Source files, API responses | `raw.*` Kafka topics (JSON) | Python producers, file watchers, API pollers | Batch-ish (5/6 sources) |
| 2 | **Canonical Model** | Validate, normalize, and map raw events to governed Avro contracts | `raw.*` topics | `canonical.*` topics (Avro) | PyFlink transform, Schema Registry | Streaming |
| 3 | **Stream Processing** | Compute metrics with event-time semantics, keyed state, windowed aggregations | `canonical.*` topics | Metric updates → PG; events → Iceberg | PyFlink, RocksDB state backend | Streaming |
| 4 | **Serving** | Point lookups for latest computed athlete metrics | Flink sink output | REST API responses | PostgreSQL (UPSERT) | Near-real-time |
| 5 | **Analytical** | Historical queries, trend analysis, time-travel over full event history | Flink Iceberg sink | Ad-hoc SQL results | Iceberg (Parquet), DuckDB | Micro-batch / Ad-hoc |
| 6 | **API / UI** | Human and programmatic access to operational and analytical data | PG + Iceberg | JSON responses, dashboards | FastAPI, Streamlit | Near-real-time |

#### Scenario: End-to-end strength event flow

- GIVEN a Strong CSV file is dropped in the watched directory
- WHEN the file watcher detects it
- THEN events flow: CSV → `raw.strength` (JSON) → canonical transform → `canonical.training_event` (Avro) → Flink computes metrics → PostgreSQL UPSERT + Iceberg append → FastAPI serves latest metrics

#### Scenario: Layer isolation

- GIVEN the canonical model layer schema changes (new optional field)
- WHEN downstream Flink jobs consume the updated schema
- THEN the serving and analytical layers are unaffected (they consume Flink output, not canonical events directly)

---

### Requirement: Data Flow Per Source

Each of the 6 data sources MUST follow a defined path from ingestion to both serving and analytical stores.

| Source | Ingestion Pattern | Raw Topic | Canonical Topic | Processing | Serving | Analytical |
|--------|-------------------|-----------|-----------------|------------|---------|------------|
| Strength (Strong CSV) | File watcher → CSV parser | `raw.strength` | `canonical.training_event` | acute/chronic load, ACR, deload_flag | PG `athlete_metrics` | Iceberg `training_event` |
| Cardio (Strava) | API poller (5-15 min) | `raw.cardio` | `canonical.training_event` | session_load from TSS/HR | PG `athlete_metrics` | Iceberg `training_event` |
| Recovery (Apple Health) | Batch XML upload | `raw.recovery` | `canonical.wellness_event` | readiness, fatigue composites | PG `athlete_metrics` | Iceberg `wellness_event` |
| Nutrition (CSV) | File upload | `raw.nutrition` | `canonical.wellness_event` | adherence_score | PG `athlete_metrics` | Iceberg `wellness_event` |
| Wellness (manual) | API input | `raw.wellness` | `canonical.wellness_event` | readiness, fatigue composites | PG `athlete_metrics` | Iceberg `wellness_event` |
| Planning (YAML/JSON) | Pre-season upload | `raw.planning` | `canonical.planning_block` | adherence (planned vs actual) | PG `athlete_metrics` | Iceberg `planning_block` |

#### Scenario: Multi-source metric computation

- GIVEN an athlete has strength events (training) and Apple Health recovery events
- WHEN Flink processes both canonical streams keyed by `athlete_id`
- THEN the readiness_score composite is computed from both training load and recovery inputs

---

### Requirement: Component Classification Contract

Every component in the system MUST declare its processing classification. This classification governs watermark strategy, checkpoint intervals, and latency expectations.

| Classification | Definition | Watermark Strategy | Checkpoint Alignment |
|----------------|-----------|-------------------|---------------------|
| **Batch-ish** | Triggered by file arrival or scheduled poll; events arrive in bursts | Periodic, generous lateness (24h allowed) | N/A (producers, not Flink) |
| **Streaming** | Continuous consumption from Kafka topics; event-time windows | Periodic, 1h allowed lateness | Aligned with Flink checkpoints |
| **Near-real-time** | Low-latency serving or composite metrics updated on input change | Inherits from upstream | Aligned with Flink checkpoints |
| **Micro-batch** | Writes on checkpoint boundaries (not per-event) | N/A (sink) | Checkpoint-aligned writes |
| **Ad-hoc** | Interactive queries, not continuous | N/A | N/A |

**Component classification table:**

| Component | Classification |
|-----------|---------------|
| Strong CSV File Watcher | Batch-ish |
| Strava API Poller | Near-real-time (5-15 min poll) |
| Apple Health XML Parser | Batch-ish |
| Nutrition CSV Parser | Batch-ish |
| Wellness Input Service | Near-real-time |
| Planning Ingestion | Batch-ish |
| Raw→Canonical Transform | Streaming |
| Acute/Chronic Load Computation | Streaming |
| Readiness/Fatigue Computation | Near-real-time |
| Adherence Score | Batch-ish (joins batch planning + stream training) |
| PostgreSQL Sink | Streaming (micro-batch UPSERT) |
| Iceberg Sink | Micro-batch (checkpoint-aligned) |
| FastAPI Serving | Near-real-time |
| Streamlit UI | Near-real-time |
| DuckDB Queries | Ad-hoc |
| DLQ Monitor | Batch-ish |

#### Scenario: Watermark configuration for batch source

- GIVEN a CSV file uploaded 48 hours after the events it contains
- WHEN Flink processes with 24h allowed lateness
- THEN events within the lateness window are included in window computations; events older than 24h beyond the watermark are dropped to side output

---

### Requirement: Idempotency and Replay

The system MUST guarantee idempotent processing end-to-end.

- Producers MUST generate a deterministic `event_id` (UUID v4) per event
- Flink MUST deduplicate on `event_id` using keyed state
- PostgreSQL sink MUST use UPSERT on `(athlete_id, metric_date)` — reprocessing the same window produces the same row
- Iceberg sink MUST use append mode within checkpoint boundaries — replay within the same checkpoint is idempotent

#### Scenario: Replay after failure

- GIVEN Flink restarts from a checkpoint and reprocesses events from offset N
- WHEN events with already-processed `event_id` values are encountered
- THEN Flink deduplicates them and no duplicate rows appear in PostgreSQL or Iceberg

---

### Requirement: Technology Stack Binding

The system MUST use the following technology stack. Each choice is LOCKED per ADR and MUST NOT be substituted without a new ADR.

| Concern | Technology | ADR |
|---------|-----------|-----|
| Event backbone | Apache Kafka | ADR-3 |
| Schema governance | Confluent Schema Registry | ADR-2 |
| Stream processing | Apache Flink (PyFlink for MVP) | ADR-6 |
| State backend | RocksDB | ADR-7 |
| Operational store | PostgreSQL | ADR-8 |
| Analytical store | Apache Iceberg (Parquet) | ADR-8, ADR-9 |
| Local analytics | DuckDB | ADR-8 |
| API framework | FastAPI | — |
| Dashboard | Streamlit | — |
| Containerization | Docker Compose | — |
| Language | Python 3.12+ (Java 11+ for Flink runtime) | ADR-6 |
