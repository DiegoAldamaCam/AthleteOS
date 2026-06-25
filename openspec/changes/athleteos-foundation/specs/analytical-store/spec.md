# Analytical Store Specification

## Purpose

Defines the Apache Iceberg table schemas for historical and analytical queries over AthleteOS event data. Optimized for range scans, trend analysis, and time-travel queries. Read by DuckDB (local/CI) and future Trino. Written by Flink Iceberg sink. Uses Hadoop file-based catalog (LOCKED ADR-9).

## Requirements

### Requirement: Training Event History Table

The system MUST maintain an Iceberg `training_event` table storing the full canonical training event history.

**Table Schema:**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | string | NO | Unique event identifier |
| `event_time` | timestamp(3) | NO | Event-time (millisecond precision) |
| `ingest_time` | timestamp(3) | NO | Ingestion wall-clock time |
| `source` | string | NO | Origin source identifier |
| `schema_version` | int | NO | Avro schema version |
| `athlete_id` | string | NO | Partition key |
| `event_type` | string | NO | STRENGTH_SET or CARDIO_ACTIVITY |
| `workout_id` | string | YES | Workout identifier (strength) |
| `exercise_id` | string | YES | Exercise identifier (strength) |
| `set_number` | int | YES | Set number (strength) |
| `reps` | int | YES | Repetitions (strength) |
| `weight_kg` | float | YES | Weight in kg (strength) |
| `rpe` | float | YES | Rate of perceived exertion |
| `rir` | float | YES | Reps in reserve |
| `activity_type` | string | YES | Activity type (cardio) |
| `distance_km` | float | YES | Distance (cardio) |
| `duration_sec` | int | YES | Duration (cardio) |
| `avg_hr` | int | YES | Average heart rate (cardio) |
| `tss` | float | YES | Training Stress Score (cardio) |
| `session_load` | float | NO | Computed session load |

**Partitioning**: Identity partition on `(athlete_id, days(event_time))`.

- `athlete_id` partition: enables per-athlete data isolation and pruning
- `days(event_time)` partition: daily granularity for range scans and compaction

**File format**: Parquet (columnar, compressed).

**Catalog**: Hadoop file-based catalog (LOCKED ADR-9). Catalog path: configurable, default `s3://athleteos-lake/warehouse` or local `./warehouse` for dev.

#### Scenario: Per-athlete range scan

- GIVEN a query for athlete-1's training events in June 2026
- WHEN DuckDB reads the Iceberg table
- THEN partition pruning skips all non-athlete-1 and non-June partitions

#### Scenario: Time-travel query

- GIVEN the table has multiple snapshot versions
- WHEN a query specifies `FOR SYSTEM_TIME AS OF <timestamp>`
- THEN Iceberg returns the table state at that snapshot

---

### Requirement: Wellness Event History Table

The system MUST maintain an Iceberg `wellness_event` table for recovery, nutrition, and wellness history.

**Table Schema:**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | string | NO | Unique event identifier |
| `event_time` | timestamp(3) | NO | Event-time |
| `ingest_time` | timestamp(3) | NO | Ingestion time |
| `source` | string | NO | Origin source |
| `schema_version` | int | NO | Schema version |
| `athlete_id` | string | NO | Partition key |
| `event_type` | string | NO | RECOVERY_SNAPSHOT, NUTRITION_DAILY, or WELLNESS_DAILY |
| `sleep_hours` | float | YES | Sleep duration |
| `resting_hr` | int | YES | Resting heart rate |
| `hrv` | float | YES | Heart rate variability |
| `steps` | int | YES | Daily step count |
| `body_weight_kg` | float | YES | Body weight |
| `calories` | int | YES | Daily caloric intake |
| `protein_g` | float | YES | Protein grams |
| `carbs_g` | float | YES | Carbohydrate grams |
| `fat_g` | float | YES | Fat grams |
| `nutrition_adherence` | float | YES | Nutrition adherence score |
| `energy` | int | YES | Energy level (1-5) |
| `soreness` | int | YES | Soreness level (1-5) |
| `mood` | int | YES | Mood level (1-5) |
| `stress` | int | YES | Stress level (1-5) |
| `perceived_recovery` | int | YES | Perceived recovery (1-5) |

**Partitioning**: Identity partition on `(athlete_id, days(event_time))`.

**File format**: Parquet.

#### Scenario: Cross-table join for readiness analysis

- GIVEN `training_event` and `wellness_event` Iceberg tables
- WHEN DuckDB joins them on `(athlete_id, date)` for a trend analysis
- THEN partition pruning on both tables reduces I/O to relevant partitions only

---

### Requirement: Planning Block History Table

The system MUST maintain an Iceberg `planning_block` table for training plan history.

**Table Schema:**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | string | NO | Unique event identifier |
| `event_time` | timestamp(3) | NO | Event-time |
| `ingest_time` | timestamp(3) | NO | Ingestion time |
| `source` | string | NO | Origin source |
| `schema_version` | int | NO | Schema version |
| `athlete_id` | string | NO | Partition key |
| `block_id` | string | NO | Planning block identifier |
| `goal` | string | NO | Training goal |
| `start_date` | timestamp(3) | NO | Block start date |
| `end_date` | timestamp(3) | NO | Block end date |
| `planned_sessions_per_week` | int | NO | Planned weekly sessions |
| `weekly_volume_targets` | string | NO | JSON-encoded volume targets |

**Partitioning**: Identity partition on `athlete_id`. Planning blocks are infrequent — date partitioning is unnecessary.

**File format**: Parquet.

---

### Requirement: Compaction and Maintenance

The system MUST address the Iceberg small-file problem caused by streaming writes.

- Flink Iceberg sink writes one or more Parquet files per checkpoint per partition
- At MVP scale (8 partitions, few athletes), small files are acceptable
- A compaction maintenance job SHOULD be implemented as a scheduled task that:
  - Rewrites small files (< 128MB) into larger files
  - Removes orphaned files older than retention period
  - Runs via `iceberg-python` rewrite_data_files action or equivalent

**Compaction trigger**: Files smaller than 128MB in a partition with more than 10 files.

#### Scenario: Small file accumulation

- GIVEN Flink writes to `training_event` every 60s checkpoint for 24 hours
- WHEN 1440 small files accumulate in a single day partition
- THEN the compaction job merges them into fewer, larger Parquet files

---

### Requirement: DuckDB Read Path

The system MUST support DuckDB as a zero-operational-overhead read engine for Iceberg tables.

- DuckDB MUST read Iceberg tables via the `iceberg_scan()` function
- DuckDB reads from the same warehouse path as the Hadoop catalog
- DuckDB is used for local development, CI testing, and ad-hoc analytical queries
- DuckDB MUST NOT write to Iceberg tables (read-only)

#### Scenario: Local ad-hoc query

- GIVEN the Iceberg warehouse is at `./warehouse`
- WHEN a developer runs `SELECT * FROM iceberg_scan('./warehouse/training_event') WHERE athlete_id='athlete-1'`
- THEN DuckDB reads the Parquet files directly with partition pruning

#### Scenario: CI analytical test

- GIVEN a CI pipeline needs to validate metric computations
- WHEN it queries Iceberg tables via DuckDB
- THEN results match the PostgreSQL serving layer for the same athlete and date range
