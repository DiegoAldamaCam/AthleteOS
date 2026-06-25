# Event Contracts Specification

## Purpose

Defines the canonical event model, Kafka topic topology, serialization contracts, schema evolution governance, and dead-letter routing for AthleteOS. This is the **source of truth** — no Flink job, API endpoint, or store schema may contradict these contracts without an explicit ADR.

## Requirements

### Requirement: Common Event Envelope

Every event produced into canonical topics MUST carry a common envelope with these fields:

| Field | Type | Required | Purpose |
|-------|------|----------|---------|
| `event_id` | string (UUID v4) | YES | Idempotency key; dedup at Flink and sinks |
| `event_time` | long (epoch-ms) | YES | Event-time semantics; real-world occurrence time |
| `ingest_time` | long (epoch-ms) | YES | Wall-clock time at ingestion; latency monitoring |
| `source` | string | YES | Origin identifier (e.g., `strong_csv`, `strava`, `apple_health`) |
| `schema_version` | int | YES | Schema Registry version; evolution tracking |
| `athlete_id` | string | YES | Partition key; natural key for all per-athlete processing |

#### Scenario: Idempotent event production

- GIVEN a producer emits two events with the same `event_id`
- WHEN Flink consumes both events
- THEN only one event is processed (dedup on `event_id`)

#### Scenario: Event-time ordering

- GIVEN events arrive out of order (batch CSV upload after real-time API)
- WHEN Flink processes with event-time windows and watermarks
- THEN windows are computed based on `event_time`, not `ingest_time`

---

### Requirement: Canonical Training Event Schema

The system MUST define a `TrainingEvent` Avro record covering both strength sets and cardio activities on `canonical.training_event`.

**Avro Schema** (`TrainingEvent.avsc`):

```json
{
  "type": "record",
  "name": "TrainingEvent",
  "namespace": "com.athleteos.canonical",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingest_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "source", "type": "string"},
    {"name": "schema_version", "type": "int"},
    {"name": "athlete_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "workout_id", "type": ["null", "string"], "default": null},
    {"name": "exercise_id", "type": ["null", "string"], "default": null},
    {"name": "set_number", "type": ["null", "int"], "default": null},
    {"name": "reps", "type": ["null", "int"], "default": null},
    {"name": "weight_kg", "type": ["null", "float"], "default": null},
    {"name": "rpe", "type": ["null", "float"], "default": null},
    {"name": "rir", "type": ["null", "float"], "default": null},
    {"name": "activity_type", "type": ["null", "string"], "default": null},
    {"name": "distance_km", "type": ["null", "float"], "default": null},
    {"name": "duration_sec", "type": ["null", "int"], "default": null},
    {"name": "avg_hr", "type": ["null", "int"], "default": null},
    {"name": "tss", "type": ["null", "float"], "default": null},
    {"name": "session_load", "type": "float"}
  ]
}
```

**`event_type` representation (ADR-15):** `event_type` is an Avro **`string`** (not an enum) on the wire. The semantic guarantee of the former enum is preserved at the application layer: the allowed values are exactly `{"STRENGTH_SET", "CARDIO_ACTIVITY"}`, enforced in the canonicalize transform's `validate_training_event()`. An out-of-set `event_type` raises a validation error and is routed to the DLQ as `VALIDATION_FAILURE`. Rationale: the Flink 1.19 `avro-confluent` Table sink derives the Avro writer schema from Table column types and has no Avro enum type, so it emits `string`; relaxing the contract to `string` makes the design contract and the runtime wire format converge, while the transform-layer symbol-set guard keeps the enum's semantic guarantee (consumers no longer get registry-level enum enforcement, but get application-level validation + DLQ routing instead).

**Source Field Mappings:**

| Raw Source | Raw Field | Canonical Field | Transform |
|------------|-----------|-----------------|-----------|
| Strong CSV | `athlete_id` | `athlete_id` | direct |
| Strong CSV | `workout_id` | `workout_id` | direct |
| Strong CSV | `exercise_id` | `exercise_id` | direct |
| Strong CSV | `set_number` | `set_number` | direct |
| Strong CSV | `reps` | `reps` | direct |
| Strong CSV | `weight_kg` | `weight_kg` | direct |
| Strong CSV | `rpe` | `rpe` | direct (nullable) |
| Strong CSV | `rir` | `rir` | direct (nullable) |
| Strong CSV | `timestamp` | `event_time` | parse ISO→epoch-ms |
| Strava | `athlete_id` | `athlete_id` | direct |
| Strava | `activity_type` | `activity_type` | direct |
| Strava | `distance_km` | `distance_km` | direct |
| Strava | `duration_sec` | `duration_sec` | direct |
| Strava | `avg_hr` | `avg_hr` | direct (nullable) |
| Strava | `training_stress_score` | `tss` | direct (nullable) |
| Strava | `timestamp` | `event_time` | parse ISO→epoch-ms |

**session_load derivation** (computed at canonicalization, required field):

| event_type | Formula | Assumptions |
|------------|---------|-------------|
| `STRENGTH_SET` | `reps × weight_kg × (rpe / 10.0)` when rpe present; `reps × weight_kg` when rpe absent | Volume-load proxy; RPE weighting optional |
| `CARDIO_ACTIVITY` | `tss` when present; fallback: `duration_sec × (avg_hr / 190) × 0.01` | TSS from Strava preferred; HR-based proxy uses 190 as reference max HR |

#### Scenario: Strength CSV canonicalization

- GIVEN a Strong CSV row with `reps=8, weight_kg=100, rpe=8.5`
- WHEN the raw→canonical transform processes it
- THEN `session_load = 8 × 100 × (8.5/10) = 680.0`, `event_type = STRENGTH_SET`

#### Scenario: Cardio with TSS

- GIVEN a Strava activity with `training_stress_score=75.3`
- WHEN canonicalized
- THEN `session_load = 75.3`, `event_type = CARDIO_ACTIVITY`

#### Scenario: Cardio without TSS (HR fallback)

- GIVEN a Strava activity with `duration_sec=3600, avg_hr=155, tss=null`
- WHEN canonicalized
- THEN `session_load = 3600 × (155/190) × 0.01 = 29.37`

---

### Requirement: Canonical Wellness Event Schema

The system MUST define a `WellnessEvent` Avro record covering recovery, nutrition, and subjective wellness on `canonical.wellness_event`.

**Avro Schema** (`WellnessEvent.avsc`):

```json
{
  "type": "record",
  "name": "WellnessEvent",
  "namespace": "com.athleteos.canonical",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingest_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "source", "type": "string"},
    {"name": "schema_version", "type": "int"},
    {"name": "athlete_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "name": "WellnessEventType", "symbols": ["RECOVERY_SNAPSHOT", "NUTRITION_DAILY", "WELLNESS_DAILY"]}},
    {"name": "sleep_hours", "type": ["null", "float"], "default": null},
    {"name": "resting_hr", "type": ["null", "int"], "default": null},
    {"name": "hrv", "type": ["null", "float"], "default": null},
    {"name": "steps", "type": ["null", "int"], "default": null},
    {"name": "body_weight_kg", "type": ["null", "float"], "default": null},
    {"name": "calories", "type": ["null", "int"], "default": null},
    {"name": "protein_g", "type": ["null", "float"], "default": null},
    {"name": "carbs_g", "type": ["null", "float"], "default": null},
    {"name": "fat_g", "type": ["null", "float"], "default": null},
    {"name": "nutrition_adherence", "type": ["null", "float"], "default": null},
    {"name": "energy", "type": ["null", "int"], "default": null},
    {"name": "soreness", "type": ["null", "int"], "default": null},
    {"name": "mood", "type": ["null", "int"], "default": null},
    {"name": "stress", "type": ["null", "int"], "default": null},
    {"name": "perceived_recovery", "type": ["null", "int"], "default": null}
  ]
}
```

**WellnessEvent `event_type` (consistency note):** `WellnessEvent.avsc` still declares `event_type` as an Avro enum today, but the same enum→string + application-layer symbol-set validation pattern (ADR-15) will apply to wellness when the wellness canonicalize branch is implemented, so the canonical contract stays consistent across entities. This PR (PR3) does NOT change `WellnessEvent` — it is out of scope and not yet implemented.

**Source Field Mappings:**

| Raw Source | Raw Field | Canonical Field | Notes |
|------------|-----------|-----------------|-------|
| Apple Health | `date` | `event_time` | start-of-day epoch-ms |
| Apple Health | `sleep_hours` | `sleep_hours` | direct |
| Apple Health | `resting_hr` | `resting_hr` | direct |
| Apple Health | `hrv` | `hrv` | nullable |
| Apple Health | `steps` | `steps` | direct |
| Apple Health | `body_weight_kg` | `body_weight_kg` | nullable |
| Nutrition CSV | `date` | `event_time` | start-of-day |
| Nutrition CSV | `calories` | `calories` | direct |
| Nutrition CSV | `protein_g` | `protein_g` | direct |
| Nutrition CSV | `carbs_g` | `carbs_g` | direct |
| Nutrition CSV | `fat_g` | `fat_g` | direct |
| Nutrition CSV | `adherence_score` | `nutrition_adherence` | renamed for clarity |
| Wellness | `date` | `event_time` | start-of-day |
| Wellness | `energy` | `energy` | 1-5 scale |
| Wellness | `soreness` | `soreness` | 1-5 scale |
| Wellness | `mood` | `mood` | 1-5 scale |
| Wellness | `stress` | `stress` | 1-5 scale |
| Wellness | `perceived_recovery` | `perceived_recovery` | 1-5 scale |

#### Scenario: Recovery event from Apple Health

- GIVEN an Apple Health export with `sleep_hours=7.5, resting_hr=58, hrv=42`
- WHEN canonicalized
- THEN `event_type=RECOVERY_SNAPSHOT`, recovery fields populated, nutrition/wellness fields null

---

### Requirement: Canonical Planning Block Schema

The system MUST define a `PlanningBlock` Avro record on `canonical.planning_block`.

**Avro Schema** (`PlanningBlock.avsc`):

```json
{
  "type": "record",
  "name": "PlanningBlock",
  "namespace": "com.athleteos.canonical",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ingest_time", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "source", "type": "string"},
    {"name": "schema_version", "type": "int"},
    {"name": "athlete_id", "type": "string"},
    {"name": "block_id", "type": "string"},
    {"name": "goal", "type": "string"},
    {"name": "start_date", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "end_date", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "planned_sessions_per_week", "type": "int"},
    {"name": "weekly_volume_targets", "type": "string"}
  ]
}
```

`weekly_volume_targets` is a JSON-encoded string (e.g., `{"strength": 5000, "cardio_tss": 300}`) to allow flexible per-sport targets without requiring Avro map with fixed value types.

#### Scenario: Planning block ingestion

- GIVEN a YAML planning file with a 6-week hypertrophy block
- WHEN canonicalized
- THEN `PlanningBlock` emitted with `goal="hypertrophy"`, dates as epoch-ms, targets as JSON string

---

### Requirement: Kafka Topic Topology

The system MUST maintain a two-tier topic topology with raw and canonical layers.

**Raw Topics (JSON serialization):**

| Topic | Source | Key | Retention | Purpose |
|-------|--------|-----|-----------|---------|
| `raw.strength` | Strong CSV | `athlete_id` | 7 days (time-bound) | Source-faithful replay |
| `raw.cardio` | Strava API | `athlete_id` | 7 days | Source-faithful replay |
| `raw.recovery` | Apple Health | `athlete_id` | 14 days | Source-faithful replay |
| `raw.nutrition` | Nutrition CSV | `athlete_id` | 7 days | Source-faithful replay |
| `raw.wellness` | Manual/synthetic | `athlete_id` | 7 days | Source-faithful replay |
| `raw.planning` | YAML/JSON/CSV | `athlete_id` | 14 days | Source-faithful replay |

**Canonical Topics (Avro + Schema Registry):**

| Topic | Event Types | Key | Retention | Partitions |
|-------|-------------|-----|-----------|------------|
| `canonical.training_event` | STRENGTH_SET, CARDIO_ACTIVITY | `athlete_id` | compacted + 30d | 8 |
| `canonical.wellness_event` | RECOVERY_SNAPSHOT, NUTRITION_DAILY, WELLNESS_DAILY | `athlete_id` | compacted + 30d | 8 |
| `canonical.planning_block` | Planning blocks | `athlete_id` | compacted + 90d | 8 |

**Partitioning rules:**
- ALL topics MUST use `athlete_id` as partition key
- ALL canonical topics MUST have exactly 8 partitions (LOCKED ADR-4)
- ALL raw topics MUST have exactly 8 partitions (co-partitioning requirement)
- Co-partitioning enables Flink stream-stream joins across canonical topics without reshuffling

#### Scenario: Co-partitioned join

- GIVEN `canonical.training_event` and `canonical.wellness_event` both have 8 partitions keyed by `athlete_id`
- WHEN Flink joins training load with recovery data for the same athlete
- THEN both events land in the same partition index — no network shuffle required

---

### Requirement: Raw Topic JSON Shape

Raw topics MUST use JSON serialization with a documented shape (not Avro). Each raw topic's JSON structure MUST faithfully represent the source format with minimal transformation.

**Common raw envelope (all raw topics):**

```json
{
  "event_id": "uuid-v4",
  "event_time": "ISO-8601-string",
  "ingest_time": "ISO-8601-string",
  "source": "source-identifier",
  "athlete_id": "athlete-id-string",
  "payload": { "...source-specific fields..." }
}
```

The `payload` object contains the raw source fields as-is (parsed from CSV/XML/API but not normalized). This preserves source fidelity for replay and auditing.

#### Scenario: Raw strength event

- GIVEN a Strong CSV row is parsed
- WHEN published to `raw.strength`
- THEN JSON contains `payload: { workout_id, exercise_id, set_number, reps, weight_kg, rpe, rir, timestamp }` verbatim from CSV

---

### Requirement: Schema Evolution Governance

The system MUST enforce schema evolution via Confluent Schema Registry with **BACKWARD** compatibility as the default mode on all canonical topics.

| Compatibility | Meaning | When Used |
|---------------|---------|-----------|
| **BACKWARD** (default) | New schema can read data from previous schema | All canonical topics — consumers upgrade before producers |
| FORWARD | Previous schema can read data from new schema | Not used in MVP |
| FULL | Both BACKWARD and FORWARD | Reserved for future multi-consumer scenarios |

**Evolution rules:**

| Change | BACKWARD Compatible? | Action |
|--------|---------------------|--------|
| Add optional field with default | YES | Register new schema version |
| Add required field | NO | Must use optional + default |
| Remove field | NO | Deprecate via documentation, keep field |
| Change field type | NO | Create new field, deprecate old |
| Rename field | NO | Add new field, deprecate old |

**Version bumping**: Schema Registry auto-increments version on successful registration. Producers MUST NOT hardcode version numbers.

#### Scenario: Adding a new optional field

- GIVEN `TrainingEvent` schema v1 is registered
- WHEN a producer registers v2 with a new optional field `{"name": "velocity_mps", "type": ["null", "float"], "default": null}`
- THEN Schema Registry accepts v2 (BACKWARD compatible), existing consumers continue reading v1 data

#### Scenario: Incompatible schema change rejected

- GIVEN `TrainingEvent` schema v1 is registered
- WHEN a producer attempts to register v2 that removes the `reps` field
- THEN Schema Registry rejects the registration with a compatibility error

---

### Requirement: Dead Letter Queue Routing

The system MUST route invalid events to per-topic DLQ topics instead of dropping them.

**DLQ Topics:**

| DLQ Topic | Source Topic |
|-----------|-------------|
| `dlq.canonical.training_event` | `canonical.training_event` |
| `dlq.canonical.wellness_event` | `canonical.wellness_event` |
| `dlq.canonical.planning_block` | `canonical.planning_block` |

**DLQ Error Envelope (JSON):**

```json
{
  "original_topic": "canonical.training_event",
  "original_key": "athlete-123",
  "original_value": "base64-encoded-bytes",
  "error_type": "VALIDATION_FAILURE",
  "error_message": "Missing required field: session_load",
  "error_stack": "optional stack trace",
  "timestamp": 1719331200000
}
```

**Routing rules:**

| Error Type | Trigger | Action |
|------------|---------|--------|
| `VALIDATION_FAILURE` | Missing required field, out-of-range value | Route to DLQ |
| `SCHEMA_INCOMPATIBILITY` | Schema Registry rejects producer write | Route to DLQ |
| `DESERIALIZATION_ERROR` | Malformed Avro bytes on consumer side | Route to DLQ |
| `TRANSFORM_ERROR` | Raw→canonical mapping failure | Route to DLQ |

DLQ messages MUST use JSON serialization (not Avro) because the original event may have an unparseable schema.

#### Scenario: Validation failure routed to DLQ

- GIVEN a canonical training event with `session_load = NaN`
- WHEN the validation step detects the invalid value
- THEN the event is published to `dlq.canonical.training_event` with `error_type=VALIDATION_FAILURE`

#### Scenario: Schema Registry rejection

- GIVEN a producer attempts to write an event incompatible with the registered schema
- WHEN Schema Registry returns a compatibility error
- THEN the original raw bytes are published to the DLQ with `error_type=SCHEMA_INCOMPATIBILITY`

#### Scenario: DLQ monitoring

- GIVEN events accumulate in a DLQ topic
- WHEN the DLQ depth exceeds a threshold (configurable, default 10)
- THEN the system SHOULD surface an alert via the Streamlit DLQ dashboard
