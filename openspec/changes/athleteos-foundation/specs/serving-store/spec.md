# Serving Store Specification

## Purpose

Defines the PostgreSQL operational schema for serving the latest computed athlete metrics. Optimized for point lookups by FastAPI and Streamlit. Written by Flink sinks via UPSERT semantics. This is the operational read path — NOT the analytical/historical path (see analytical-store spec).

## Requirements

### Requirement: Athlete Metrics Table

The system MUST maintain an `athlete_metrics` table storing the latest computed metrics per athlete per day.

**Table DDL:**

```sql
CREATE TABLE athlete_metrics (
    athlete_id      TEXT        NOT NULL,
    metric_date     DATE        NOT NULL,
    -- MUST metrics (MVP)
    acute_load      FLOAT,
    chronic_load_28d FLOAT,
    chronic_load_42d FLOAT,
    acute_chronic_ratio FLOAT,
    deload_flag     SMALLINT    DEFAULT 0,
    -- SHOULD metrics (post-MVP)
    readiness_score FLOAT,
    fatigue_score   FLOAT,
    adherence_score FLOAT,
    -- Metadata
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    compute_version TEXT,
    CONSTRAINT pk_athlete_metrics PRIMARY KEY (athlete_id, metric_date)
);
```

**UPSERT key**: `(athlete_id, metric_date)` — Flink sink writes are idempotent. Reprocessing the same day's data overwrites the existing row.

**Column semantics:**

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `athlete_id` | TEXT | NO | Partition/lookup key |
| `metric_date` | DATE | NO | Day the metrics represent |
| `acute_load` | FLOAT | YES | 7-day rolling sum of daily session_load |
| `chronic_load_28d` | FLOAT | YES | 28-day rolling average of daily session_load |
| `chronic_load_42d` | FLOAT | YES | 42-day rolling average of daily session_load |
| `acute_chronic_ratio` | FLOAT | YES | acute_load / chronic_load_28d (null if chronic=0) |
| `deload_flag` | SMALLINT | NO | +1 overreaching risk, -1 undertraining, 0 normal |
| `readiness_score` | FLOAT | YES | 0-100 composite (SHOULD metric) |
| `fatigue_score` | FLOAT | YES | 0-100 composite (SHOULD metric) |
| `adherence_score` | FLOAT | YES | 0.0-1.0 ratio (SHOULD metric) |
| `updated_at` | TIMESTAMPTZ | NO | Last Flink sink write timestamp |
| `compute_version` | TEXT | YES | Flink job version for traceability |

#### Scenario: Idempotent UPSERT from Flink

- GIVEN Flink computes `acute_load=450.0` for athlete-1 on 2026-06-20
- WHEN the sink writes to PostgreSQL
- THEN `INSERT ... ON CONFLICT (athlete_id, metric_date) DO UPDATE` produces exactly one row

#### Scenario: Null SHOULD metrics in MVP

- GIVEN only MUST metrics are computed (no readiness/fatigue/adherence yet)
- WHEN the row is inserted
- THEN `readiness_score`, `fatigue_score`, `adherence_score` are NULL

---

### Requirement: Metric Formulas

The system MUST compute metrics using explicit, interpretable formulas. No black-box models or pseudoscience.

**Daily session_load aggregation** (input to all rolling metrics):

For each athlete and each day, sum all `session_load` values from `canonical.training_event`:

```
daily_load(d) = Σ session_load(e) for all training events e on day d
```

**MUST Metrics:**

| Metric | Formula | Window | Notes |
|--------|---------|--------|-------|
| `acute_load` | `Σ daily_load(d) for d in [t-6, t]` | 7-day rolling sum | Total recent training load |
| `chronic_load_28d` | `Σ daily_load(d) for d in [t-27, t] / 28` | 28-day rolling average | Baseline fitness proxy |
| `chronic_load_42d` | `Σ daily_load(d) for d in [t-41, t] / 42` | 42-day rolling average | Longer-term baseline |
| `acute_chronic_ratio` | `acute_load(t) / chronic_load_28d(t)` | Point-in-time | NULL if chronic_load_28d = 0 |
| `deload_flag` | Rule-based (see below) | 3-day consecutive | +1 / 0 / -1 |

**deload_flag rules:**

```
IF ACR(t) > 1.3 for ≥ 3 consecutive days → deload_flag = +1 (overreaching risk)
IF ACR(t) < 0.8 for ≥ 3 consecutive days → deload_flag = -1 (undertraining signal)
OTHERWISE → deload_flag = 0
```

**SHOULD Metrics (post-MVP):**

| Metric | Formula | Inputs | Range |
|--------|---------|--------|-------|
| `readiness_score` | `0.4 × hrv_z_norm + 0.3 × sleep_ratio + 0.3 × perceived_recovery_norm` | HRV z-score (14d baseline), sleep_hours/target, perceived_recovery (1-5) | 0-100 |
| `fatigue_score` | `0.35 × acute_load_norm + 0.25 × soreness_norm + 0.20 × stress_norm + 0.20 × (1 - sleep_ratio)` | acute_load (min-max over 28d), soreness (1-5), stress (1-5), sleep | 0-100 |
| `adherence_score` | `0.5 × min(actual_sessions/planned_sessions, 1.0) + 0.5 × min(actual_volume/target_volume, 1.0)` | Planning block + actual training events | 0.0-1.0 |

**Normalization notes:**
- `hrv_z_norm`: z-score of today's HRV relative to 14-day rolling baseline, clamped to [0, 1] via sigmoid
- `sleep_ratio`: `min(sleep_hours / target_sleep_hours, 1.0)` where target = 8h default
- `perceived_recovery_norm`: `(perceived_recovery - 1) / 4` (maps 1-5 to 0-1)
- `acute_load_norm`: min-max normalization over 28-day window
- `soreness_norm`: `(soreness - 1) / 4` (maps 1-5 to 0-1)
- `stress_norm`: `(stress - 1) / 4` (maps 1-5 to 0-1)
- Missing inputs: re-normalize weights proportionally among available inputs

#### Scenario: ACR computation

- GIVEN athlete-1 has `acute_load=700` and `chronic_load_28d=500`
- WHEN metrics are computed
- THEN `acute_chronic_ratio = 1.4`

#### Scenario: Deload flag triggered

- GIVEN athlete-1 has ACR > 1.3 for 3 consecutive days (days 18, 19, 20)
- WHEN metrics for day 20 are computed
- THEN `deload_flag = +1`

#### Scenario: Readiness with missing HRV

- GIVEN an athlete has sleep and perceived_recovery but no HRV data
- WHEN readiness_score is computed
- THEN weights re-normalize: `0.5 × sleep_ratio + 0.5 × perceived_recovery_norm` (HRV weight redistributed)

---

### Requirement: Indexing for Point Lookups

The system MUST optimize for the primary access pattern: point lookups by `(athlete_id, metric_date)`.

| Index | Columns | Type | Purpose |
|-------|---------|------|---------|
| `pk_athlete_metrics` | `(athlete_id, metric_date)` | PRIMARY KEY (B-tree) | Point lookup, UPSERT conflict resolution |
| `idx_athlete_metrics_athlete` | `(athlete_id)` | B-tree | Range scans for single athlete history |

Additional indexes SHOULD be added if query patterns emerge (e.g., date-range scans across athletes).

#### Scenario: FastAPI point lookup

- GIVEN a request `GET /athletes/athlete-1/metrics?date=2026-06-20`
- WHEN FastAPI queries `SELECT * FROM athlete_metrics WHERE athlete_id='athlete-1' AND metric_date='2026-06-20'`
- THEN PostgreSQL uses the primary key index — O(log n) lookup

---

### Requirement: Flink Sink Write Pattern

The Flink PostgreSQL sink MUST use micro-batch writes aligned with checkpoint boundaries, NOT per-event writes.

- Writes MUST be batched per checkpoint interval (configurable, default 60s)
- Each batch MUST use `INSERT ... ON CONFLICT (athlete_id, metric_date) DO UPDATE SET ...`
- Connection pooling MUST be used (e.g., HikariCP or pgBouncer)
- Write failures MUST be retried with exponential backoff before routing to DLQ

#### Scenario: Checkpoint-aligned batch write

- GIVEN Flink checkpoint interval is 60 seconds
- WHEN 50 metric updates are computed within one checkpoint
- THEN all 50 are written in a single batched UPSERT at checkpoint completion
