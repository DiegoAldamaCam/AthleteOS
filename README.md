# AthleteOS

Real-time athlete data platform built on an event-staged architecture
(`raw -> canonical -> stream processing -> serving -> analytical -> API/UI`).

Stack: Python 3.11, Apache Kafka, Confluent Schema Registry, Apache Flink
(PyFlink), Apache Iceberg (Parquet), PostgreSQL, DuckDB, FastAPI, React (Vite +
Nginx), Docker Compose.

## Repository layout

```
jobs/          PyFlink stream processing jobs (canonicalize, metrics) - PR3/PR4
ingestion/     Source connectors -> raw.* topics - PR2+
schemas/       Canonical Avro schemas (.avsc) governed by Schema Registry
bootstrap/     One-shot schema registration + topic creation
storage/       PostgreSQL DDL + Iceberg table definitions - PR5
api/          FastAPI serving - PR6
web/          React SPA (Vite build, served via Nginx on port 80) - PR6
tests/unit/        pytest unit tests (no Docker needed)
tests/integration/ pytest integration tests (require a running Docker daemon)
```

## Local runtime (Docker Compose profiles)

Profiles mitigate service-count friction:

- `core` - kafka, schema-registry, flink-jobmanager, flink-taskmanager, postgres
- `bootstrap` - one-shot schema registration + topic creation
- `ingest` - ingestion file watchers (PR2)
- `serve` - FastAPI + React SPA via Nginx (PR6)

```bash
# Start the processing core + bootstrap the topics/schemas once.
docker compose --profile core --profile bootstrap up -d

# Just (re)run the one-shot bootstrap against an already-running core.
docker compose --profile bootstrap run --rm schema-bootstrap
```

Bootstrap registers the three canonical Avro schemas with `BACKWARD`
compatibility (TopicNameStrategy subjects, `canonical.<event>-value`) and
creates the 12-topic Kafka topology (6 raw + 3 canonical + 3 DLQ), each with
exactly 8 partitions and the retention/compaction configs from the
event-contracts spec.

## Test harness

```bash
pip install -e ".[dev]"          # pytest, testcontainers[kafka], requests
pytest                           # unit tests run; integration tests need Docker
pytest --collect-only            # verify the harness is wired
pytest -m "not integration"     # skip Docker-gated integration tests
```

Integration tests use [testcontainers](https://testcontainers-python.readthedocs.io/)
and a Redpanda container that serves both Kafka and a Schema Registry. They are
**skipped automatically when the Docker daemon is unreachable**. To run them:

```bash
# Ensure Docker Desktop / the docker daemon is running, then:
pytest -m integration
```

## Launch with data (zero to populated UI)

End-to-end sequence after a clean clone. Requires Docker Compose and Python 3.11.

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD, API_KEY, JWT_SECRET, GF_SECURITY_ADMIN_PASSWORD

cp web/.env.example web/.env
# Edit web/.env — set VITE_API_KEY to the SAME value as API_KEY in root .env
# (Vite bakes this into the bundle; rebuild the web image after any change)
```

### 2. Start core infrastructure

```bash
docker compose --profile core up -d
# Postgres DDL is applied automatically on first start (docker-entrypoint-initdb.d).
# Wait for all services to be healthy before proceeding.
```

### 3. Register schemas and create Kafka topics (one-shot)

```bash
docker compose --profile bootstrap run --rm schema-bootstrap
```

### 4. Seed a login user

```bash
# Requires psycopg2-binary: pip install psycopg2-binary  (or pip install -e ".[dev]")
DATABASE_URL=postgresql://athleteos:<POSTGRES_PASSWORD>@localhost:5432/athleteos \
    python -m tools.seed_user --username admin --password <your-password>
```

### 5. Start ingestion and drop sample data

```bash
docker compose --profile ingest up -d
# Sample CSVs are already in data/inbox/*/sample.csv — the watchers will pick them up.
# Add more files to data/inbox/<connector>/ at any time.
```

### 6. ⚠️ Submit Flink jobs (manual step — G4, not yet automated)

The raw-to-canonical and canonical-to-metrics Flink jobs must be submitted manually.
Until they run, the `athlete_metrics` and `planning_blocks` tables will be empty
and the UI will show no metric data.

```bash
# Submit jobs via the Flink REST API or Flink dashboard at http://localhost:8082
# Example (adjust JAR path for your setup):
docker exec flink-jobmanager flink run -py /opt/flink/jobs/strength_canonicalize.py
# Repeat for each job: wellness, cardio, recovery, nutrition, planning, metrics
```

> **Tracking**: Flink job submission automation is tracked separately as G4.

### 7. Start the API and React SPA

```bash
docker compose --profile serve up -d
# React SPA: http://localhost:80
# FastAPI:   http://localhost:8000/docs
```

### What works after these steps

| Layer | Status |
|-------|--------|
| Kafka topics + Avro schemas | ✅ registered by bootstrap |
| Postgres tables | ✅ created automatically on first postgres start |
| Login user | ✅ seeded via tools/seed_user.py |
| CSV ingestion → raw Kafka topics | ✅ sample.csv files trigger the watchers |
| Flink canonicalize + metrics jobs | ⚠️ **manual** — G4 not yet wired |
| athlete_metrics populated | ⚠️ requires Flink jobs to run first |
| React SPA + FastAPI | ✅ serve profile |

## SDD context

This repository is being built under the SDD workflow. Change artifacts for the
foundation slice live under `openspec/changes/athleteos-foundation/`. The
event-contracts, architecture, serving-store, and analytical-store specs are
the source of truth; nothing in code may contradict them without an explicit
ADR.