# AthleteOS

Real-time athlete data platform built on an event-staged architecture
(`raw -> canonical -> stream processing -> serving -> analytical -> API/UI`).

Stack: Python 3.12+, Apache Kafka, Confluent Schema Registry, Apache Flink
(PyFlink), Apache Iceberg (Parquet), PostgreSQL, DuckDB, FastAPI, Streamlit,
Docker Compose.

## Repository layout

```
jobs/          PyFlink stream processing jobs (canonicalize, metrics) - PR3/PR4
ingestion/     Source connectors -> raw.* topics - PR2+
schemas/       Canonical Avro schemas (.avsc) governed by Schema Registry
bootstrap/     One-shot schema registration + topic creation
storage/       PostgreSQL DDL + Iceberg table definitions - PR5
api/          FastAPI serving - PR6
ui/           Streamlit dashboards - PR6
tests/unit/        pytest unit tests (no Docker needed)
tests/integration/ pytest integration tests (require a running Docker daemon)
```

## Local runtime (Docker Compose profiles)

Profiles mitigate service-count friction:

- `core` - kafka, schema-registry, flink-jobmanager, flink-taskmanager, postgres
- `bootstrap` - one-shot schema registration + topic creation
- `ingest` - ingestion file watchers (PR2)
- `serve` - FastAPI + Streamlit (PR6)

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

## SDD context

This repository is being built under the SDD workflow. Change artifacts for the
foundation slice live under `openspec/changes/athleteos-foundation/`. The
event-contracts, architecture, serving-store, and analytical-store specs are
the source of truth; nothing in code may contradict them without an explicit
ADR.