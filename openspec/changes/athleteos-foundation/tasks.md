# Tasks: AthleteOS Foundation — MVP Vertical Slice (Strength)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~3100 |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | 6 PRs (see work units) |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | PR | Est. Lines | Effort |
|------|------|----|------------|--------|
| 1 | Scaffolding + Compose + harness + schemas + bootstrap | PR 1 | ~600 | M |
| 2 | Strength ingestion connector | PR 2 | ~300 | M |
| 3 | Canonicalize job + DLQ | PR 3 | ~500 | L |
| 4 | Metrics job (event-time windows) | PR 4 | ~600 | L |
| 5 | Sinks (PG + Iceberg) + DuckDB | PR 5 | ~450 | M |
| 6 | API + UI + synthetic data + observability | PR 6 | ~650 | M |

## Phase 1: Foundation (deps: none, M ~4d)

- [x] 1.1 Create `pyproject.toml` (pyflink, confluent-kafka, fastavro, psycopg2, duckdb, fastapi, streamlit, pytest, testcontainers), `pytest.ini`, `.gitignore`, directory tree (`jobs/`, `ingestion/`, `schemas/`, `bootstrap/`, `storage/`, `api/`, `ui/`, `tests/`)
- [x] 1.2 Create `docker-compose.yml`: kafka, schema-registry, flink-jobmanager, flink-taskmanager, postgres, ingestion, fastapi, streamlit, schema-bootstrap; profiles (core/serve/ingest/bootstrap); healthchecks
- [x] 1.3 Create `tests/conftest.py` with testcontainers fixtures (Kafka, Schema Registry, PG); `tests/unit/` + `tests/integration/`; verify `pytest --collect-only` passes

## Phase 2: Schema & Topic Bootstrap (deps: 1.x, S ~2d)

- [x] 2.1 Create `schemas/canonical/{TrainingEvent,WellnessEvent,PlanningBlock}.avsc` per spec schemas
- [x] 2.2 Create `bootstrap/register_schemas.py` (register .avsc, set BACKWARD via Registry API) + `bootstrap/create_topics.py` (12 topics per design/spec, 8 partitions, athlete_id key, retention configs; tasks.md "15" is superseded by the authoritative 6+3+3=12 topology from design + event-contracts spec)
- [x] 2.3 Integration test: schemas register successfully, BACKWARD enforced (reject incompatible), topics created with correct partition count and retention

## Phase 3: Strength Ingestion (deps: 1.x+2.x, M ~4d)

- [x] 3.1 Create `ingestion/strength/{parser,watcher,producer}.py`: Strong CSV → raw envelope JSON → `raw.strength` (KafkaProducer, athlete_id key)
- [x] 3.2 Unit tests: CSV parsing (valid/malformed), envelope generation (UUID, ISO time, source=strong_csv). NOTE: session_load formula (reps×weight_kg×(rpe/10), fallback reps×weight_kg) is intentionally NOT tested here — per event-contracts spec "session_load derivation (computed at canonicalization)" it belongs to the PR3 canonicalize job, not the raw ingestion connector. The raw connector emits the verbatim payload; session_load is derived in PR3. tasks.md 3.2 wording contradicts the spec; spec is source of truth.
- [x] 3.3 Integration test: CSV file drop → events in `raw.strength` with correct JSON shape (testcontainers Kafka)

## Phase 4: Canonicalize Job (deps: 2.x, L ~6d)

- [ ] 4.1 Create `jobs/canonicalize/main.py`: KafkaSource(raw.strength, JSON) → key_by(athlete_id) → transform → KafkaSink(canonical.training_event, ConfluentRegistryAvroSerializationSchema); WatermarkStrategy 24h bounded out-of-orderness; dedup ValueState<bool> per event_id, 7d TTL
- [ ] 4.2 DLQ side output: OutputTag("dlq") for VALIDATION_FAILURE + TRANSFORM_ERROR → JSON KafkaSink(AT_LEAST_ONCE) to `dlq.canonical.training_event`; error envelope per spec (original_value base64, error_type, stack)
- [ ] 4.3 Integration test (HIGHEST RISK): raw JSON → canonical Avro roundtrip via Schema Registry + DLQ routing on invalid input + dedup drops duplicate event_ids (testcontainers Kafka + Schema Registry)

## Phase 5: Metrics Job (deps: 4.x, L ~8d)

- [ ] 5.1 Create `jobs/metrics/main.py`: KafkaSource(canonical.training_event, ConfluentRegistryAvroDeserializationSchema); EmbeddedRocksDBStateBackend; checkpointing 60s EXACTLY_ONCE (EXTERNALIZED, RETAIN_ON_CANCELLATION); dedup ValueState 7d TTL
- [ ] 5.2 Daily pre-agg: TumblingEventTimeWindows(1d) summing session_load + ContinuousEventTimeTrigger (emit-on-update, ADR-13); rolling: SlidingEventTimeWindows(7d/28d/42d, slide 1d) + allowed_lateness(24h) + side_output_late_data(OutputTag("late.metrics"))
- [ ] 5.3 Deload flag: KeyedProcessFunction over daily ACR stream; ValueState<(count,sign)>; +1 if ACR>1.3 ≥3 consecutive days, -1 if ACR<0.8 ≥3 days, else 0
- [ ] 5.4 DLQ for DESERIALIZATION_ERROR + NaN guards; late side output captured to `dlq.canonical.training_event`
- [ ] 5.5 Integration test (HIGHEST RISK): canonical events → acute_load/chronic_load/ACR/deload_flag correct + late data routed to side output + dedup verified (testcontainers Kafka + Registry)

## Phase 6: Sinks & DuckDB (deps: 5.x, M ~5d)

- [ ] 6.1 `storage/postgres/ddl.sql` (athlete_metrics per spec DDL + indexes); JDBC UPSERT sink in metrics job (INSERT...ON CONFLICT DO UPDATE, batched per checkpoint, HikariCP, exponential-backoff retry → DLQ)
- [ ] 6.2 `storage/iceberg/tables.py` (create training_event table, partitioned (athlete_id, days(event_time)), Parquet, Hadoop catalog ./warehouse); Flink Iceberg sink (append, checkpoint-commit)
- [ ] 6.3 DuckDB `iceberg_scan('./warehouse/training_event')` helper + PG↔Iceberg parity check script
- [ ] 6.4 Integration test: Flink → PG UPSERT correct row + Iceberg files written + DuckDB iceberg_scan() returns matching data (testcontainers PG)

## Phase 7: API & UI (deps: 6.x, S ~3d)

- [ ] 7.1 `api/main.py`: FastAPI `GET /athletes/{id}/metrics?date=YYYY-MM-DD` querying PG + health endpoint + Dockerfile
- [ ] 7.2 `ui/dashboard.py`: Streamlit acute_load/chronic_load/ACR/deload_flag view + DLQ depth panel + Dockerfile
- [ ] 7.3 Tests: API endpoint returns correct metrics (mock PG) + Streamlit smoke test

## Phase 8: Synthetic Data & Demo (deps: 3.x, S ~2d)

- [ ] 8.1 `data/synthetic/generator.py`: parameterized (N athletes, M weeks) Strong CSV generator with realistic patterns
- [ ] 8.2 Sample sanitized Strong CSV in `data/samples/`; `make demo` E2E script (compose up → bootstrap → drop CSV → wait → query API + DuckDB)

## Phase 9: Observability & Data Quality (deps: 5.x, S ~2d)

- [ ] 9.1 Prometheus metrics via Flink reporter: checkpoint duration, records processed, DLQ depth, late event count
- [ ] 9.2 Data quality checks: schema validation rate, null-rate on MUST fields, partition lag monitoring
- [ ] 9.3 Streamlit DLQ dashboard: topic depth, error type breakdown, recent event viewer
