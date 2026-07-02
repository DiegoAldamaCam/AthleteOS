#!/usr/bin/env bash
# submit.sh — one-shot Flink job submission for the strength pipeline (G4).
#
# Submits both streaming jobs sequentially to the remote Flink JobManager.
# flink run returns after the JobGraph is accepted by the cluster; the jobs
# continue running in the cluster indefinitely (the submit container exits 0).
#
# Environment variables (all provided by docker-compose.yml flink-job-submit):
#   FLINK_JM                 — host:port of the Flink REST/JobManager, e.g. flink-jobmanager:8082
#   KAFKA_BOOTSTRAP_SERVERS  — Kafka broker, e.g. kafka:9092
#   SCHEMA_REGISTRY_URL      — Confluent Schema Registry URL
#   METRICS_PG_DSN           — PostgreSQL DSN for athlete_metrics UPSERT sink
#   METRICS_CHECKPOINT_DIR   — checkpoint dir (file:///flink-checkpoints)
#
# -pyfs /opt/flink/usrlib adds the usrlib dir to PYTHONPATH on the cluster,
# covering jobs.*, storage.*, and schemas.* relative imports in all job modules.

set -euo pipefail

echo "[submit] Submitting jobs.canonicalize.main to ${FLINK_JM}..."
flink run \
    -m "${FLINK_JM}" \
    -pym jobs.canonicalize.main \
    -pyfs /opt/flink/usrlib

echo "[submit] Submitting jobs.metrics.main to ${FLINK_JM}..."
flink run \
    -m "${FLINK_JM}" \
    -pym jobs.metrics.main \
    -pyfs /opt/flink/usrlib

echo "[submit] Both jobs submitted. Container exiting 0."
