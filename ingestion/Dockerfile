# python:3.11-slim — aligns with pyproject requires-python >=3.11,<3.12 (ADR-IW5).
FROM python:3.11-slim

WORKDIR /app

# Install only the runtime dep: confluent-kafka for Producer.
# No Schema Registry client needed (raw topics use JSON, not Avro).
COPY pyproject.toml ./
RUN pip install --no-cache-dir "confluent-kafka>=2.5,<3"

# Copy the ingestion package (all 6 connectors + supervisor __main__.py).
COPY ingestion/ ./ingestion/

ENTRYPOINT ["python", "-m", "ingestion"]
