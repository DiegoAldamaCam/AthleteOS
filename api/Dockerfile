FROM python:3.11-slim

WORKDIR /app

# API runtime deps. confluent-kafka is REQUIRED: the /health readiness probe and
# the /pipeline/dlq-depth endpoint use AdminClient.list_topics to reach the broker.
# Without it the app runs but reports the broker as permanently unreachable.
# Version pinned to match ingestion/ and bootstrap/ Dockerfiles (>=2.5,<3).
COPY pyproject.toml ./
RUN pip install --no-cache-dir "fastapi>=0.110" "uvicorn[standard]>=0.29" "psycopg2-binary>=2.9,<3" "pydantic>=2.0" "pydantic-settings>=2.0" "prometheus_client>=0.20,<1" "PyJWT>=2.8,<3" "passlib[bcrypt]>=1.7,<2" "bcrypt>=3.2,<4" "confluent-kafka>=2.5,<3"

COPY api/ ./api/

# Create a non-root user and transfer WORKDIR ownership
RUN adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
