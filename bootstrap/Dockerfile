# Python 3.12-slim base; matches the test/dev runtime.
FROM python:3.12-slim

WORKDIR /app

# Runtime client deps only: confluent-kafka for AdminClient topic creation and
# requests for the Schema Registry REST API. No Flink, no heavy streaming deps.
RUN pip install --no-cache-dir "confluent-kafka>=2.5" "requests>=2.31"

# Copy the bootstrap package + canonical schemas. The repo root is the build
# context, so the package layout is preserved for `python -m bootstrap.run`.
COPY bootstrap/ /app/bootstrap/
COPY schemas/ /app/schemas/

ENTRYPOINT ["python", "-m", "bootstrap.run"]