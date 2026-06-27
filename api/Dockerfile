FROM python:3.11-slim

WORKDIR /app

# Install only the API runtime deps (not heavy streaming deps)
COPY pyproject.toml ./
RUN pip install --no-cache-dir "fastapi>=0.110" "uvicorn[standard]>=0.29" "psycopg2-binary>=2.9,<3" "pydantic>=2.0" "pydantic-settings>=2.0"

COPY api/ ./api/

# Create a non-root user and transfer WORKDIR ownership
RUN adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
