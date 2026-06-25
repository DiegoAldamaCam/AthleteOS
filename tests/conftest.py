"""Shared pytest fixtures for AthleteOS.

The integration harness uses testcontainers. Heavy third-party imports
(testcontainers, docker SDK) are loaded lazily inside fixtures so that
`pytest --collect-only` works even when those libraries are absent - the
integration tests skip at runtime instead.

Docker gating: integration tests are skipped automatically when the Docker
daemon is not reachable (CI, sandboxed runners, Docker Desktop not started).
"""

from __future__ import annotations

import os

import pytest


def _docker_available() -> bool:
    """Return True iff a Docker daemon is reachable from this process."""
    try:
        import docker  # provided by testcontainers[kafka]
    except Exception:
        return False
    try:
        client = docker.from_env()
        client.ping()
        return True
    except Exception:
        return False


def requires_docker():
    """Skip the calling test when Docker is not available."""
    if not _docker_available():
        pytest.skip("Docker daemon not available; integration test skipped", allow_module_level=True)


@pytest.fixture(scope="session")
def docker_ok() -> bool:
    """Session-level flag: True when the Docker daemon is reachable."""
    return _docker_available()


@pytest.fixture(scope="session")
def redpanda(docker_ok):
    """A Redpanda container that serves BOTH Kafka and a Schema Registry.

    Redpanda embeds the Confluent-compatible Schema Registry on port 8081, so a
    single container provides the two services that the event-contracts spec
    requires (Kafka + Schema Registry) without a separate compose dependency.

    Skipped when Docker is unavailable. Imported lazily so collection does not
    require testcontainers to be installed.
    """
    if not docker_ok:
        pytest.skip("Docker daemon not available; redpanda fixture skipped")
    from testcontainers.kafka import RedpandaContainer

    container = RedpandaContainer()
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def redpanda_endpoints(redpanda) -> dict:
    """Bootstrap servers and Schema Registry HTTP URL backed by the Redpanda session."""
    return {
        "bootstrap_servers": redpanda.get_bootstrap_servers(),
        "schema_registry_url": redpanda.get_schema_registry_address(),
    }


@pytest.fixture(scope="session")
def postgres_container(docker_ok):
    """A throwaway PostgreSQL container for sink/parity tests (PR5+).

    Defined now so the harness shape is fixed; PR5 will exercise it.
    """
    if not docker_ok:
        pytest.skip("Docker daemon not available; postgres fixture skipped")
    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer("postgres:16")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def repo_root() -> "Path":  # type: ignore[name-defined]
    """Absolute path to the repository root."""
    from pathlib import Path

    return Path(__file__).resolve().parents[1]


@pytest.fixture
def canonical_schema_dir(repo_root) -> "Path":  # type: ignore[name-defined]
    return repo_root / "schemas" / "canonical"


# Make integration-only env knobs injectable without hard deps.
@pytest.fixture
def schema_registry_env() -> dict:
    return {
        "SCHEMA_REGISTRY_URL": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        "KAFKA_BOOTSTRAP_SERVERS": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
    }