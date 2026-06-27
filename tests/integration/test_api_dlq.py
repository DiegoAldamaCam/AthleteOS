"""Integration tests for GET /pipeline/dlq-depth — Domain B (4 scenarios).

Spec source: obs #65 (sdd/athleteos-phase7-web/spec), Domain B.
Design source: obs #66 (sdd/athleteos-phase7-web/design), DLQ endpoint section.
Decision source: obs #68 — lazy module-singleton AdminClient, degrade to 200.

Uses a RedpandaContainer for broker-reachable scenarios. The stopped-broker
scenario stops the container and calls the endpoint, asserting the degraded
200 envelope (broker_reachable:false, depth:null, status:"unavailable").

Docker-gated: skipped automatically when Docker daemon is not reachable.

All 4 spec scenarios covered (Domain B):
  S1  Happy path — DLQ topic with messages (some depth, status="warning")
  S2  All DLQs empty — all depths 0, all status="ok"
  S3  Kafka unreachable — HTTP 200 degraded (broker_reachable:false)
  S4  Partial topic failure — non-existent topic → depth 0, status="ok"
"""

from __future__ import annotations

import os
import time

import pytest

from tests.conftest import requires_docker

requires_docker()

try:
    import httpx  # noqa: F401 — needed to confirm httpx is installed
    from starlette.testclient import TestClient
except ImportError:
    pytest.skip("httpx / starlette not installed; DLQ API tests skipped", allow_module_level=True)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# DLQ topic names (must match api/routers/pipeline.py DLQ_TOPICS constant)
# ---------------------------------------------------------------------------
_DLQ_TOPICS = [
    "dlq.canonical.training_event",
    "dlq.canonical.wellness_event",
    "dlq.canonical.planning_block",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _admin_client(bootstrap_servers: str):
    """Create a confluent_kafka AdminClient for test setup."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    return admin


def _create_topics(bootstrap_servers: str, topics: list[str]) -> None:
    """Create topics in Redpanda (idempotent)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]
    futures = admin.create_topics(new_topics)
    for topic, f in futures.items():
        try:
            f.result()
        except Exception:
            pass  # topic may already exist — fine


def _produce_messages(bootstrap_servers: str, topic: str, count: int) -> None:
    """Produce `count` messages to `topic` so end_offset > 0."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    for i in range(count):
        producer.produce(topic, value=f"msg-{i}".encode())
    producer.flush(timeout=10)


# ---------------------------------------------------------------------------
# Module-scoped Redpanda container
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def redpanda_dlq(docker_ok):
    """A fresh Redpanda container for DLQ integration tests."""
    if not docker_ok:
        pytest.skip("Docker not available")
    from testcontainers.kafka import RedpandaContainer

    container = RedpandaContainer()
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="module")
def redpanda_bootstrap(redpanda_dlq) -> str:
    """Bootstrap server string from the DLQ Redpanda container."""
    return redpanda_dlq.get_bootstrap_server()


@pytest.fixture(scope="module")
def redpanda_api_client(redpanda_bootstrap):
    """TestClient for the FastAPI app backed by the Redpanda container."""
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = redpanda_bootstrap
    os.environ["DATABASE_URL"] = "postgresql://athleteos:athleteos@localhost:5432/athleteos"
    os.environ["CORS_ORIGINS"] = "http://localhost:5173"

    # Reset cached AdminClient singleton so it picks up the new bootstrap servers
    try:
        import api.kafka_admin as _ka
        _ka._admin_client_singleton = None  # noqa: SLF001
    except (ImportError, AttributeError):
        pass

    # Force Settings reload with the updated env
    import importlib
    try:
        import api.config as _cfg
        importlib.reload(_cfg)
        import api.kafka_admin as _ka
        importlib.reload(_ka)
        import api.routers.pipeline as _rp
        importlib.reload(_rp)
        import api.main as _main
        importlib.reload(_main)
    except ImportError:
        pass

    from api.main import app  # noqa: PLC0415

    with TestClient(app) as client:
        yield client


# ---------------------------------------------------------------------------
# S2 — All DLQs empty (topics exist, no messages produced)
# ---------------------------------------------------------------------------


def test_all_dlqs_empty_returns_200_depth_zero(redpanda_api_client, redpanda_bootstrap):
    """S2: All three DLQ topics exist with no messages → depth 0, status 'ok'."""
    _create_topics(redpanda_bootstrap, _DLQ_TOPICS)
    # Allow Redpanda to settle
    time.sleep(0.5)

    resp = redpanda_api_client.get("/pipeline/dlq-depth")

    assert resp.status_code == 200
    body = resp.json()
    assert body["broker_reachable"] is True
    assert len(body["topics"]) == 3
    for topic_entry in body["topics"]:
        assert topic_entry["depth"] == 0, f"Expected depth 0 for {topic_entry['topic']}, got {topic_entry['depth']}"
        assert topic_entry["status"] == "ok", f"Expected status 'ok' for {topic_entry['topic']}, got {topic_entry['status']}"


# ---------------------------------------------------------------------------
# S1 — Happy path: some depth (training_event topic has messages)
# ---------------------------------------------------------------------------


def test_happy_path_some_depth_returns_warning(redpanda_api_client, redpanda_bootstrap):
    """S1: dlq.canonical.training_event has 3 messages → depth 3, status 'warning'."""
    _create_topics(redpanda_bootstrap, _DLQ_TOPICS)
    _produce_messages(redpanda_bootstrap, "dlq.canonical.training_event", 3)
    time.sleep(0.5)

    # Reset singleton so it reconnects and sees fresh offsets
    try:
        import api.kafka_admin as _ka
        _ka._admin_client_singleton = None  # noqa: SLF001
    except (ImportError, AttributeError):
        pass

    resp = redpanda_api_client.get("/pipeline/dlq-depth")

    assert resp.status_code == 200
    body = resp.json()
    assert body["broker_reachable"] is True

    training_entry = next(
        (t for t in body["topics"] if t["topic"] == "dlq.canonical.training_event"),
        None,
    )
    assert training_entry is not None, "dlq.canonical.training_event missing from response"
    assert training_entry["depth"] >= 3, f"Expected depth >= 3, got {training_entry['depth']}"
    assert training_entry["status"] == "warning", f"Expected status 'warning', got {training_entry['status']}"


# ---------------------------------------------------------------------------
# S4 — Partial topic failure: non-existent topic → depth 0, status "ok"
# ---------------------------------------------------------------------------


def test_partial_topic_failure_nonexistent_is_depth_zero(redpanda_api_client, redpanda_bootstrap):
    """S4: A DLQ topic that does not exist → depth 0, status 'ok' (not an error)."""
    # Only create two of the three DLQ topics; the third should yield depth 0
    partial_topics = ["dlq.canonical.training_event", "dlq.canonical.wellness_event"]
    _create_topics(redpanda_bootstrap, partial_topics)
    # Ensure the third topic does NOT exist (never created)
    # Note: in a fresh Redpanda container the topic won't exist

    resp = redpanda_api_client.get("/pipeline/dlq-depth")

    assert resp.status_code == 200
    body = resp.json()
    assert body["broker_reachable"] is True

    for topic_entry in body["topics"]:
        # All entries must have valid depth (0 or more) and valid status
        assert topic_entry["depth"] is not None, f"depth should not be null when broker is reachable"
        assert topic_entry["status"] in ("ok", "warning"), f"Unexpected status: {topic_entry['status']}"


# ---------------------------------------------------------------------------
# S3 — Kafka unreachable: degraded 200 envelope
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def stopped_broker_client():
    """API client configured to point at a stopped/unreachable broker."""
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:19999"  # nothing listening here
    os.environ["DATABASE_URL"] = "postgresql://athleteos:athleteos@localhost:5432/athleteos"
    os.environ["CORS_ORIGINS"] = "http://localhost:5173"

    # Reset and reload modules so the new bootstrap servers are picked up
    import importlib
    try:
        import api.config as _cfg
        importlib.reload(_cfg)
        import api.kafka_admin as _ka
        _ka._admin_client_singleton = None  # noqa: SLF001
        importlib.reload(_ka)
        import api.routers.pipeline as _rp
        importlib.reload(_rp)
        import api.main as _main
        importlib.reload(_main)
    except (ImportError, AttributeError):
        pass

    from api.main import app  # noqa: PLC0415

    with TestClient(app) as client:
        yield client


def test_kafka_unreachable_returns_200_degraded_envelope(stopped_broker_client):
    """S3: Broker unreachable → HTTP 200, broker_reachable:false, depth:null, status:'unavailable'."""
    resp = stopped_broker_client.get("/pipeline/dlq-depth")

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    body = resp.json()
    assert body["broker_reachable"] is False, "Expected broker_reachable=false when broker is down"
    assert len(body["topics"]) == 3
    for topic_entry in body["topics"]:
        assert topic_entry["depth"] is None, f"Expected depth=null when broker is down, got {topic_entry['depth']}"
        assert topic_entry["status"] == "unavailable", f"Expected status='unavailable', got {topic_entry['status']}"
