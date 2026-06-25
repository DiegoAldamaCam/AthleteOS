"""Phase 2.3 integration: Schema Registry registration + BACKWARD enforcement.

Verifies (against a real Schema Registry via the testcontainers Redpanda
fixture):
  - each canonical schema registers successfully under ``<topic>-value``
  - BACKWARD compatibility is enforced: an incompatible v2 schema is rejected
    (the event-contracts spec "Incompatible schema change rejected" scenario)

Heavy third-party imports (testcontainers, requests) are resolved lazily where
practical; the Redpanda fixture (in conftest) skips these tests when Docker is
unavailable. Requires ``requests`` (a dev/test dependency).
"""

from __future__ import annotations

import json

import pytest
import requests

from bootstrap._topology import CANONICAL_TOPICS
from bootstrap.register_schemas import register_all, set_compatibility

pytestmark = pytest.mark.integration


@pytest.fixture
def registry(redpanda_endpoints) -> dict:
    """Register all schemas against the Redpanda-backed registry, return urls."""
    url = redpanda_endpoints["schema_registry_url"]
    register_all(url)
    return {"url": url}


def test_each_canonical_schema_registers(registry):
    """Every canonical subject has at least one registered version."""
    url = registry["url"]
    resp = requests.get(f"{url}/subjects", timeout=10)
    resp.raise_for_status()
    subjects = set(resp.json())

    for topic in CANONICAL_TOPICS:
        subject = f"{topic}-value"
        assert subject in subjects, f"missing registered subject: {subject}"

        versions = requests.get(f"{url}/subjects/{subject}/versions", timeout=10)
        versions.raise_for_status()
        assert len(versions.json()) >= 1, f"{subject} has no versions"


def test_backward_compatibility_is_set_per_subject(registry):
    """Per-subject compatibility is BACKWARD (Registry config API)."""
    url = registry["url"]
    for topic in CANONICAL_TOPICS:
        subject = f"{topic}-value"
        resp = requests.get(f"{url}/config/{subject}", timeout=10)
        resp.raise_for_status()
        cfg = resp.json()
        assert cfg.get("compatibilityLevel") == "BACKWARD", (
            f"{subject} compatibility is {cfg.get('compatibilityLevel')}, expected BACKWARD"
        )


def test_incompatible_schema_is_rejected(registry):
    """An incompatible v2 (removing a required field) is REJECTED under BACKWARD.

    Mirrors the spec scenario: register TrainingEvent v1, then attempt v2 that
    removes the required ``reps`` field -> 409 Conflict with a compatibility
    error. BACKWARD means existing consumers (reader schema v1) must still be
    able to read v2 data, so removing a field they expect is rejected.
    """
    # Re-register TrainingEvent alone to get a known v1 baseline.
    url = registry["url"]
    subject = "canonical.training_event-value"

    # Read the registered v1 schema so we have a faithful starting point.
    latest = requests.get(f"{url}/subjects/{subject}/versions/latest", timeout=10)
    latest.raise_for_status()
    v1 = json.loads(latest.json()["schema"])

    # Build an incompatible v2 by REMOVING a required field (session_load).
    # `session_load` is non-nullable/required in v1 -> not backward compatible.
    v2_fields = [f for f in v1["fields"] if f["name"] != "session_load"]
    v2 = {"type": "record", "name": v1["name"], "namespace": v1["namespace"], "fields": v2_fields}

    resp = requests.post(
        f"{url}/subjects/{subject}/versions",
        json={"schema": json.dumps(v2), "schemaType": "AVRO"},
        timeout=10,
    )
    assert resp.status_code in (409, 422), (
        f"expected compatibility rejection (409/422), got {resp.status_code}: {resp.text}"
    )


def test_compatible_optional_field_addition_is_accepted(registry):
    """Adding a new optional field with a default IS accepted under BACKWARD.

    Mirrors the spec scenario: a new optional ``velocity_mps`` (nullable +
    default null) is backward compatible and registers as a new version.
    """
    url = registry["url"]
    subject = "canonical.training_event-value"
    latest = requests.get(f"{url}/subjects/{subject}/versions/latest", timeout=10)
    latest.raise_for_status()
    v1 = json.loads(latest.json()["schema"])

    v2_fields = list(v1["fields"]) + [
        {"name": "velocity_mps", "type": ["null", "float"], "default": None}
    ]
    v2 = {"type": "record", "name": v1["name"], "namespace": v1["namespace"], "fields": v2_fields}

    resp = requests.post(
        f"{url}/subjects/{subject}/versions",
        json={"schema": json.dumps(v2), "schemaType": "AVRO"},
        timeout=10,
    )
    assert resp.status_code == 200, (
        f"expected successful registration (200), got {resp.status_code}: {resp.text}"
    )