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
from bootstrap.register_schemas import _SCHEMA_DIR, register_all, register_schema, set_compatibility

pytestmark = pytest.mark.integration


@pytest.fixture
def registry(redpanda_endpoints) -> dict:
    """Seed canonical schemas directly into the Redpanda-backed registry.

    As of the G4 DEFECT-5 fix, ``register_all()`` is a NOOP — the Flink
    avro-confluent sink owns its own writer-schema registration on first
    emission.  This fixture simulates what the sink does on first emission by
    POSTing each canonical v1 ``.avsc`` schema to the Schema Registry REST API
    directly and setting BACKWARD compatibility per subject.  The existing
    BACKWARD-enforcement assertions (tests 3 & 4) are preserved without change.

    Yields the url dict and tears down all registered subjects after each test
    so that a session-scoped Redpanda container does not bleed schema state
    across test functions.
    """
    import time

    url = redpanda_endpoints["schema_registry_url"]

    # Seed v1 schemas directly (simulates Flink sink's first-emission registration).
    # register_all() is kept in the import for completeness but is intentionally
    # not called here — it returns {} and does nothing (DEFECT-5 fix).
    for topic, cfg in CANONICAL_TOPICS.items():
        subject = f"{topic}-value"
        avsc_path = _SCHEMA_DIR / cfg["avsc"]
        register_schema(url, subject, avsc_path)
        set_compatibility(url, subject, "BACKWARD")

    yield {"url": url}

    # Teardown: delete every subject registered during this test so subsequent
    # tests start from a clean slate against the same session-scoped container.
    try:
        subjects_resp = requests.get(f"{url}/subjects", timeout=10)
        subjects = subjects_resp.json() if subjects_resp.ok else []
    except Exception:
        subjects = []

    for subject in subjects:
        # Soft delete first (removes from active listing).
        try:
            requests.delete(f"{url}/subjects/{subject}", timeout=10)
        except Exception:
            pass
        # Hard (permanent) delete removes all schema data so re-registration
        # in the next test does not see leftover state.
        try:
            requests.delete(f"{url}/subjects/{subject}?permanent=true", timeout=10)
        except Exception:
            pass

    # Brief pause so the SR can flush deletes before the next test starts.
    time.sleep(0.2)


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
    """An incompatible v2 (adding a required field with no default) is REJECTED under BACKWARD.

    BACKWARD compatibility means: a v2 *reader* must be able to read v1 *data*.
    Adding a required field (non-nullable, no default) to v2 breaks this because
    v1 data will not contain that field and the v2 reader has no default to fall
    back on -> Schema Registry must reject the registration (409 or 422).

    Note: *removing* a field is BACKWARD-compatible (the v2 reader simply ignores
    the extra field present in v1 data). The old version of this test had the
    semantics inverted — it removed a field and expected rejection, but SR
    correctly returned 200.
    """
    url = registry["url"]
    subject = "canonical.training_event-value"

    # Read the registered v1 schema so we have a faithful starting point.
    latest = requests.get(f"{url}/subjects/{subject}/versions/latest", timeout=10)
    latest.raise_for_status()
    v1 = json.loads(latest.json()["schema"])

    # Build an incompatible v2 by ADDING a required field with NO default.
    # A v2 reader cannot read v1 data that lacks this field -> breaks BACKWARD.
    new_required_field = {"name": "required_intensity_score", "type": "float"}
    v2_fields = list(v1["fields"]) + [new_required_field]
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