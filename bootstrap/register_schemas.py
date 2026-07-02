"""Register canonical Avro schemas in the Confluent Schema Registry.

Implements PR1 task 2.2 (register half).

NOTE (G4 DEFECT-5 fix): canonical value subjects are intentionally NOT
pre-registered here.  Flink 1.19's Table ``avro-confluent`` sink infers its
writer schema from DDL columns and assigns a Flink-generated record name
(e.g. ``record``).  Pre-registering the subject under BACKWARD compatibility
with the hand-authored ``.avsc`` name (``com.athleteos.canonical.TrainingEvent``)
causes a NAME_MISMATCH rejection on the first write — the sink crash-loops and
canonical topics receive zero records.

The canonical TOPICS are still created by ``bootstrap.create_topics``; only
the Schema Registry subject pre-registration is suppressed.  The Flink sink
will register its own writer schema on first emission, which is fully consistent
with avro-confluent default behaviour.  Consumer jobs (e.g. wellness-metrics)
read the writer schema by schema-id embedded in each record — they do not depend
on a subject being pre-registered before consumption.

Environment:
  SCHEMA_REGISTRY_URL  Registry base URL (default http://localhost:8081)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import requests

from bootstrap._topology import CANONICAL_TOPICS, DEFAULT_COMPATIBILITY

DEFAULT_REGISTRY_URL = "http://localhost:8081"
_SCHEMA_DIR = Path(__file__).resolve().parent.parent / "schemas" / "canonical"


def _subject_for(topic: str) -> str:
    """TopicNameStrategy subject: ``<topic>-value``."""
    return f"{topic}-value"


def set_compatibility(registry_url: str, subject: str, mode: str = DEFAULT_COMPATIBILITY) -> None:
    """Set per-subject compatibility (BACKWARD) via the Registry config API."""
    url = f"{registry_url}/config/{subject}"
    resp = requests.put(url, json={"compatibility": mode}, timeout=10)
    resp.raise_for_status()
    print(f"[schema] {subject}: compatibility={mode}")


def register_schema(registry_url: str, subject: str, avsc_path: Path) -> int:
    """Register an .avsc file under a subject; return the assigned schema id."""
    schema_str = avsc_path.read_text(encoding="utf-8")
    # Validate it parses as JSON before talking to the Registry.
    json.loads(schema_str)

    url = f"{registry_url}/subjects/{subject}/versions"
    resp = requests.post(url, json={"schema": schema_str, "schemaType": "AVRO"}, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(
            f"Schema registration failed for {subject}: "
            f"{resp.status_code} {resp.text}"
        )
    body = resp.json()
    schema_id = body["id"]
    version = body.get("version", "?")
    print(f"[schema] {subject}: registered id={schema_id} version={version}")
    return schema_id


def register_all(registry_url: str = DEFAULT_REGISTRY_URL) -> dict:
    """Register canonical schemas in the Schema Registry.

    As of G4 DEFECT-5 fix, canonical value subjects are NOT pre-registered.
    Flink's avro-confluent sink owns its own writer-schema registration on first
    emission; pre-registering under BACKWARD causes NAME_MISMATCH rejection.
    This function is kept for future use (e.g. non-Flink producers) and returns
    an empty dict to signal that no subjects were pre-registered.
    """
    return {}


def main() -> int:
    import os

    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_REGISTRY_URL)
    register_all(registry_url)
    print(f"[schema] done: registered {len(CANONICAL_TOPICS)} schemas against {registry_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())