"""Register canonical Avro schemas in the Confluent Schema Registry.

Implements PR1 task 2.2 (register half). For each canonical topic it:

  1. Sets BACKWARD compatibility on the subject ``<topic>-value`` (TopicNameStrategy,
     ADR-10) via the Registry config API.
  2. Registers the .avsc payload as a new version of that subject.

Schema Registry never hardcodes version numbers (spec: "Producers MUST NOT
hardcode version numbers"); the Registry auto-increments and returns the id.

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
    """Register every canonical schema. Returns subject -> schema_id."""
    results: dict[str, int] = {}
    for topic, spec in CANONICAL_TOPICS.items():
        subject = _subject_for(topic)
        avsc_path = _SCHEMA_DIR / spec["avsc"]
        if not avsc_path.exists():
            raise FileNotFoundError(f"Missing Avro schema: {avsc_path}")
        set_compatibility(registry_url, subject)
        results[subject] = register_schema(registry_url, subject, avsc_path)
    return results


def main() -> int:
    import os

    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", DEFAULT_REGISTRY_URL)
    register_all(registry_url)
    print(f"[schema] done: registered {len(CANONICAL_TOPICS)} schemas against {registry_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())