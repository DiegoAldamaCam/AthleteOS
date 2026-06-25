"""One-shot bootstrap entrypoint: register schemas then create topics.

Invoked by the ``schema-bootstrap`` Compose service (profile ``bootstrap``).
Idempotent enough for re-runs: schema registration re-versions up only when
the schema content changes; topic creation skips existing topics.
"""

from __future__ import annotations

import os
import sys

from bootstrap.create_topics import create_all
from bootstrap.register_schemas import register_all


def main() -> int:
    registry_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    print(f"[bootstrap] schema registry: {registry_url}")
    print(f"[bootstrap] kafka bootstrap:  {bootstrap}")

    register_all(registry_url)
    create_all(bootstrap)
    print("[bootstrap] done")
    return 0


if __name__ == "__main__":
    sys.exit(main())