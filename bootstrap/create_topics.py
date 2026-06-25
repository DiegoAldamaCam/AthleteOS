"""Create the AthleteOS Kafka topic topology.

Implements PR1 task 2.2 (topics half). Creates all raw, canonical, and DLQ
topics with exactly 8 partitions (LOCKED ADR-4) and the retention/compaction
configs defined in ``bootstrap._topology`` (per the event-contracts spec and
the gate-passed design).

The message key (athlete_id) is a producer-side concern and is therefore
documented rather than enforced here; ingestion connectors (PR2+) populate it.

Environment:
  KAFKA_BOOTSTRAP_SERVERS  broker address (default localhost:9092)
"""

from __future__ import annotations

import os
import sys

from bootstrap._topology import (
    PARTITION_COUNT,
    REPLICATION_FACTOR,
    all_topics,
    topic_config,
)

DEFAULT_BOOTSTRAP = "localhost:9092"


def create_all(bootstrap_servers: str = DEFAULT_BOOTSTRAP) -> dict[str, dict]:
    """Create every topic if absent. Returns topic -> reported config dict."""
    # confluent-kafka AdminClient is the project's chosen Kafka client (LOCKED
    # stack). Imported lazily so this module remains importable in environments
    # where confluent-kafka is not yet installed (e.g. `pytest --collect-only`
    # before `pip install -e ".[dev]"` has run).
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing = set(admin.list_topics(timeout=10).topics.keys())
    new_topics = []
    for topic, spec in all_topics().items():
        if topic in existing:
            print(f"[topics] {topic} exists, leaving as-is")
            continue
        cfg = topic_config(spec["retention_ms"], spec["compacted"])
        new_topics.append(
            NewTopic(
                topic=topic,
                num_partitions=PARTITION_COUNT,
                replication_factor=REPLICATION_FACTOR,
                config=cfg,
            )
        )

    if new_topics:
        # create_topics returns a dict of futures, one per topic, to await.
        fs = admin.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()  # raises on failure
                print(f"[topics] created {topic} (partitions={PARTITION_COUNT})")
            except Exception as exc:  # noqa: BLE001
                print(f"[topics] FAILED creating {topic}: {exc}", file=sys.stderr)
                raise

    # Report the intended topology for callers; topic existence + configs are
    # verified by tests via AdminClient.describe/metadata (see integration suite).
    return {t: topic_config(spec["retention_ms"], spec["compacted"]) for t, spec in all_topics().items()}


def main() -> int:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    create_all(bootstrap)
    print(f"[topics] done: ensured {len(all_topics())} topics ({PARTITION_COUNT} partitions each)")
    return 0


if __name__ == "__main__":
    sys.exit(main())