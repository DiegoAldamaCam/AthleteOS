"""Single source of truth for the AthleteOS Kafka topic topology and
Schema Registry subjects.

Shared by ``bootstrap.create_topics`` (runtime) and the Phase 2 integration
tests (verification). Keeping it here prevents the bootstrap scripts and tests
from drifting on partition counts, retention, or subject naming.

Topology (from the gate-passed design + event-contracts spec):

  raw topics (JSON, time-bound, no compaction):
    raw.strength       7d
    raw.cardio         7d
    raw.recovery      14d
    raw.nutrition      7d
    raw.wellness       7d
    raw.planning      14d

  canonical topics (Avro + Schema Registry, compacted + time window):
    canonical.training_event   compacted + 30d
    canonical.wellness_event   compacted + 30d
    canonical.planning_block   compacted + 90d

  dead-letter topics (JSON, time-bound diagnostics):
    dlq.canonical.training_event    7d
    dlq.canonical.wellness_event    7d
    dlq.canonical.planning_block    7d

Partitioning: ALL topics use exactly 8 partitions (LOCKED ADR-4) with
``athlete_id`` as the message key (co-partitioning for Flink stream-stream
joins). The message key is a producer-side concern and cannot be enforced at
topic creation; it is documented here and asserted in the ingestion connectors.

Subject naming: TopicNameStrategy -> ``<topic>-value`` (ADR-10). One schema
per canonical topic (WIDE-RECORD). Compatibility = BACKWARD per subject (set at
bootstrap via the Registry config API per ADR-10 / design).

Note: tasks.md mentions "15 topics"; the gate-passed design and the
event-contracts spec topology enumerate 6 + 3 + 3 = 12. This module implements
the authoritative 12-topic topology from design + spec.
"""

from __future__ import annotations

# All topics MUST have exactly 8 partitions (LOCKED ADR-4).
PARTITION_COUNT = 8
REPLICATION_FACTOR = 1  # single-broker local dev cluster

# Day lengths in milliseconds.
_MS_PER_DAY = 24 * 60 * 60 * 1000
SEVEN_DAYS = 7 * _MS_PER_DAY
FOURTEEN_DAYS = 14 * _MS_PER_DAY
THIRTY_DAYS = 30 * _MS_PER_DAY
NINETY_DAYS = 90 * _MS_PER_DAY

# Canonical subject compatibility (Schema Registry config API).
DEFAULT_COMPATIBILITY = "BACKWARD"

# Canonical topics -> (avsc filename, retention_ms). Compacted + time window.
# Key naming follows TopicNameStrategy: "<topic>-value".
CANONICAL_TOPICS: dict[str, dict] = {
    "canonical.training_event": {
        "avsc": "TrainingEvent.avsc",
        "retention_ms": THIRTY_DAYS,
        "compacted": True,
    },
    "canonical.wellness_event": {
        "avsc": "WellnessEvent.avsc",
        "retention_ms": THIRTY_DAYS,
        "compacted": True,
    },
    "canonical.planning_block": {
        "avsc": "PlanningBlock.avsc",
        "retention_ms": NINETY_DAYS,
        "compacted": True,
    },
}

# Raw topics -> retention_ms. Time-bound only (no compaction).
RAW_TOPICS: dict[str, dict] = {
    "raw.strength": {"retention_ms": SEVEN_DAYS, "compacted": False},
    "raw.cardio": {"retention_ms": SEVEN_DAYS, "compacted": False},
    "raw.recovery": {"retention_ms": FOURTEEN_DAYS, "compacted": False},
    "raw.nutrition": {"retention_ms": SEVEN_DAYS, "compacted": False},
    "raw.wellness": {"retention_ms": SEVEN_DAYS, "compacted": False},
    "raw.planning": {"retention_ms": FOURTEEN_DAYS, "compacted": False},
}

# Dead-letter topics -> retention_ms. JSON diagnostics, time-bound, no compaction.
DLQ_TOPICS: dict[str, dict] = {
    "dlq.canonical.training_event": {"retention_ms": SEVEN_DAYS, "compacted": False},
    "dlq.canonical.wellness_event": {"retention_ms": SEVEN_DAYS, "compacted": False},
    "dlq.canonical.planning_block": {"retention_ms": SEVEN_DAYS, "compacted": False},
}


def all_topics() -> dict[str, dict]:
    """Return the full topology as topic -> config dict (retention_ms, compacted)."""
    return {**RAW_TOPICS, **CANONICAL_TOPICS, **DLQ_TOPICS}


def topic_config(retention_ms: int, compacted: bool) -> dict:
    """Build the Kafka topic configs for a given retention/compaction policy.

    Compacted+time-window canonical topics use cleanup.policy=compact,delete with
    a retention.ms time window so compacted keys remain while aged segments are
    still reaped past the window. Raw/DLQ topics use delete-only cleanup.
    """
    cleanup_policy = "compact,delete" if compacted else "delete"
    return {
        "retention.ms": str(retention_ms),
        "cleanup.policy": cleanup_policy,
        # Co-partitioning requirement: 8 partitions, single key (athlete_id).
    }