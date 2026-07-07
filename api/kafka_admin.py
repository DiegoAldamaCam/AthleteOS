"""Lazy AdminClient singleton and DLQ depth computation.

Design: obs #66 (ADR-16) — module-singleton AdminClient created lazily on the
first call to get_dlq_depths(), cached across requests. All admin calls wrapped
in try/except so a broker outage degrades to a 200 envelope instead of 5xx.

Decision source: obs #68 — depth = end_offset - committed_offset per partition;
no consumer group → depth = total end-offset; missing topic → depth 0 / status "ok".

Public API:
  - compute_dlq_status(depth)          pure function: depth → "ok" | "warning"
  - build_degraded_response(topics)    pure function: builds broker_reachable=false envelope
  - get_dlq_depths(bootstrap, timeout) queries AdminClient, returns DlqDepthResponse dict
"""

from __future__ import annotations

from typing import Optional

# ---------------------------------------------------------------------------
# Module-level singleton (lazy: created on first call, reused thereafter)
# ---------------------------------------------------------------------------

_admin_client_singleton: Optional[object] = None


def _get_or_create_admin_client(bootstrap_servers: str, request_timeout: float):
    """Return the cached AdminClient, creating it lazily on first call.

    The client is created without connecting eagerly — confluent_kafka
    AdminClient connects on first operation, which keeps the API bootable
    even when the broker is down (ADR-16).
    """
    global _admin_client_singleton  # noqa: PLW0603
    if _admin_client_singleton is None:
        from confluent_kafka.admin import AdminClient

        _admin_client_singleton = AdminClient(
            {
                "bootstrap.servers": bootstrap_servers,
                "socket.timeout.ms": int(request_timeout * 1000),
                "request.timeout.ms": int(request_timeout * 1000),
            }
        )
    return _admin_client_singleton


# ---------------------------------------------------------------------------
# Pure helper functions (extracted for testability — zero mocks needed)
# ---------------------------------------------------------------------------


def compute_dlq_status(depth: int) -> str:
    """Map a DLQ depth value to a status string.

    Spec contract (obs #65 Domain B):
      - depth == 0 → "ok"
      - depth  > 0 → "warning"

    Negative values (shouldn't occur: committed > end) are clamped to 0 → "ok".
    """
    if depth <= 0:
        return "ok"
    return "warning"


def build_degraded_response(topics: list[str]) -> dict:
    """Build the broker-unreachable degraded envelope.

    Spec contract: HTTP 200, broker_reachable=false, depth=null, status="unavailable"
    for every DLQ topic entry. NEVER 5xx.
    """
    return {
        "broker_reachable": False,
        "topics": [
            {"topic": t, "depth": None, "status": "unavailable"}
            for t in topics
        ],
    }


# ---------------------------------------------------------------------------
# Main query function
# ---------------------------------------------------------------------------


def get_dlq_depths(
    bootstrap_servers: str,
    dlq_topics: list[str],
    request_timeout: float = 5.0,
) -> dict:
    """Query the Kafka broker for DLQ topic depths.

    Returns a dict with:
      {
        "broker_reachable": bool,
        "topics": [
          {"topic": str, "depth": int | None, "status": "ok" | "warning" | "unavailable"}
        ]
      }

    Depth contract (obs #68):
      - depth = sum(end_offset) - sum(committed_offset) per partition
      - no consumer group → depth = total end-offset (all messages unconsumed)
      - missing/unknown topic → depth 0, status "ok"
      - any connectivity exception → degraded envelope (broker_reachable=False)

    All Kafka exceptions are caught and converted to the degraded envelope.
    NEVER raises or returns 5xx.
    """
    try:
        admin = _get_or_create_admin_client(bootstrap_servers, request_timeout)
        return _compute_depths(admin, dlq_topics, request_timeout)
    except Exception:
        # Any failure (broker down, timeout, auth, etc.) → degraded envelope.
        # Clear the cached client so the next request rebuilds it: the singleton
        # is assigned before any I/O, so a first-call failure must not leave a
        # client pinned to a stale/unreachable bootstrap address forever.
        global _admin_client_singleton  # noqa: PLW0603
        _admin_client_singleton = None
        return build_degraded_response(dlq_topics)


def _compute_depths(admin, dlq_topics: list[str], request_timeout: float) -> dict:
    """Compute per-topic DLQ depths using AdminClient.list_offsets.

    Raises on connectivity failure — caller wraps in try/except.
    """
    from confluent_kafka.admin import OffsetSpec
    from confluent_kafka import TopicPartition

    # Step 1: Fetch topic metadata to discover partition counts
    metadata = admin.list_topics(timeout=request_timeout)

    topic_entries = []
    for topic_name in dlq_topics:
        try:
            depth = _depth_for_topic(admin, metadata, topic_name, request_timeout)
            status = compute_dlq_status(depth)
            topic_entries.append({"topic": topic_name, "depth": depth, "status": status})
        except Exception:
            # Partial failure: treat this specific topic as depth 0 / ok
            topic_entries.append({"topic": topic_name, "depth": 0, "status": "ok"})

    return {"broker_reachable": True, "topics": topic_entries}


def _depth_for_topic(
    admin,
    metadata,
    topic_name: str,
    request_timeout: float,
) -> int:
    """Return the depth (live unprocessed message count) for a single DLQ topic.

    Depth = sum(latest_offset - earliest_offset) across partitions.

    DLQ topics have no consumer group, so every retained message is unconsumed.
    But they DO have retention (cleanup.policy=delete, retention.ms), so expired
    messages are deleted and the earliest offset advances while the latest offset
    keeps climbing. Using latest alone would report the cumulative historical
    end-offset (messages that ever existed) instead of what is actually retained.
    Subtracting earliest yields the true live depth (0 once everything expires).

    Missing topic in metadata → depth 0.
    """
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import OffsetSpec

    # Missing topic → depth 0 (partial-failure spec scenario S4)
    if topic_name not in metadata.topics:
        return 0

    topic_meta = metadata.topics[topic_name]
    if not topic_meta.partitions:
        return 0

    # Build TopicPartition list for all partitions
    partitions = [
        TopicPartition(topic_name, partition_id)
        for partition_id in topic_meta.partitions.keys()
    ]

    # Fetch both latest (high-watermark) and earliest (low-watermark) offsets so
    # depth reflects only retained messages, not the historical cumulative count.
    end_offset_specs = {tp: OffsetSpec.latest() for tp in partitions}
    start_offset_specs = {tp: OffsetSpec.earliest() for tp in partitions}
    end_futures = admin.list_offsets(end_offset_specs, request_timeout=request_timeout)
    start_futures = admin.list_offsets(start_offset_specs, request_timeout=request_timeout)

    from confluent_kafka import OFFSET_INVALID

    # Resolve earliest offsets into a per-partition lookup keyed by (topic, partition).
    earliest_by_tp: dict = {}
    for tp, future in start_futures.items():
        result = future.result()  # raises on failure
        offset = result.offset
        earliest_by_tp[(tp.topic, tp.partition)] = (
            offset if offset != OFFSET_INVALID and offset > 0 else 0
        )

    total_depth = 0
    for tp, future in end_futures.items():
        result = future.result()  # raises on failure
        latest = result.offset
        # ADR H7: explicit named guard for the OFFSET_INVALID sentinel (-1001).
        # A partition returning OFFSET_INVALID contributes 0 to depth;
        # broker_reachable remains True (the broker responded — the offset is
        # simply unavailable for this partition).
        if latest == OFFSET_INVALID or latest <= 0:
            continue
        earliest = earliest_by_tp.get((tp.topic, tp.partition), 0)
        # Clamp per-partition so a transient earliest > latest never goes negative.
        total_depth += max(0, latest - earliest)

    return max(0, total_depth)
