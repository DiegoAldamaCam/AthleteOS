"""Pipeline health endpoint: GET /pipeline/dlq-depth.

Spec: obs #65 Domain B — DLQ Topic Depth Endpoint.
Design: obs #66 Backend Design — DLQ endpoint section.
Decision: obs #68 — lazy module-singleton AdminClient, degrade-to-200 contract.

Business rules (LOCKED):
  - Returns depth of each DLQ topic as end_offset - committed_offset.
  - No consumer group for DLQ topics → depth = total end-offset.
  - Missing/unknown topic → depth 0, status "ok".
  - Broker unreachable (any exception) → HTTP 200 degraded envelope:
      broker_reachable: false, depth: null, status: "unavailable".
  - NEVER returns HTTP 5xx for Kafka connectivity failures.
"""

from __future__ import annotations

from fastapi import APIRouter

from api.config import settings
from api.kafka_admin import get_dlq_depths
from api.observability import DLQ_COLLECTOR, update_dlq_gauge

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

# ---------------------------------------------------------------------------
# DLQ topics to monitor (LOCKED by spec Domain B)
# Extracted as a module-level constant for testability (task 3.8 REFACTOR)
# ---------------------------------------------------------------------------
DLQ_TOPICS = [
    "dlq.canonical.training_event",
    "dlq.canonical.wellness_event",
    "dlq.canonical.planning_block",
]


@router.get(
    "/dlq-depth",
    summary="Get unprocessed message count for each DLQ topic",
)
def get_dlq_depth() -> dict:
    """Return the current DLQ depth for each tracked topic.

    Depth = end_offset − committed_offset per partition.
    No consumer group for DLQ topics → depth = total end-offset.
    Missing topic → depth 0, status "ok".

    On any Kafka connectivity failure, returns HTTP 200 with a degraded
    envelope (broker_reachable: false) — NEVER 5xx.
    """
    result = get_dlq_depths(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        dlq_topics=DLQ_TOPICS,
        request_timeout=settings.kafka_admin_request_timeout,
    )
    # Update DLQ gauge: skips degraded envelope (broker_reachable=False) to
    # retain last-value — degrade-to-200 contract and response body unchanged.
    if DLQ_COLLECTOR is not None:
        update_dlq_gauge(result, DLQ_COLLECTOR)
    return result
