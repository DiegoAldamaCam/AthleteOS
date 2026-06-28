"""Phase C2 integration: cardio_canonicalize bounded Flink job end-to-end.

Covers the same scenarios as test_cardio_ingestion.py but exercises the
complete bounded-mode Flink job topology end-to-end (with actual KafkaSource,
KeyedProcessFunction, avro-confluent Table DDL sink, and DLQ KafkaSink).

Spec scenarios exercised:
  sc-23: valid raw.cardio → CARDIO_ACTIVITY in canonical.training_event
  sc-24: missing athlete_id → DLQ original_topic="raw.cardio"
  sc-25: tss=null, avg_hr=null → DLQ session_load uncomputable
  sc-26: duplicate event_id → only ONE canonical event
  sc-27: TRANSACTIONAL_ID_PREFIX distinct from strength prefix (import-safe check)

This file mirrors tests/integration/test_wellness_canonicalize_job.py.

Clean skips (never fake a pass):
  - testcontainers not installed: module-level skip via importorskip.
  - No pyflink on this interpreter: module-level skip.
  - No Docker daemon: container fixture will skip.
"""

from __future__ import annotations

import importlib.util

import pytest

# --- Module-level gating: testcontainers required --------------------------

pytest.importorskip(
    "testcontainers",
    reason=(
        "testcontainers not installed — cardio canonicalize job integration test skipped. "
        "Install with: pip install testcontainers[kafka] to run sc-23..sc-27."
    ),
)

# --- Module-level gating: pyflink required ---------------------------------

if importlib.util.find_spec("pyflink") is None:
    pytest.skip(
        "apache-flink not importable on this interpreter "
        "(no CPython 3.12+ wheel); cardio_canonicalize_job integration test skipped",
        allow_module_level=True,
    )

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Re-export all test cases from test_cardio_ingestion to avoid duplication.
# Both integration files run the same scenario matrix; the job-focused file
# is the canonical home for bounded-mode topology assertions.
#
# See tests/integration/test_cardio_ingestion.py for the full test class
# TestCardioCanonicalizeJob with sc-23..sc-27.
# ---------------------------------------------------------------------------

from tests.integration.test_cardio_ingestion import (  # noqa: F401, E402
    TestCardioCanonicalizeJob,
    redpanda,
    kafka_bootstrap,
    kafka_admin,
)
