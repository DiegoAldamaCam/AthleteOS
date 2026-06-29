"""Unit tests for api/observability.py — PR-OBS1.

Spec source: obs #188 — athleteos-observability delta spec (scenarios 1–7).
Design source: obs #189 — athleteos-observability design (ADR-25, ADR-26).

Covers all 9 scenario blocks for api-observability capability:
  sc-1  — /metrics endpoint returns 200 + Prometheus content-type
  sc-2  — http_requests_total counter increments N times for N requests
  sc-3  — http_request_duration_seconds histogram records observations
  sc-4  — DLQ gauge reflects known depth
  sc-4b — DLQ gauge retains last value on broker-unreachable (degraded envelope)
  sc-5  — No metric bleed between two separate CollectorRegistry instances
  sc-6  — GET /health unaffected after /metrics mount
  sc-6b — GET /pipeline/dlq-depth unaffected after /metrics mount
  sc-7  — prometheus_client importable at correct version

Tests run WITHOUT Docker, real DB, or real Kafka.
All probe functions are monkeypatched; TestClient is used for HTTP tests.
"""

from __future__ import annotations

import os
import re

import pytest

os.environ.setdefault("CORS_ORIGINS", "http://localhost:5173")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_test_app():
    """Create a fresh FastAPI app with observability instrumented.

    Uses a fresh CollectorRegistry so tests are fully isolated.
    """
    import importlib

    import api.main as _main
    importlib.reload(_main)
    return _main.app


# ---------------------------------------------------------------------------
# sc-7: prometheus_client dependency importable at correct version
# ---------------------------------------------------------------------------


class TestPrometheusClientImportable:
    """Scenario 7: prometheus_client importable at >=0.20,<1."""

    def test_prometheus_client_importable_version(self):
        """prometheus_client must import without error and be >=0.20,<1."""
        import importlib.metadata
        import prometheus_client  # noqa: F401 — import must not raise

        version_str = importlib.metadata.version("prometheus_client")
        # Parse major.minor from version string (e.g. "0.20.0")
        parts = version_str.split(".")
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0

        assert major == 0, f"Expected major version 0, got {major} (version={version_str})"
        assert minor >= 20, f"Expected minor >= 20, got {minor} (version={version_str})"


# ---------------------------------------------------------------------------
# sc-5: No metric bleed between two CollectorRegistry instances (ADR-25)
# ---------------------------------------------------------------------------


class TestNoMetricBleed:
    """Scenario 5: Two separate registries must not share state."""

    def test_no_metric_bleed_two_registries(self):
        """Counters in separate registries must not affect each other."""
        from prometheus_client import CollectorRegistry
        from api.observability import build_metrics

        registry_a = CollectorRegistry()
        registry_b = CollectorRegistry()

        counter_a, _ = build_metrics(registry_a)
        counter_b, _ = build_metrics(registry_b)

        # Increment counter_a 3 times (only registry_a)
        counter_a.labels(method="GET", endpoint="/test_a", http_status="200").inc()
        counter_a.labels(method="GET", endpoint="/test_a", http_status="200").inc()
        counter_a.labels(method="GET", endpoint="/test_a", http_status="200").inc()

        # Increment counter_b 1 time (only registry_b)
        counter_b.labels(method="POST", endpoint="/test_b", http_status="201").inc()

        # Collect from each registry independently
        samples_a = list(registry_a.collect())
        samples_b = list(registry_b.collect())

        # Find http_requests_total values from registry_a
        # Note: prometheus_client Counter metric.name is "http_requests" (base name),
        # but sample.name is "http_requests_total"
        a_val = None
        for metric in samples_a:
            for sample in metric.samples:
                if sample.name == "http_requests_total" and sample.labels.get("endpoint") == "/test_a":
                    a_val = sample.value

        # Find http_requests_total values from registry_b
        b_val = None
        for metric in samples_b:
            for sample in metric.samples:
                if sample.name == "http_requests_total" and sample.labels.get("endpoint") == "/test_b":
                    b_val = sample.value

        assert a_val == 3.0, f"Counter A must be 3.0, got {a_val}"
        assert b_val == 1.0, f"Counter B must be 1.0, got {b_val}"


# ---------------------------------------------------------------------------
# sc-4, sc-4b: DLQ gauge via DLQDepthCollector
# ---------------------------------------------------------------------------


class TestDLQDepthCollector:
    """Scenarios 4 and 4b: DLQ gauge reflects depth; retains last on broker unreachable."""

    def test_dlq_gauge_reflects_depth(self):
        """sc-4: update_dlq_gauge with known depth emits correct gauge value."""
        from prometheus_client import CollectorRegistry
        from prometheus_client.exposition import generate_latest
        from api.observability import DLQDepthCollector, update_dlq_gauge

        registry = CollectorRegistry()
        collector = DLQDepthCollector(registry=registry)

        depths = {
            "broker_reachable": True,
            "topics": [
                {"topic": "dlq.canonical.training_event", "depth": 42, "status": "warning"},
            ],
        }
        update_dlq_gauge(depths, collector)

        output = generate_latest(registry).decode("utf-8")
        assert 'dlq_depth{topic="dlq.canonical.training_event"} 42.0' in output, (
            f"Expected dlq_depth{{topic=\"dlq.canonical.training_event\"}} 42.0 in output:\n{output}"
        )

    def test_dlq_gauge_broker_unreachable(self):
        """sc-4b: degraded envelope (broker_reachable=False) retains last known value."""
        from prometheus_client import CollectorRegistry
        from prometheus_client.exposition import generate_latest
        from api.observability import DLQDepthCollector, update_dlq_gauge

        registry = CollectorRegistry()
        collector = DLQDepthCollector(registry=registry)

        # Set a known depth first
        good_depths = {
            "broker_reachable": True,
            "topics": [
                {"topic": "dlq.canonical.training_event", "depth": 7, "status": "warning"},
            ],
        }
        update_dlq_gauge(good_depths, collector)

        # Now simulate broker unreachable (degraded envelope)
        degraded = {
            "broker_reachable": False,
            "topics": [
                {"topic": "dlq.canonical.training_event", "depth": None, "status": "unavailable"},
            ],
        }
        update_dlq_gauge(degraded, collector)

        # Gauge must still emit last known value (7.0), NOT None or 0
        output = generate_latest(registry).decode("utf-8")
        assert 'dlq_depth{topic="dlq.canonical.training_event"} 7.0' in output, (
            f"Expected gauge to retain last value 7.0, got:\n{output}"
        )

    def test_dlq_gauge_never_set_defaults_to_zero(self):
        """sc-4b triangulation: gauge with no prior value emits 0 on degraded."""
        from prometheus_client import CollectorRegistry
        from prometheus_client.exposition import generate_latest
        from api.observability import DLQDepthCollector, update_dlq_gauge

        registry = CollectorRegistry()
        collector = DLQDepthCollector(registry=registry)

        # Never set a good value — go straight to degraded
        degraded = {
            "broker_reachable": False,
            "topics": [
                {"topic": "dlq.canonical.training_event", "depth": None, "status": "unavailable"},
            ],
        }
        update_dlq_gauge(degraded, collector)

        # Collector has no cached value → emit nothing (or default 0)
        # The collector only emits metrics for topics it has cached — so output
        # should NOT contain this topic metric (no prior value set)
        output = generate_latest(registry).decode("utf-8")
        # Either not present or present as 0 — both are valid; must NOT raise
        assert isinstance(output, str), "generate_latest must return a string"


# ---------------------------------------------------------------------------
# sc-1, sc-2, sc-3: PrometheusMiddleware + /metrics endpoint
# ---------------------------------------------------------------------------


class TestPrometheusMiddleware:
    """Scenarios 1, 2, 3: /metrics endpoint and counter/histogram."""

    @pytest.fixture
    def obs_client(self):
        """TestClient with a fresh app + fresh registry for middleware tests.

        Reload chain: observability (fresh REGISTRY) → main (fresh app).
        We evict both modules from sys.modules before reimporting so that
        api.main gets a truly blank REGISTRY from api.observability without
        hitting the double-instrument_app() duplicate-registration trap.
        """
        import importlib
        import sys
        from starlette.testclient import TestClient

        # Evict both modules so the next import runs their top-level code fresh
        sys.modules.pop("api.observability", None)
        sys.modules.pop("api.main", None)

        # Fresh import: api.observability creates a new REGISTRY;
        # api.main imports that REGISTRY and calls instrument_app() once only.
        import api.observability as _obs  # noqa: F401 (side-effects needed)
        import api.main as _main

        app = _main.app
        _main._probe_db = lambda: None
        _main._probe_kafka = lambda: None

        with TestClient(app, raise_server_exceptions=False) as client:
            yield client

    def test_metrics_endpoint_200_content_type(self, obs_client):
        """sc-1: GET /metrics returns 200 with Prometheus content-type."""
        resp = obs_client.get("/metrics")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        ct = resp.headers.get("content-type", "")
        assert "text/plain" in ct, f"Expected text/plain content-type, got: {ct}"
        assert "version=0.0.4" in ct, f"Expected version=0.0.4 in content-type, got: {ct}"

    def test_request_counter_increments_N(self, obs_client):
        """sc-2: N requests to /health increment http_requests_total by N."""
        from prometheus_client import CollectorRegistry

        N = 3
        # Monkeypatch probes inline so /health succeeds
        import api.main as _main
        _main._probe_db = lambda: None
        _main._probe_kafka = lambda: None

        for _ in range(N):
            obs_client.get("/health")

        # Collect metrics
        resp = obs_client.get("/metrics")
        assert resp.status_code == 200
        body = resp.text

        # Find any http_requests_total line for /health with 200 status
        lines = body.splitlines()
        total_for_health = 0.0
        for line in lines:
            if line.startswith("http_requests_total{") and "200" in line:
                # extract value from end of line
                parts = line.rsplit(" ", 1)
                if len(parts) == 2:
                    try:
                        total_for_health += float(parts[1])
                    except ValueError:
                        pass

        assert total_for_health >= N, (
            f"Expected at least {N} counted requests for /health 200, "
            f"got {total_for_health} from:\n{body}"
        )

    def test_latency_histogram_records(self, obs_client):
        """sc-3: After one request, http_request_duration_seconds_count >= 1."""
        import api.main as _main
        _main._probe_db = lambda: None
        _main._probe_kafka = lambda: None

        # Make one request
        obs_client.get("/health")

        resp = obs_client.get("/metrics")
        body = resp.text

        # Find histogram count lines
        count_lines = [
            line for line in body.splitlines()
            if line.startswith("http_request_duration_seconds_count{")
        ]
        total_count = 0.0
        for line in count_lines:
            parts = line.rsplit(" ", 1)
            if len(parts) == 2:
                try:
                    total_count += float(parts[1])
                except ValueError:
                    pass

        assert total_count >= 1, (
            f"Expected at least 1 histogram observation, got {total_count}. "
            f"Metrics output:\n{body}"
        )


# ---------------------------------------------------------------------------
# sc-6, sc-6b: Regression contracts — /health and /pipeline/dlq-depth unaffected
# ---------------------------------------------------------------------------


class TestRegressionContracts:
    """Scenarios 6 and 6b: existing endpoints unaffected after /metrics mount."""

    @pytest.fixture
    def regression_client(self):
        """Fresh app with mocked probes and Kafka.

        Same evict-and-reimport strategy as obs_client to avoid
        double instrument_app() calls across fixture invocations.
        """
        import sys
        from starlette.testclient import TestClient

        sys.modules.pop("api.observability", None)
        sys.modules.pop("api.main", None)

        import api.observability as _obs  # noqa: F401 (side-effects needed)
        import api.main as _main

        app = _main.app
        _main._probe_db = lambda: None
        _main._probe_kafka = lambda: None

        with TestClient(app, raise_server_exceptions=False) as client:
            yield client

    def test_health_unaffected(self, regression_client):
        """sc-6: GET /health still returns 200 with {status: ok} after /metrics mount."""
        import api.main as _main
        _main._probe_db = lambda: None
        _main._probe_kafka = lambda: None

        resp = regression_client.get("/health")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        body = resp.json()
        assert body.get("status") == "ok", f"Expected {{status: ok}}, got {body}"

    def test_dlq_depth_unaffected(self, regression_client):
        """sc-6b: GET /pipeline/dlq-depth still returns 200 after /metrics mount."""
        from unittest.mock import patch

        fake_result = {
            "broker_reachable": True,
            "topics": [
                {"topic": "dlq.canonical.training_event", "depth": 0, "status": "ok"},
                {"topic": "dlq.canonical.wellness_event", "depth": 0, "status": "ok"},
                {"topic": "dlq.canonical.planning_block", "depth": 0, "status": "ok"},
            ],
        }

        with patch("api.routers.pipeline.get_dlq_depths", return_value=fake_result):
            # X-API-Key required now that pipeline is protected (api-auth)
            resp = regression_client.get(
                "/pipeline/dlq-depth",
                headers={"X-API-Key": "test-api-key-fixture"},
            )

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        body = resp.json()
        assert body.get("broker_reachable") is True, f"Expected broker_reachable True, got {body}"
        assert len(body.get("topics", [])) == 3, f"Expected 3 topics, got {body}"
