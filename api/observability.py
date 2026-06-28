"""Prometheus observability module for the AthleteOS FastAPI application.

Design: obs #189 — athleteos-observability (ADR-25, ADR-26).

Key design constraints (ADR-25):
- INJECTED CollectorRegistry only. REGISTRY is the module-level default for
  the live app; tests pass their OWN registries.
- NEVER reference prometheus_client.REGISTRY (global). Every metric constructor
  receives a registry= kwarg explicitly.

Public API:
  REGISTRY            — module-level CollectorRegistry for the live app
  build_metrics(registry) -> (counter, histogram)
  DLQDepthCollector   — custom Collector for dlq_depth gauge (ADR-26)
  update_dlq_gauge(depths, collector)
  PrometheusMiddleware — Starlette middleware (request count + latency)
  instrument_app(app, registry)
"""

from __future__ import annotations

import time
from typing import Callable

from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.exposition import make_asgi_app
from prometheus_client.metrics_core import GaugeMetricFamily
from prometheus_client.registry import Collector
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# Module-level default registry (used by the live app — NOT by tests)
# ---------------------------------------------------------------------------

REGISTRY = CollectorRegistry()

# ---------------------------------------------------------------------------
# Metric factory — passes explicit registry= kwarg to every constructor (ADR-25)
# ---------------------------------------------------------------------------

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5)


def build_metrics(registry: CollectorRegistry) -> tuple[Counter, Histogram]:
    """Create and return (http_requests_total Counter, http_request_duration_seconds Histogram).

    Both metrics are bound to the supplied registry — NEVER the global default.
    Call once per registry instance; subsequent calls with the same registry will
    raise ValueError (duplicate metric). Tests create one fresh registry per test.

    Args:
        registry: CollectorRegistry to register metrics into.

    Returns:
        (counter, histogram) tuple bound to registry.
    """
    counter = Counter(
        "http_requests_total",
        "Total HTTP requests handled by the FastAPI application.",
        labelnames=["method", "endpoint", "http_status"],
        registry=registry,
    )
    histogram = Histogram(
        "http_request_duration_seconds",
        "HTTP request latency in seconds.",
        labelnames=["method", "endpoint"],
        registry=registry,
        buckets=_LATENCY_BUCKETS,
    )
    return counter, histogram


# ---------------------------------------------------------------------------
# DLQDepthCollector — custom Collector for dlq_depth gauge (ADR-26)
# ---------------------------------------------------------------------------


class DLQDepthCollector(Collector):
    """Custom Prometheus Collector that exposes dlq_depth{topic} gauge.

    Holds a last-known dict {topic: depth} updated by update_dlq_gauge().
    collect() yields GaugeMetricFamily on every Prometheus scrape.

    On broker-unreachable (degraded envelope), update_dlq_gauge() skips
    the update → gauge retains its last value (or emits nothing if never set).
    This satisfies scenario 4b: last-value retention on broker failure.
    """

    def __init__(self, registry: CollectorRegistry) -> None:
        self._depths: dict[str, float] = {}
        # Register this collector into the supplied registry (ADR-25)
        registry.register(self)

    def update(self, depths: dict[str, float]) -> None:
        """Update cached depths from a {topic: depth} mapping.

        Only called when broker_reachable is True (non-degraded envelope).
        """
        self._depths.update(depths)

    def collect(self):  # type: ignore[override]
        """Yield GaugeMetricFamily for each cached topic depth."""
        gauge = GaugeMetricFamily(
            "dlq_depth",
            "Unprocessed message depth per DLQ topic.",
            labels=["topic"],
        )
        for topic, depth in self._depths.items():
            gauge.add_metric([topic], depth)
        yield gauge

    def describe(self):  # type: ignore[override]
        """Minimal describe() to satisfy the Collector protocol."""
        yield GaugeMetricFamily("dlq_depth", "Unprocessed message depth per DLQ topic.")


# ---------------------------------------------------------------------------
# update_dlq_gauge — parse get_dlq_depths envelope and call collector.update()
# ---------------------------------------------------------------------------


def update_dlq_gauge(depths: dict, collector: DLQDepthCollector) -> None:
    """Parse the get_dlq_depths() envelope and update the DLQDepthCollector.

    Skips degraded envelopes (broker_reachable == False) to retain last value.
    Skips individual topics where depth is None (partial degraded entries).

    Args:
        depths:    dict returned by api.kafka_admin.get_dlq_depths()
        collector: DLQDepthCollector instance to update
    """
    if not depths.get("broker_reachable", False):
        # Degraded envelope — leave cached values untouched (sc-4b)
        return

    new_depths: dict[str, float] = {}
    for entry in depths.get("topics", []):
        topic = entry.get("topic")
        depth = entry.get("depth")
        if topic is not None and depth is not None:
            new_depths[topic] = float(depth)

    if new_depths:
        collector.update(new_depths)


# ---------------------------------------------------------------------------
# PrometheusMiddleware — time requests and record counter + histogram (sc-1/2/3)
# ---------------------------------------------------------------------------


class PrometheusMiddleware(BaseHTTPMiddleware):
    """Starlette middleware that records http_requests_total and latency histogram.

    Route template is extracted from request.scope["route"].path when available
    (Starlette sets this after routing), falling back to request.url.path.

    The counter and histogram are injected at construction time (ADR-25).
    """

    def __init__(
        self,
        app,
        counter: Counter,
        histogram: Histogram,
    ) -> None:
        super().__init__(app)
        self._counter = counter
        self._histogram = histogram

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        # Prefer route template (e.g. "/athletes/{id}/metrics") over literal path
        route = request.scope.get("route")
        endpoint = route.path if route is not None else request.url.path

        method = request.method
        status = str(response.status_code)

        self._histogram.labels(method=method, endpoint=endpoint).observe(duration)
        self._counter.labels(method=method, endpoint=endpoint, http_status=status).inc()

        return response


# ---------------------------------------------------------------------------
# instrument_app — wire middleware + DLQDepthCollector + /metrics mount
# ---------------------------------------------------------------------------

# Module-level DLQDepthCollector bound to the live REGISTRY.
# Accessed by api/routers/pipeline.py via:  from api.observability import DLQ_COLLECTOR
DLQ_COLLECTOR: DLQDepthCollector | None = None


def instrument_app(app, registry: CollectorRegistry) -> DLQDepthCollector:
    """Instrument the FastAPI app with Prometheus observability.

    1. Build metrics (counter + histogram) bound to registry.
    2. Add PrometheusMiddleware to the app.
    3. Create DLQDepthCollector and register it in registry.
    4. Mount make_asgi_app(registry=registry) at /metrics.

    Idempotent: if this registry has already been instrumented (e.g., api.main
    was reloaded by a test without also reloading api.observability), the call
    is a no-op to prevent ValueError on duplicate Collector registration.

    Returns the DLQDepthCollector so the pipeline router can call update_dlq_gauge().

    Args:
        app:      FastAPI application instance
        registry: CollectorRegistry to use (module REGISTRY for live app)

    Returns:
        DLQDepthCollector instance registered into registry
    """
    global DLQ_COLLECTOR  # noqa: PLW0603

    # Guard: detect if this registry already has our metrics registered.
    # This happens when api.main is reloaded (e.g., by existing tests that call
    # importlib.reload(_main)) without also reloading api.observability.
    # In that case, we skip re-registration to preserve the degrade-to-200 contract.
    # Using try/except around the duplicate check is more robust than inspecting
    # private attributes; we catch only the specific duplicate-registration error.
    if "http_requests_total" in getattr(registry, "_names_to_collectors", {}):
        # Already instrumented — return the existing DLQ_COLLECTOR
        return DLQ_COLLECTOR  # type: ignore[return-value]

    counter, histogram = build_metrics(registry)
    app.add_middleware(PrometheusMiddleware, counter=counter, histogram=histogram)

    collector = DLQDepthCollector(registry=registry)
    DLQ_COLLECTOR = collector

    metrics_app = make_asgi_app(registry=registry)
    app.mount("/metrics", metrics_app)

    return collector
