"""Unit tests for WellnessMetricsJobConfig input validation (RISK F8 — DDL injection).

These tests run WITHOUT pyflink (import-safe by design). They verify that
WellnessMetricsJobConfig rejects values containing quote/newline/property-injection
characters before those values can be interpolated raw into the Flink Table DDL
f-string (_ddl_source). Mirrors test_canonicalize_config.py (F1/F2 template).

Key differences from F2-F7 tests:
- WellnessMetricsJobConfig is a @dataclass; validation is in __post_init__ (not __init__)
- 4 fields validated (not 3): includes group_id (interpolated as properties.group.id)
- Field name is kafka_bootstrap_servers (NOT bootstrap_servers)
- Class name prefix in ValueError: WellnessMetricsJobConfig.

Scenarios covered:
  sc-15: kafka_bootstrap_servers single-quote reject
  sc-16: schema_registry_url newline reject
  sc-17: group_id single-quote reject (F8-only — group_id IS interpolated in F8 source DDL)
  sc-18: canonical_topic null-byte reject
  sc-19: clean values accepted / happy path (all four fields)
"""

from __future__ import annotations

import pytest

from jobs.wellness_metrics.main import WellnessMetricsJobConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GOOD = dict(
    kafka_bootstrap_servers="kafka:9092",
    schema_registry_url="http://schema-registry:8081",
    canonical_topic="canonical.wellness_event",
    group_id="wellness-metrics-job",
)


def _make(**overrides) -> WellnessMetricsJobConfig:
    kwargs = dict(_GOOD)
    kwargs.update(overrides)
    return WellnessMetricsJobConfig(**kwargs)


# ---------------------------------------------------------------------------
# Happy-path: valid values must construct without error (sc-19)
# ---------------------------------------------------------------------------


class TestWellnessMetricsJobConfigValid:
    def test_default_values_accepted(self):
        cfg = _make()
        assert cfg.kafka_bootstrap_servers == "kafka:9092"
        assert cfg.schema_registry_url == "http://schema-registry:8081"

    def test_host_port_list_accepted(self):
        cfg = _make(kafka_bootstrap_servers="broker1:9092,broker2:9092")
        assert cfg.kafka_bootstrap_servers == "broker1:9092,broker2:9092"

    def test_https_registry_accepted(self):
        cfg = _make(schema_registry_url="https://registry.example.com:8081")
        assert cfg.schema_registry_url == "https://registry.example.com:8081"

    def test_canonical_topic_accepted(self):
        cfg = _make(canonical_topic="canonical.wellness_event.v2")
        assert cfg.canonical_topic == "canonical.wellness_event.v2"


# ---------------------------------------------------------------------------
# Injection rejection: sc-15, sc-16, sc-17, sc-18 + additional forbidden chars
# ---------------------------------------------------------------------------


class TestWellnessMetricsJobConfigRejectsInjection:
    # sc-15: single quote in kafka_bootstrap_servers
    def test_single_quote_in_kafka_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="kafka_bootstrap_servers"):
            _make(kafka_bootstrap_servers="kafka:9092'")

    # sc-16: newline in schema_registry_url
    def test_newline_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081\n'extra'='injected'")

    # sc-17: single quote in group_id (F8-only — group_id IS interpolated)
    def test_single_quote_in_group_id_rejected(self):
        with pytest.raises(ValueError, match="group_id"):
            _make(group_id="wellness-metrics-job'")

    # sc-18: null byte in canonical_topic
    def test_null_byte_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic="canonical.wellness_event\x00injected")

    def test_double_quote_in_kafka_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="kafka_bootstrap_servers"):
            _make(kafka_bootstrap_servers='kafka:9092"')

    def test_newline_in_kafka_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="kafka_bootstrap_servers"):
            _make(kafka_bootstrap_servers="kafka:9092\nnew.prop=injected")

    def test_carriage_return_in_kafka_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="kafka_bootstrap_servers"):
            _make(kafka_bootstrap_servers="kafka:9092\rinjected")

    def test_null_byte_in_kafka_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="kafka_bootstrap_servers"):
            _make(kafka_bootstrap_servers="kafka:9092\x00injected")

    def test_single_quote_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081'extra=injected")

    def test_double_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic='canonical.wellness_event"injected')
