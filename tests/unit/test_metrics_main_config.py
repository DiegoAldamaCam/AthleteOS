"""Unit tests for MetricsJobConfig input validation (RISK F1 — DDL injection).

These tests run WITHOUT pyflink (import-safe by design). They verify that
MetricsJobConfig rejects values containing quote/newline/property-injection
characters before those values can be interpolated raw into tbl_env.execute_sql
(source_ddl f-string). An attacker-controlled value such as
  bootstrap_servers = "kafka:9092'\n  'extra.prop' = 'injected"
would inject arbitrary connector properties into the Flink Table DDL.

The validation is intentionally strict (allowlist, not blocklist) so the
rejection surface is predictable.
"""

from __future__ import annotations

import pytest

from jobs.metrics.main import MetricsJobConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GOOD = dict(
    bootstrap_servers="kafka:9092",
    schema_registry_url="http://schema-registry:8081",
    group_id="metrics-training-event",
    canonical_topic="canonical.training_event",
)


def _make(**overrides) -> MetricsJobConfig:
    kwargs = dict(_GOOD)
    kwargs.update(overrides)
    return MetricsJobConfig(**kwargs)


# ---------------------------------------------------------------------------
# Happy-path: valid values must construct without error
# ---------------------------------------------------------------------------


class TestMetricsJobConfigValid:
    def test_default_values_accepted(self):
        cfg = _make()
        assert cfg.bootstrap_servers == "kafka:9092"
        assert cfg.schema_registry_url == "http://schema-registry:8081"

    def test_host_port_list_accepted(self):
        cfg = _make(bootstrap_servers="broker1:9092,broker2:9092")
        assert cfg.bootstrap_servers == "broker1:9092,broker2:9092"

    def test_https_registry_accepted(self):
        cfg = _make(schema_registry_url="https://registry.example.com:8081")
        assert cfg.schema_registry_url == "https://registry.example.com:8081"

    def test_custom_group_id_accepted(self):
        cfg = _make(group_id="my-metrics-group-123")
        assert cfg.group_id == "my-metrics-group-123"

    def test_custom_canonical_topic_accepted(self):
        cfg = _make(canonical_topic="canonical.training_event.v2")
        assert cfg.canonical_topic == "canonical.training_event.v2"


# ---------------------------------------------------------------------------
# Injection rejection: quote chars
# ---------------------------------------------------------------------------


class TestMetricsJobConfigRejectsInjection:
    def test_single_quote_in_bootstrap_servers_rejected(self):
        # A single quote closes the SQL string literal -> injection.
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092'")

    def test_newline_in_bootstrap_servers_rejected(self):
        # Newline inside a DDL WITH property value -> new property line injection.
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092\nnew.prop=injected")

    def test_double_quote_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers='kafka:9092"')

    def test_single_quote_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081'extra=injected")

    def test_newline_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081\n'extra'='val'")

    def test_single_quote_in_group_id_rejected(self):
        with pytest.raises(ValueError, match="group_id"):
            _make(group_id="group-id'")

    def test_newline_in_group_id_rejected(self):
        with pytest.raises(ValueError, match="group_id"):
            _make(group_id="group\ninjected")

    def test_single_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic="canonical.training_event'injected")

    def test_carriage_return_rejected(self):
        # CR is equivalent to newline for multi-line injection.
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092\rinjected")

    def test_null_byte_rejected(self):
        # Null byte can confuse parsers.
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092\x00injected")
