"""Unit tests for NutritionCanonicalizeJobConfig input validation (RISK F6 — DDL injection).

These tests run WITHOUT pyflink (import-safe by design). They verify that
NutritionCanonicalizeJobConfig rejects values containing quote/newline/property-injection
characters before those values can be interpolated raw into the Flink Table DDL
f-string. Mirrors test_canonicalize_config.py (F2 template / F1 authoritative pattern).

Scenarios covered: sc-11 (forbidden chars rejected on each field),
sc-12 (clean values accepted / happy path).
"""

from __future__ import annotations

import pytest

from jobs.nutrition_canonicalize.main import NutritionCanonicalizeJobConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GOOD = dict(
    bootstrap_servers="kafka:9092",
    schema_registry_url="http://schema-registry:8081",
    canonical_topic="canonical.wellness_event",
)


def _make(**overrides) -> NutritionCanonicalizeJobConfig:
    kwargs = dict(_GOOD)
    kwargs.update(overrides)
    return NutritionCanonicalizeJobConfig(**kwargs)


# ---------------------------------------------------------------------------
# Happy-path: valid values must construct without error (sc-12)
# ---------------------------------------------------------------------------


class TestNutritionCanonicalizeJobConfigValid:
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

    def test_canonical_topic_accepted(self):
        cfg = _make(canonical_topic="canonical.wellness_event.v2")
        assert cfg.canonical_topic == "canonical.wellness_event.v2"


# ---------------------------------------------------------------------------
# Injection rejection: sc-11 — forbidden chars on each validated field
# ---------------------------------------------------------------------------


class TestNutritionCanonicalizeJobConfigRejectsInjection:
    # bootstrap_servers rejections
    def test_single_quote_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092'")

    def test_double_quote_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers='kafka:9092"')

    def test_newline_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092\nnew.prop=injected")

    def test_carriage_return_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092\rinjected")

    def test_null_byte_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092\x00injected")

    # schema_registry_url rejections
    def test_single_quote_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081'extra=injected")

    def test_newline_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081\n'extra'='injected'")

    def test_null_byte_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081\x00injected")

    # canonical_topic rejections
    def test_single_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic="canonical.wellness_event'")

    def test_double_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic='canonical.wellness_event"injected')
