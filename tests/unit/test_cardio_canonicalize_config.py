"""Unit tests for CardioCanonicalizeJobConfig input validation (RISK F4 — DDL injection).

These tests run WITHOUT pyflink (import-safe by design). They verify that
CardioCanonicalizeJobConfig rejects values containing quote/newline/property-injection
characters before those values can be interpolated raw into the Flink Table DDL
f-string. Mirrors test_metrics_main_config.py (F1 template).

Note: TRANSACTIONAL_ID_PREFIX is a hardcoded module constant (not env-var-sourced)
and is intentionally NOT validated.

Scenarios covered: sc-7 (injection in any field rejected),
sc-8 (clean values accepted / happy path).
"""

from __future__ import annotations

import pytest

from jobs.cardio_canonicalize.main import CardioCanonicalizeJobConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GOOD = dict(
    bootstrap_servers="kafka:9092",
    schema_registry_url="http://schema-registry:8081",
    canonical_topic="canonical.training_event",
)


def _make(**overrides) -> CardioCanonicalizeJobConfig:
    kwargs = dict(_GOOD)
    kwargs.update(overrides)
    return CardioCanonicalizeJobConfig(**kwargs)


# ---------------------------------------------------------------------------
# Happy-path: valid values must construct without error (sc-8)
# ---------------------------------------------------------------------------


class TestCardioCanonicalizeJobConfigValid:
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
        cfg = _make(canonical_topic="canonical.training_event.v2")
        assert cfg.canonical_topic == "canonical.training_event.v2"


# ---------------------------------------------------------------------------
# Injection rejection: sc-7 + additional forbidden chars per field
# Note: TRANSACTIONAL_ID_PREFIX is hardcoded — NOT validated (by design).
# ---------------------------------------------------------------------------


class TestCardioCanonicalizeJobConfigRejectsInjection:
    # sc-7: single quote in bootstrap_servers
    def test_single_quote_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092'")

    # sc-7: newline in schema_registry_url
    def test_newline_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081\n'extra'='injected'")

    # sc-7: double quote in canonical_topic
    def test_double_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic='canonical.training_event"injected')

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

    def test_single_quote_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081'extra=injected")

    def test_null_byte_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic="canonical.training_event\x00injected")

    def test_single_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic="canonical.training_event'injected")
