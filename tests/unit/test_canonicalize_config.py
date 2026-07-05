"""Unit tests for CanonicalizeJobConfig input validation (RISK F2 — DDL injection).

These tests run WITHOUT pyflink (import-safe by design). They verify that
CanonicalizeJobConfig rejects values containing quote/newline/property-injection
characters before those values can be interpolated raw into the Flink Table DDL
f-string. Mirrors test_metrics_main_config.py (F1 template).

Scenarios covered: sc-1 (bootstrap_servers single-quote reject),
sc-2 (schema_registry_url newline reject), sc-3 (canonical_topic single-quote reject),
sc-4 (clean values accepted / happy path).
"""

from __future__ import annotations

import pytest

from jobs.canonicalize.main import CanonicalizeJobConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GOOD = dict(
    bootstrap_servers="kafka:9092",
    schema_registry_url="http://schema-registry:8081",
    canonical_topic="canonical.training_event",
)


def _make(**overrides) -> CanonicalizeJobConfig:
    kwargs = dict(_GOOD)
    kwargs.update(overrides)
    return CanonicalizeJobConfig(**kwargs)


# ---------------------------------------------------------------------------
# Happy-path: valid values must construct without error (sc-4)
# ---------------------------------------------------------------------------


class TestCanonicalizeJobConfigValid:
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
# Injection rejection: sc-1, sc-2, sc-3 + additional forbidden chars
# ---------------------------------------------------------------------------


class TestCanonicalizeJobConfigRejectsInjection:
    # sc-1: single quote in bootstrap_servers
    def test_single_quote_in_bootstrap_servers_rejected(self):
        with pytest.raises(ValueError, match="bootstrap_servers"):
            _make(bootstrap_servers="kafka:9092'")

    # sc-2: newline in schema_registry_url
    def test_newline_in_schema_registry_url_rejected(self):
        with pytest.raises(ValueError, match="schema_registry_url"):
            _make(schema_registry_url="http://registry:8081\n'extra'='injected'")

    # sc-3: single quote in canonical_topic
    def test_single_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic="canonical.training_event'")

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

    def test_double_quote_in_canonical_topic_rejected(self):
        with pytest.raises(ValueError, match="canonical_topic"):
            _make(canonical_topic='canonical.training_event"injected')


# ---------------------------------------------------------------------------
# Regression guard: exactly-once Kafka sink transaction timeout (G4 DEFECT-2)
# ---------------------------------------------------------------------------


class TestSinkTransactionTimeout:
    """Lock the producer transaction timeout on the EXACTLY_ONCE canonical sink.

    The Flink Kafka connector default is 3_600_000 ms (1 h), which exceeds the
    broker's transaction.max.timeout.ms=900_000 and causes an InitProducerId
    rejection -> sink crash-loop (G4 DEFECT-2, commit cd1f6e5). This guards
    against anyone removing or raising the override past the broker maximum.

    Source-level assertion (not execution): the DDL is an inline f-string inside
    a pyflink-dependent function, so we assert on the module source rather than
    standing up a TableEnvironment.
    """

    _BROKER_MAX_TXN_TIMEOUT_MS = 900_000

    def _sink_ddl_source(self) -> str:
        import inspect

        import jobs.canonicalize.main as canon

        return inspect.getsource(canon)

    def test_transaction_timeout_property_present(self):
        src = self._sink_ddl_source()
        assert "'properties.transaction.timeout.ms'" in src, (
            "canonical sink DDL is missing the transaction.timeout.ms override — "
            "the connector default (3_600_000) exceeds the broker max and will "
            "crash-loop the sink (G4 DEFECT-2)"
        )

    def test_transaction_timeout_under_broker_max(self):
        import re

        src = self._sink_ddl_source()
        match = re.search(
            r"'properties\.transaction\.timeout\.ms'\s*=\s*'(\d+)'", src
        )
        assert match is not None, "could not find the transaction.timeout.ms value in the DDL"
        timeout_ms = int(match.group(1))
        assert timeout_ms < self._BROKER_MAX_TXN_TIMEOUT_MS, (
            f"transaction.timeout.ms={timeout_ms} must stay below the broker "
            f"transaction.max.timeout.ms={self._BROKER_MAX_TXN_TIMEOUT_MS} "
            "(InitProducerId rejection / sink crash-loop otherwise)"
        )
