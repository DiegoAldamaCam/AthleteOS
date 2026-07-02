"""Unit tests: main() reads METRICS_PG_DSN and METRICS_CHECKPOINT_DIR from env.

Strict TDD — these tests are written BEFORE the implementation.
They verify sc-11 (pg_dsn absent → None) and R1/R4 (env reads).

Tests are pure and import-safe: no pyflink required. We stub out `run()`
so `main()` never reaches the streaming job execution code.
"""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers to call main() safely (stub run so no pyflink execution happens)
# ---------------------------------------------------------------------------


def _call_main_with_env(monkeypatch, env: dict) -> "MetricsJobConfig":  # type: ignore[name-defined]
    """Call jobs.metrics.main.main() with a controlled env, capture the config.

    Patches run() to a no-op that records the config it receives.
    Returns the MetricsJobConfig passed to run().
    """
    # Clear env vars that might bleed in from conftest or shell.
    for key in ("METRICS_PG_DSN", "METRICS_CHECKPOINT_DIR", "METRICS_GROUP_ID",
                "KAFKA_BOOTSTRAP_SERVERS", "SCHEMA_REGISTRY_URL"):
        monkeypatch.delenv(key, raising=False)

    for key, value in env.items():
        monkeypatch.setenv(key, value)

    captured: list = []

    from jobs.metrics.main import main as metrics_main, MetricsJobConfig  # noqa: PLC0415

    def fake_run(config: "MetricsJobConfig") -> None:  # type: ignore[name-defined]
        captured.append(config)

    with patch("jobs.metrics.main.run", side_effect=fake_run):
        metrics_main()

    assert len(captured) == 1, "run() was not called exactly once"
    return captured[0]


# ---------------------------------------------------------------------------
# sc-11 / R1: pg_dsn from environment
# ---------------------------------------------------------------------------


class TestPgDsnFromEnv:
    def test_pg_dsn_read_from_env(self, monkeypatch):
        """R1: When METRICS_PG_DSN is set, MetricsJobConfig.pg_dsn reflects it."""
        config = _call_main_with_env(monkeypatch, {"METRICS_PG_DSN": "postgresql://x:5432/db"})
        assert config.pg_dsn == "postgresql://x:5432/db"

    def test_pg_dsn_absent_is_none(self, monkeypatch):
        """sc-11: When METRICS_PG_DSN is absent, pg_dsn defaults to None (no PG sink)."""
        config = _call_main_with_env(monkeypatch, {})
        assert config.pg_dsn is None


# ---------------------------------------------------------------------------
# R4: checkpoint_dir from environment
# ---------------------------------------------------------------------------


class TestCheckpointDirFromEnv:
    def test_checkpoint_dir_read_from_env(self, monkeypatch):
        """R4: When METRICS_CHECKPOINT_DIR is set, MetricsJobConfig.checkpoint_dir reflects it."""
        config = _call_main_with_env(
            monkeypatch, {"METRICS_CHECKPOINT_DIR": "file:///flink-checkpoints"}
        )
        assert config.checkpoint_dir == "file:///flink-checkpoints"

    def test_checkpoint_dir_absent_uses_default(self, monkeypatch):
        """R4: When METRICS_CHECKPOINT_DIR is absent, checkpoint_dir uses the code default."""
        config = _call_main_with_env(monkeypatch, {})
        # The code default is file:///tmp/athleteos-metrics-checkpoints (main.py:280).
        assert config.checkpoint_dir == "file:///tmp/athleteos-metrics-checkpoints"
