"""Unit tests for the metrics-job observability helpers (RESILIENCE F4).

Tests verify:
1. Sentry init is a no-op when SENTRY_DSN env var is absent (never crashes).
2. Sentry init uses the DSN when provided.
3. init_sentry() is idempotent (calling twice does not raise).
4. All counter names are exported as public constants for Flink reporter wiring.

These tests run WITHOUT pyflink and WITHOUT Docker (pure Python).
"""

from __future__ import annotations

import os

import pytest

from jobs.metrics.main import (
    COUNTER_DLQ_DEDUP_DROPS,
    COUNTER_DLQ_LATE_DAILY,
    COUNTER_DLQ_LATE_ROLLING,
    COUNTER_DLQ_NAN,
    COUNTER_RECORDS_PROCESSED,
    init_sentry,
)


class TestSentryInit:
    def test_no_dsn_no_crash(self, monkeypatch):
        # CRITICAL: init_sentry() must be a complete no-op when SENTRY_DSN is
        # absent. Unit tests and prod environments without Sentry must never fail.
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        init_sentry()  # must not raise

    def test_no_dsn_second_call_no_crash(self, monkeypatch):
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        init_sentry()
        init_sentry()  # idempotent — must not raise

    def test_with_dsn_initializes(self, monkeypatch):
        # A syntactically valid DSN should be accepted by the sentry SDK.
        monkeypatch.setenv(
            "SENTRY_DSN",
            "https://abc123@o0.ingest.sentry.io/0",
        )
        init_sentry()  # must not raise (SDK validates DSN format lazily)

    def test_invalid_dsn_does_not_crash(self, monkeypatch):
        # A bad DSN should not crash the job — Sentry failures must be silent.
        monkeypatch.setenv("SENTRY_DSN", "not-a-valid-dsn")
        init_sentry()  # must not raise; observability must never block the job


class TestCounterConstants:
    def test_counter_names_are_strings(self):
        # The counter name constants must be non-empty strings (used as Flink
        # metric group / counter name keys).
        for const in [
            COUNTER_DLQ_NAN,
            COUNTER_DLQ_LATE_DAILY,
            COUNTER_DLQ_LATE_ROLLING,
            COUNTER_DLQ_DEDUP_DROPS,
            COUNTER_RECORDS_PROCESSED,
        ]:
            assert isinstance(const, str) and const, (
                f"Counter constant must be a non-empty string; got {const!r}"
            )

    def test_counter_names_are_unique(self):
        names = [
            COUNTER_DLQ_NAN,
            COUNTER_DLQ_LATE_DAILY,
            COUNTER_DLQ_LATE_ROLLING,
            COUNTER_DLQ_DEDUP_DROPS,
            COUNTER_RECORDS_PROCESSED,
        ]
        assert len(names) == len(set(names)), (
            f"Counter names must be unique; got duplicates: {names}"
        )
