"""Unit tests for tools.dlq_quality.quality — scan() orchestration (strict TDD).

Covers: sc-6, sc-19, sc-20, sc-26, sc-33 (parts a and b).
No real Kafka connection — DLQConsumer is fully mocked.
"""

from __future__ import annotations

import importlib
import json
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_envelope(error_type=None, timestamp=None, original_topic="raw.strength"):
    """Build a minimal DLQEnvelope for testing."""
    from tools.dlq_replay.envelope import DLQEnvelope

    return DLQEnvelope(
        original_topic=original_topic,
        original_key=None,
        original_value=b"IGNORED",
        error_type=error_type,
        timestamp=timestamp,
    )


def _make_config(
    topics=None,
    reports=None,
    max_count=None,
    sample_count=3,
    from_timestamp_ms=None,
):
    """Build a QualityConfig without env var lookup."""
    from tools.dlq_quality.config import QualityConfig

    return QualityConfig(
        bootstrap_servers="localhost:9092",
        topics=topics or ["dlq.canonical.training_event"],
        reports=reports or frozenset({"error-type", "age", "triage"}),
        fmt="table",
        from_timestamp_ms=from_timestamp_ms,
        max_count=max_count,
        sample_count=sample_count,
    )


def _raw_envelope_bytes(error_type="VALIDATION_FAILURE", timestamp=1_000_000):
    """Produce raw bytes that decode() can parse into a DLQEnvelope."""
    import base64
    import json as _json

    return _json.dumps({
        "original_topic": "raw.strength",
        "original_key": None,
        "original_value": base64.b64encode(b"payload").decode(),
        "error_type": error_type,
        "timestamp": timestamp,
    }).encode()


# ---------------------------------------------------------------------------
# sc-6: now_ms snapshotted ONCE before the consumer loop
# ---------------------------------------------------------------------------

def test_now_ms_snapshotted_once_before_loop():
    """sc-6: all age_agg.add() calls receive the same now_ms value, captured before polling."""
    import time
    from tools.dlq_quality.quality import scan
    from tools.dlq_replay.envelope import decode

    fixed_now_ms = 5_000_000_000
    raw_bytes = _raw_envelope_bytes(timestamp=1_000_000)

    # Mock consumer that yields 3 messages
    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (raw_bytes, "dlq.canonical.training_event", 0, 0),
        (raw_bytes, "dlq.canonical.training_event", 0, 1),
        (raw_bytes, "dlq.canonical.training_event", 0, 2),
    ])

    now_ms_calls = []

    def mock_time_time():
        # Simulate wall clock advancing on each call
        now_ms_calls.append(len(now_ms_calls))
        return fixed_now_ms / 1000 + len(now_ms_calls) * 60  # advances 60s per call

    cfg = _make_config(reports=frozenset({"age"}))

    with patch("tools.dlq_quality.quality.DLQConsumer", return_value=mock_consumer), \
         patch("time.time", side_effect=mock_time_time):
        result = scan(cfg)

    # time.time() was called exactly once (for the snapshot)
    assert len(now_ms_calls) == 1, (
        f"time.time() called {len(now_ms_calls)} times; expected exactly 1 (snapshot before loop)"
    )
    # All 3 messages were scanned
    assert result.scanned == 3


# ---------------------------------------------------------------------------
# sc-19: single consumer pass, consumer constructed exactly once
# ---------------------------------------------------------------------------

def test_consumer_constructed_once_for_all_reports():
    """sc-19: --report all → DLQConsumer() called exactly once."""
    from tools.dlq_quality.quality import scan

    raw_bytes = _raw_envelope_bytes()
    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (raw_bytes, "dlq.canonical.training_event", 0, 0),
    ])

    cfg = _make_config(reports=frozenset({"error-type", "age", "triage"}))

    with patch("tools.dlq_quality.quality.DLQConsumer") as MockConsumer:
        MockConsumer.return_value = mock_consumer
        scan(cfg)

    assert MockConsumer.call_count == 1, (
        f"DLQConsumer() called {MockConsumer.call_count} times; expected exactly 1"
    )


# ---------------------------------------------------------------------------
# sc-20: --report age only feeds AgeAgg, not ErrorTypeAgg or TriageAgg
# ---------------------------------------------------------------------------

def test_report_age_only_feeds_age_agg():
    """sc-20: --report age → only age section populated in QualityResult."""
    from tools.dlq_quality.quality import scan

    raw_bytes = _raw_envelope_bytes(timestamp=1_000_000)
    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (raw_bytes, "dlq.canonical.training_event", 0, 0),
    ])

    cfg = _make_config(reports=frozenset({"age"}))

    with patch("tools.dlq_quality.quality.DLQConsumer", return_value=mock_consumer), \
         patch("time.time", return_value=5_000_000):
        result = scan(cfg)

    # Age data populated (tightened: removed always-true `or scanned >= 0` clause)
    assert len(result.age) > 0, f"age section should be populated; got {result.age}"
    # error-type and triage must be empty (not selected)
    assert result.error_type == {}, f"error_type should be empty for --report age; got {result.error_type}"
    assert result.triage_fix == {}, f"triage_fix should be empty for --report age; got {result.triage_fix}"


def test_report_error_type_only_feeds_error_type_agg():
    """sc-20: --report error-type → only error_type section populated."""
    from tools.dlq_quality.quality import scan

    raw_bytes = _raw_envelope_bytes()
    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (raw_bytes, "dlq.canonical.training_event", 0, 0),
    ])

    cfg = _make_config(reports=frozenset({"error-type"}))

    with patch("tools.dlq_quality.quality.DLQConsumer", return_value=mock_consumer), \
         patch("time.time", return_value=5_000_000):
        result = scan(cfg)

    assert len(result.error_type) > 0
    assert result.age == {}, f"age should be empty for --report error-type"
    assert result.triage_fix == {}, f"triage should be empty for --report error-type"


# ---------------------------------------------------------------------------
# sc-26: max_count enforced in quality loop (consumer does NOT enforce it)
# ---------------------------------------------------------------------------

def test_max_count_stops_scan_after_n_messages():
    """sc-26: mock consumer yields 20 messages; max_count=10 → scan stops at 10."""
    from tools.dlq_quality.quality import scan

    raw_bytes = _raw_envelope_bytes()
    # Consumer yields 20 messages
    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (raw_bytes, "dlq.canonical.training_event", 0, i)
        for i in range(20)
    ])

    cfg = _make_config(max_count=10)

    with patch("tools.dlq_quality.quality.DLQConsumer", return_value=mock_consumer), \
         patch("time.time", return_value=5_000_000):
        result = scan(cfg)

    assert result.scanned == 10, (
        f"Expected 10 messages scanned with max_count=10, got {result.scanned}"
    )


# ---------------------------------------------------------------------------
# CorruptEnvelope handling — scan continues, corrupt counter incremented
# ---------------------------------------------------------------------------

def test_corrupt_envelope_increments_corrupt_counter():
    """CorruptEnvelope on 2nd message → result.corrupt == 1, scan continues."""
    from tools.dlq_quality.quality import scan
    from tools.dlq_replay.envelope import CorruptEnvelope

    good_raw = _raw_envelope_bytes()
    bad_raw = b"NOT_VALID_JSON"

    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (good_raw, "dlq.canonical.training_event", 0, 0),
        (bad_raw, "dlq.canonical.training_event", 0, 1),
        (good_raw, "dlq.canonical.training_event", 0, 2),
    ])

    cfg = _make_config()

    with patch("tools.dlq_quality.quality.DLQConsumer", return_value=mock_consumer), \
         patch("time.time", return_value=5_000_000):
        result = scan(cfg)

    assert result.corrupt == 1, f"Expected 1 corrupt message, got {result.corrupt}"
    assert result.scanned == 3, f"Expected 3 total scanned, got {result.scanned}"


# ---------------------------------------------------------------------------
# sc-33 part a: import-inspection — 'Producer' not in module dir/source
# ---------------------------------------------------------------------------

def test_quality_module_has_no_producer_in_dir():
    """sc-33a: import tools.dlq_quality.quality → 'Producer' not in dir(module)."""
    import tools.dlq_quality.quality as quality_module

    # Ensure the module is fully loaded
    importlib.reload(quality_module)
    names = dir(quality_module)
    assert "Producer" not in names, (
        f"'Producer' found in dir(tools.dlq_quality.quality): {[n for n in names if 'roduc' in n]}"
    )


def test_quality_module_source_has_no_producer_import():
    """sc-33a: module source contains no Producer import statement."""
    import inspect
    import tools.dlq_quality.quality as quality_module

    source = inspect.getsource(quality_module)
    # Check for import patterns only — not docstring mentions of the word.
    # Acceptable: comments/docstrings that say "never imports Producer".
    # Forbidden: any actual import line that pulls in Producer.
    import_lines = [
        line for line in source.splitlines()
        if "import" in line and "Producer" in line
    ]
    assert len(import_lines) == 0, (
        f"Found Producer import in tools.dlq_quality.quality: {import_lines}"
    )


# ---------------------------------------------------------------------------
# sc-33 part b: monkeypatch — if Producer is ever constructed it raises; scan still completes
# ---------------------------------------------------------------------------

def test_quality_scan_completes_even_if_producer_raises(monkeypatch):
    """sc-33b: Producer patched to raise on instantiation → scan still completes normally."""
    import confluent_kafka
    from tools.dlq_quality.quality import scan

    def _raise_on_construct(*args, **kwargs):
        raise RuntimeError("Producer instantiation is FORBIDDEN in dlq-quality")

    monkeypatch.setattr(confluent_kafka, "Producer", _raise_on_construct)

    raw_bytes = _raw_envelope_bytes()
    mock_consumer = MagicMock()
    mock_consumer.iter_messages.return_value = iter([
        (raw_bytes, "dlq.canonical.training_event", 0, 0),
    ])

    cfg = _make_config()

    # Should complete without raising (Producer never touched)
    with patch("tools.dlq_quality.quality.DLQConsumer", return_value=mock_consumer), \
         patch("time.time", return_value=5_000_000):
        result = scan(cfg)

    assert result.scanned == 1
