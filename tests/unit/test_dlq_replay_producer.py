"""Unit tests for tools.dlq_replay.producer (strict TDD — RED phase first)."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from tools.dlq_replay.producer import DLQProducer
from tools.dlq_replay.report import ReplayReport


def _make_mock_confluent_producer():
    """Return a mock that stands in for confluent_kafka.Producer."""
    mock_cls = MagicMock()
    mock_instance = MagicMock()
    mock_cls.return_value = mock_instance
    return mock_cls, mock_instance


# sc-1, sc-2, sc-22: dry_run=True → confluent_kafka Producer.produce never called
def test_produce_dry_run_does_not_call_kafka(monkeypatch):
    mock_cls, mock_instance = _make_mock_confluent_producer()
    with patch("tools.dlq_replay.producer.ConfluentProducer", mock_cls):
        report = ReplayReport()
        producer = DLQProducer(bootstrap_servers="localhost:9092")
        producer.produce(
            topic="raw.strength",
            key="A1",
            value=b"data",
            report=report,
            dry_run=True,
        )
    mock_instance.produce.assert_not_called()


# sc-22: dry_run increments dry_run_would_replay
def test_produce_dry_run_increments_would_replay_counter(monkeypatch):
    mock_cls, mock_instance = _make_mock_confluent_producer()
    with patch("tools.dlq_replay.producer.ConfluentProducer", mock_cls):
        report = ReplayReport()
        producer = DLQProducer(bootstrap_servers="localhost:9092")
        producer.produce(topic="raw.strength", key="A1", value=b"data", report=report, dry_run=True)
        producer.produce(topic="raw.strength", key="A2", value=b"data2", report=report, dry_run=True)
    assert report.dry_run_would_replay == 2
    assert report.replayed == 0


# sc-3: dry_run=False → Producer.produce called with correct topic/key/value
def test_produce_no_dry_run_calls_kafka_produce(monkeypatch):
    mock_cls, mock_instance = _make_mock_confluent_producer()
    with patch("tools.dlq_replay.producer.ConfluentProducer", mock_cls):
        report = ReplayReport()
        producer = DLQProducer(bootstrap_servers="localhost:9092")
        producer.produce(
            topic="raw.strength",
            key="A1",
            value=b'{"event_id":"e1"}',
            report=report,
            dry_run=False,
        )
    mock_instance.produce.assert_called_once_with(
        topic="raw.strength",
        key="A1",
        value=b'{"event_id":"e1"}',
    )


# sc-3: flush() is called after produce
def test_flush_called_after_produce(monkeypatch):
    mock_cls, mock_instance = _make_mock_confluent_producer()
    with patch("tools.dlq_replay.producer.ConfluentProducer", mock_cls):
        report = ReplayReport()
        producer = DLQProducer(bootstrap_servers="localhost:9092")
        producer.produce(topic="raw.strength", key="A1", value=b"d", report=report, dry_run=False)
        producer.flush()
    mock_instance.flush.assert_called()


# sc-3: replayed counter incremented on non-dry-run
def test_produce_no_dry_run_increments_replayed_counter(monkeypatch):
    mock_cls, mock_instance = _make_mock_confluent_producer()
    with patch("tools.dlq_replay.producer.ConfluentProducer", mock_cls):
        report = ReplayReport()
        producer = DLQProducer(bootstrap_servers="localhost:9092")
        producer.produce(topic="raw.strength", key="A1", value=b"d", report=report, dry_run=False)
    assert report.replayed == 1
    assert report.dry_run_would_replay == 0


# sc-16: null key → produce called with key=None
def test_produce_null_key_passes_none_to_kafka(monkeypatch):
    mock_cls, mock_instance = _make_mock_confluent_producer()
    with patch("tools.dlq_replay.producer.ConfluentProducer", mock_cls):
        report = ReplayReport()
        producer = DLQProducer(bootstrap_servers="localhost:9092")
        producer.produce(
            topic="raw.strength",
            key=None,
            value=b"data",
            report=report,
            dry_run=False,
        )
    mock_instance.produce.assert_called_once_with(
        topic="raw.strength",
        key=None,
        value=b"data",
    )
