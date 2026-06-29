"""Unit tests for tools.dlq_replay.config (strict TDD — RED phase first)."""

from __future__ import annotations

import argparse

import pytest

from tools.dlq_replay.config import ReplayConfig, build_parser


# sc-23: KAFKA_BOOTSTRAP_SERVERS missing → SystemExit(1)
def test_missing_bootstrap_servers_raises_systemexit(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    parser = build_parser()
    args = parser.parse_args(["--topic", "dlq.canonical.training_event"])
    with pytest.raises(SystemExit) as exc_info:
        ReplayConfig.from_args_and_env(args, env={})
    assert exc_info.value.code == 1


# sc-5: --topic dlq.canonical.training_event → single topic list
def test_single_topic_resolves_correctly(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    parser = build_parser()
    args = parser.parse_args(["--topic", "dlq.canonical.training_event"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert cfg.topics == ["dlq.canonical.training_event"]
    assert cfg.bootstrap_servers == "localhost:9092"


# sc-6: --topic all → all 3 DLQ topics
def test_topic_all_resolves_to_three_dlq_topics(monkeypatch):
    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert len(cfg.topics) == 3
    assert "dlq.canonical.training_event" in cfg.topics
    assert "dlq.canonical.wellness_event" in cfg.topics
    assert "dlq.canonical.planning_block" in cfg.topics


# valid_topics includes raw topics
def test_valid_topics_includes_raw_strength():
    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert "raw.strength" in cfg.valid_topics


# valid_topics includes canonical topics (ADR-6: canonical-origin DLQ messages are valid)
def test_valid_topics_includes_canonical_training_event():
    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert "canonical.training_event" in cfg.valid_topics


# valid_topics excludes DLQ topics (loop-prevention, ADR-6)
def test_valid_topics_excludes_dlq_topics():
    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert "dlq.canonical.training_event" not in cfg.valid_topics
    assert "dlq.canonical.wellness_event" not in cfg.valid_topics
    assert "dlq.canonical.planning_block" not in cfg.valid_topics


# dry_run defaults to True (ADR-3)
def test_dry_run_defaults_to_true():
    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert cfg.dry_run is True


# --no-dry-run sets dry_run=False
def test_no_dry_run_flag_sets_false():
    parser = build_parser()
    args = parser.parse_args(["--topic", "all", "--no-dry-run"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert cfg.dry_run is False


# max_size_bytes defaults to 1MB
def test_max_size_bytes_defaults_to_1mb():
    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = ReplayConfig.from_args_and_env(args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"})
    assert cfg.max_size_bytes == 1_048_576


# sc-11: --from-offset and --from-timestamp are mutually exclusive → argparse exits
def test_from_offset_and_from_timestamp_mutually_exclusive():
    parser = build_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--topic", "all", "--from-offset", "0", "--from-timestamp", "2024-01-01T00:00:00Z"])
    assert exc_info.value.code != 0
