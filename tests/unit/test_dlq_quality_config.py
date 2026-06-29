"""Unit tests for tools.dlq_quality.config (strict TDD — RED phase first).

Covers: sc-23, sc-24, sc-25, sc-30, W1.
No Kafka connection required.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# sc-30: KAFKA_BOOTSTRAP_SERVERS absent → SystemExit(1) before any Kafka call
# ---------------------------------------------------------------------------

def test_missing_bootstrap_servers_exits_1(monkeypatch):
    """sc-30: env var absent → sys.exit(1) with error on stderr."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args([])
    with pytest.raises(SystemExit) as exc_info:
        QualityConfig.from_args_and_env(args, env={})
    assert exc_info.value.code == 1


def test_missing_bootstrap_servers_prints_to_stderr(monkeypatch, capsys):
    """sc-30: error message mentions KAFKA_BOOTSTRAP_SERVERS on stderr."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args([])
    with pytest.raises(SystemExit):
        QualityConfig.from_args_and_env(args, env={})
    captured = capsys.readouterr()
    assert "KAFKA_BOOTSTRAP_SERVERS" in captured.err


# ---------------------------------------------------------------------------
# sc-31 / sc-32: build_parser() defaults
# ---------------------------------------------------------------------------

def test_build_parser_default_report_all():
    """build_parser() default --report is 'all'."""
    from tools.dlq_quality.config import build_parser

    parser = build_parser()
    args = parser.parse_args([])
    assert args.report == "all"


def test_build_parser_default_format_table():
    """build_parser() default --format is 'table'."""
    from tools.dlq_quality.config import build_parser

    parser = build_parser()
    args = parser.parse_args([])
    assert args.format == "table"


def test_build_parser_default_topic_all():
    """build_parser() default --topic is 'all'."""
    from tools.dlq_quality.config import build_parser

    parser = build_parser()
    args = parser.parse_args([])
    assert args.topic == "all"


def test_build_parser_default_sample_count_3():
    """build_parser() default --sample-count is 3."""
    from tools.dlq_quality.config import build_parser

    parser = build_parser()
    args = parser.parse_args([])
    assert args.sample_count == 3


def test_build_parser_default_max_count_none():
    """build_parser() default --max-count is None."""
    from tools.dlq_quality.config import build_parser

    parser = build_parser()
    args = parser.parse_args([])
    assert args.max_count is None


def test_build_parser_default_from_timestamp_none():
    """build_parser() default --from-timestamp is None."""
    from tools.dlq_quality.config import build_parser

    parser = build_parser()
    args = parser.parse_args([])
    assert args.from_timestamp is None


# ---------------------------------------------------------------------------
# sc-23: --topic all resolves to all 3 DLQ topic strings
# ---------------------------------------------------------------------------

def test_topic_all_resolves_to_three_dlq_topics():
    """sc-23: --topic all → QualityConfig.topics has all 3 DLQ topics."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--topic", "all"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert len(cfg.topics) == 3
    assert "dlq.canonical.training_event" in cfg.topics
    assert "dlq.canonical.wellness_event" in cfg.topics
    assert "dlq.canonical.planning_block" in cfg.topics


# ---------------------------------------------------------------------------
# sc-24: --topic single restricts to one topic
# ---------------------------------------------------------------------------

def test_single_topic_resolves_correctly():
    """sc-24: --topic dlq.canonical.training_event → topics list has exactly 1 entry."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--topic", "dlq.canonical.training_event"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert cfg.topics == ["dlq.canonical.training_event"]


def test_single_topic_other():
    """sc-24: --topic dlq.canonical.wellness_event → topics list has exactly that topic."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--topic", "dlq.canonical.wellness_event"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert cfg.topics == ["dlq.canonical.wellness_event"]


# ---------------------------------------------------------------------------
# sc-25: --from-timestamp wired via _parse_timestamp
# ---------------------------------------------------------------------------

def test_from_timestamp_integer_wired():
    """sc-25: --from-timestamp integer string → QualityConfig.from_timestamp_ms is int."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--from-timestamp", "1719619200000"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert cfg.from_timestamp_ms == 1_719_619_200_000


def test_from_timestamp_iso8601_wired():
    """sc-25: --from-timestamp ISO8601 string → parsed to epoch-ms int."""
    import datetime
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--from-timestamp", "2024-06-29T00:00:00Z"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    expected = int(
        datetime.datetime(2024, 6, 29, tzinfo=datetime.timezone.utc).timestamp() * 1000
    )
    assert cfg.from_timestamp_ms == expected


def test_from_timestamp_none_by_default():
    """sc-25: no --from-timestamp → from_timestamp_ms is None."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args([])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert cfg.from_timestamp_ms is None


# ---------------------------------------------------------------------------
# W1: valid_topics is frozenset() — QualityConfig never sets it to anything else
# ---------------------------------------------------------------------------

def test_quality_config_bootstrap_servers_stored():
    """Bootstrap servers string is stored correctly in QualityConfig."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args([])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "broker1:9092,broker2:9092"}
    )
    assert cfg.bootstrap_servers == "broker1:9092,broker2:9092"


def test_quality_config_report_frozenset_all():
    """--report all → cfg.reports is frozenset containing all 3 report names."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--report", "all"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert isinstance(cfg.reports, frozenset)
    assert "error-type" in cfg.reports
    assert "age" in cfg.reports
    assert "triage" in cfg.reports


def test_quality_config_report_single():
    """--report age → cfg.reports contains only 'age'."""
    from tools.dlq_quality.config import build_parser, QualityConfig

    parser = build_parser()
    args = parser.parse_args(["--report", "age"])
    cfg = QualityConfig.from_args_and_env(
        args, env={"KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"}
    )
    assert cfg.reports == frozenset({"age"})
