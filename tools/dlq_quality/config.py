"""QualityConfig — argument and environment-variable configuration for dlq-quality.

Read-only sibling of dlq_replay.config. Builds a ReplayConfig internally for
the DLQConsumer without modifying any dlq_replay module.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

from bootstrap._topology import DLQ_TOPICS
from tools.dlq_replay.config import _ALL_DLQ_TOPICS, _parse_timestamp

# All 3 report names available to --report.
_ALL_REPORTS: frozenset[str] = frozenset({"error-type", "age", "triage"})


@dataclass
class QualityConfig:
    """Resolved runtime configuration for a single dlq-quality invocation.

    Attributes:
        bootstrap_servers: Kafka bootstrap servers string.
        topics: DLQ topics to consume (one or all three).
        reports: Which reports to produce — subset of {error-type, age, triage}.
        fmt: Output format: 'table' (default) or 'json'.
        from_timestamp_ms: If set, consumer seeks to this epoch-ms timestamp.
        max_count: If set, stop after this many messages total.
        sample_count: Max error_message samples per (topic, error_type, original_topic).
    """

    bootstrap_servers: str
    topics: list[str]
    reports: frozenset[str]
    fmt: str = "table"
    from_timestamp_ms: int | None = None
    max_count: int | None = None
    sample_count: int = 3

    @classmethod
    def from_args_and_env(
        cls,
        args: argparse.Namespace,
        env: dict[str, str] | None = None,
    ) -> "QualityConfig":
        """Build a QualityConfig from parsed CLI args and an environment mapping.

        Args:
            args: Parsed argparse.Namespace from ``build_parser().parse_args()``.
            env: Environment variable dict (defaults to os.environ if None).

        Returns:
            A fully resolved QualityConfig.

        Raises:
            SystemExit(1): If KAFKA_BOOTSTRAP_SERVERS is missing or empty.
        """
        import os

        if env is None:
            env = os.environ  # type: ignore[assignment]

        bootstrap_servers = env.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
        if not bootstrap_servers:
            print(
                "ERROR: KAFKA_BOOTSTRAP_SERVERS is not set. "
                "Export it before running dlq-quality.",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.topic == "all":
            topics = _ALL_DLQ_TOPICS
        else:
            topics = [args.topic]

        if args.report == "all":
            reports = _ALL_REPORTS
        else:
            reports = frozenset({args.report})

        return cls(
            bootstrap_servers=bootstrap_servers,
            topics=topics,
            reports=reports,
            fmt=args.format,
            from_timestamp_ms=getattr(args, "from_timestamp", None),
            max_count=getattr(args, "max_count", None),
            sample_count=getattr(args, "sample_count", 3),
        )

    def build_replay_config(self):
        """Build a ReplayConfig for DLQConsumer from this QualityConfig.

        The valid_topics field is intentionally passed as frozenset() —
        DLQConsumer never reads it (verified on disk: consumer.py has no
        valid_topics access). This is a required ReplayConfig field whose
        value is irrelevant for read-only quality scanning.

        W1 carry-forward: valid_topics intentionally empty — DLQConsumer never
        reads it; regression guard: integration test.

        Returns:
            A ReplayConfig wired for the quality scan.
        """
        from tools.dlq_replay.config import ReplayConfig
        from confluent_kafka import OFFSET_BEGINNING

        return ReplayConfig(
            bootstrap_servers=self.bootstrap_servers,
            topics=self.topics,
            # valid_topics intentionally empty — DLQConsumer never reads it;
            # regression guard: integration test (W1 carry-forward).
            valid_topics=frozenset(),
            dry_run=True,
            from_timestamp_ms=self.from_timestamp_ms,
            from_offset=OFFSET_BEGINNING if self.from_timestamp_ms is None else None,
            max_count=None,  # enforced in quality scan loop, not consumer (ADR-2)
        )


def build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for dlq-quality.

    Returns:
        Configured ArgumentParser with all dlq-quality arguments.
    """
    dlq_topic_list = ", ".join(sorted(DLQ_TOPICS.keys()))
    parser = argparse.ArgumentParser(
        prog="dlq-quality",
        description=(
            "Scan DLQ topics and emit error-type, age, and schema-triage reports. "
            "Read-only: never produces messages."
        ),
    )
    parser.add_argument(
        "--topic",
        default="all",
        metavar="TOPIC|all",
        help=(
            "DLQ topic to scan, or 'all' (default) to scan all 3: " + dlq_topic_list
        ),
    )
    parser.add_argument(
        "--report",
        default="all",
        choices=["all", "error-type", "age", "triage"],
        metavar="REPORT",
        help=(
            "Which report to emit: all (default), error-type, age, or triage. "
            "Choices: all, error-type, age, triage."
        ),
    )
    parser.add_argument(
        "--format",
        default="table",
        choices=["table", "json"],
        metavar="FORMAT",
        help="Output format: table (default, human-readable) or json.",
    )
    parser.add_argument(
        "--sample-count",
        default=3,
        type=int,
        dest="sample_count",
        metavar="N",
        help=(
            "Max error_message samples per (topic, error_type, original_topic) "
            "combination (default 3, max 10)."
        ),
    )
    parser.add_argument(
        "--max-count",
        default=None,
        type=int,
        dest="max_count",
        metavar="N",
        help="Stop after consuming N messages total across all partitions.",
    )
    parser.add_argument(
        "--from-timestamp",
        default=None,
        type=_parse_timestamp,
        dest="from_timestamp",
        metavar="TIMESTAMP",
        help=(
            "Scope scan to messages at or after this time. Accepts epoch-ms integer "
            "or ISO8601 string (e.g. 2024-07-01T00:00:00Z)."
        ),
    )
    return parser
