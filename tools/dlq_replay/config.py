"""ReplayConfig — argument + environment-variable configuration for dlq-replay."""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field

from bootstrap._topology import DLQ_TOPICS, all_topics

# Canonical list of DLQ topics in a stable order.
_ALL_DLQ_TOPICS: list[str] = sorted(DLQ_TOPICS.keys())

# Valid replay targets: every topic that is NOT a DLQ topic.
# This is exactly RAW_TOPICS ∪ CANONICAL_TOPICS (ADR-6).
_VALID_TOPICS: frozenset[str] = frozenset(all_topics().keys()) - frozenset(DLQ_TOPICS.keys())

# Default maximum decoded value size (1 MiB).
DEFAULT_MAX_SIZE_BYTES = 1_048_576


@dataclass
class ReplayConfig:
    """Resolved runtime configuration for a single dlq-replay invocation.

    Attributes:
        bootstrap_servers: Kafka bootstrap servers string.
        topics: DLQ topics to consume (one or all three).
        valid_topics: Frozenset of allowed original_topic values
            (RAW ∪ CANONICAL, i.e. all_topics() - DLQ_TOPICS).
        dry_run: When True (default) no messages are produced.
        error_type: If set, only messages with this error_type are replayed.
        from_timestamp_ms: If set, consumer seeks to this epoch-ms timestamp.
        from_offset: If set, consumer seeks to this absolute partition offset.
        max_count: If set, stop after processing this many messages.
        max_size_bytes: Skip messages whose decoded value exceeds this size.
    """

    bootstrap_servers: str
    topics: list[str]
    valid_topics: frozenset[str]
    dry_run: bool = True
    error_type: str | None = None
    from_timestamp_ms: int | None = None
    from_offset: int | None = None
    max_count: int | None = None
    max_size_bytes: int = DEFAULT_MAX_SIZE_BYTES

    @classmethod
    def from_args_and_env(
        cls,
        args: argparse.Namespace,
        env: dict[str, str] | None = None,
    ) -> "ReplayConfig":
        """Build a ReplayConfig from parsed CLI args and an environment mapping.

        Args:
            args: Parsed argparse.Namespace from ``build_parser().parse_args()``.
            env: Environment variable dict (defaults to os.environ if None).

        Returns:
            A fully resolved ReplayConfig.

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
                "Export it before running dlq-replay.",
                file=sys.stderr,
            )
            sys.exit(1)

        if args.topic == "all":
            topics = _ALL_DLQ_TOPICS
        else:
            topics = [args.topic]

        return cls(
            bootstrap_servers=bootstrap_servers,
            topics=topics,
            valid_topics=_VALID_TOPICS,
            dry_run=args.dry_run,
            error_type=getattr(args, "error_type", None),
            from_timestamp_ms=getattr(args, "from_timestamp", None),
            from_offset=getattr(args, "from_offset", None),
            max_count=getattr(args, "max_count", None),
            max_size_bytes=getattr(args, "max_size_bytes", DEFAULT_MAX_SIZE_BYTES),
        )


def build_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser for dlq-replay.

    Returns:
        Configured ArgumentParser with all dlq-replay arguments.
    """
    parser = argparse.ArgumentParser(
        prog="dlq-replay",
        description="Replay failed DLQ messages back to their original Kafka topics.",
    )
    parser.add_argument(
        "--topic",
        required=True,
        metavar="TOPIC|all",
        help=(
            "DLQ topic to consume, or 'all' to consume all 3 DLQ topics: "
            + ", ".join(_ALL_DLQ_TOPICS)
        ),
    )
    parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        default=True,
        dest="dry_run",
        help="Dry-run mode (default ON). Use --no-dry-run to actually produce messages.",
    )
    parser.add_argument(
        "--error-type",
        default=None,
        metavar="ERROR_TYPE",
        help=(
            "Only replay messages with this error_type. "
            "Known values: VALIDATION_FAILURE, SCHEMA_INCOMPATIBILITY, "
            "DESERIALIZATION_ERROR, TRANSFORM_ERROR, LATE_DATA."
        ),
    )

    # --from-timestamp and --from-offset are mutually exclusive (ADR-2, sc-11).
    seek_group = parser.add_mutually_exclusive_group()
    seek_group.add_argument(
        "--from-timestamp",
        default=None,
        type=_parse_timestamp,
        metavar="TIMESTAMP",
        help=(
            "Start consumption from this point. Accepts epoch-ms integer "
            "or ISO8601 string (e.g. 2024-07-01T00:00:00Z). "
            "Mutually exclusive with --from-offset."
        ),
    )
    seek_group.add_argument(
        "--from-offset",
        default=None,
        type=int,
        metavar="OFFSET",
        help=(
            "Start consumption from this absolute partition offset. "
            "Mutually exclusive with --from-timestamp."
        ),
    )

    parser.add_argument(
        "--max-count",
        default=None,
        type=int,
        metavar="N",
        help="Stop after processing N messages (replayed + skipped).",
    )
    parser.add_argument(
        "--max-size-bytes",
        default=DEFAULT_MAX_SIZE_BYTES,
        type=int,
        metavar="BYTES",
        help=f"Skip messages whose decoded value exceeds this size (default {DEFAULT_MAX_SIZE_BYTES}).",
    )
    return parser


def _parse_timestamp(value: str) -> int:
    """Parse a timestamp string to epoch-milliseconds.

    Accepts either an epoch-ms integer string or an ISO8601 datetime string.

    Args:
        value: Raw CLI string from --from-timestamp.

    Returns:
        Epoch-millisecond integer.

    Raises:
        argparse.ArgumentTypeError: If the string cannot be parsed.
    """
    import datetime

    # Pure integer → treat as epoch-ms directly.
    try:
        return int(value)
    except ValueError:
        pass

    # ISO8601 string → parse and convert to epoch-ms.
    try:
        # Replace Z with +00:00 for fromisoformat compatibility (Python 3.11).
        dt = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # Naive datetime → assume UTC.
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid timestamp '{value}': expected epoch-ms integer or ISO8601 string. {exc}"
        ) from exc
