"""Entry point for the dlq-replay CLI.

Invokable as:
    python -m tools.dlq_replay [args]   (without install)
    dlq-replay [args]                    (after pip install -e .)
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, build config, run replay, print summary.

    Args:
        argv: Argument list (defaults to sys.argv[1:] when None).

    Returns:
        Exit code (0 on success, 1 on configuration error).
    """
    import os

    from tools.dlq_replay.config import ReplayConfig, build_parser
    from tools.dlq_replay.consumer import DLQConsumer
    from tools.dlq_replay.producer import DLQProducer
    from tools.dlq_replay.replay import run_replay

    parser = build_parser()
    args = parser.parse_args(argv)

    # Fail-fast on missing KAFKA_BOOTSTRAP_SERVERS (sc-23).
    # from_args_and_env calls sys.exit(1) directly when the env var is missing;
    # allow that SystemExit to propagate so the process exits with code 1.
    config = ReplayConfig.from_args_and_env(args, env=dict(os.environ))

    consumer = DLQConsumer(config=config)
    producer = DLQProducer(bootstrap_servers=config.bootstrap_servers)

    report = run_replay(config, consumer, producer)
    report.print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
