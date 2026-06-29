"""Entry point for dlq-quality.

Invokable as:
    python -m tools.dlq_quality [args]
    dlq-quality [args]                  # when registered in [project.scripts]

Exits 1 if KAFKA_BOOTSTRAP_SERVERS is not set (checked inside
QualityConfig.from_args_and_env before any Kafka call — sc-30).
"""

from __future__ import annotations

import sys
from typing import Sequence

from tools.dlq_quality.config import QualityConfig, build_parser
from tools.dlq_quality.quality import scan
from tools.dlq_quality.reports import render_json, render_table, retention_warning


def main(argv: Sequence[str] | None = None) -> None:
    """Parse args, build config, run scan, render output, emit retention warning.

    Args:
        argv: CLI argument list. Defaults to sys.argv[1:] when None.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    # QualityConfig.from_args_and_env() calls sys.exit(1) if
    # KAFKA_BOOTSTRAP_SERVERS is absent — no Kafka call is made (sc-30).
    config = QualityConfig.from_args_and_env(args)

    result = scan(config)

    if config.fmt == "json":
        print(render_json(result))
    else:
        print(render_table(result))

    retention_warning(result)


if __name__ == "__main__":
    main()
