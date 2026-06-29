"""Unit tests for tools.dlq_replay.__main__ (strict TDD — RED phase first).

Tests argparse behaviour, env-var fail-fast, timestamp parsing, and entry points.
No Kafka connection required.
"""

from __future__ import annotations

import subprocess
import sys
from unittest.mock import patch

import pytest

from tools.dlq_replay.config import _parse_timestamp


# sc-11: --from-offset + --from-timestamp together → exit code 1 + stderr naming both flags
def test_mutex_from_offset_and_from_timestamp_exits_nonzero(capsys):
    from tools.dlq_replay.config import build_parser
    parser = build_parser()
    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--topic", "all", "--from-offset", "0", "--from-timestamp", "2024-01-01T00:00:00Z"])
    assert exc_info.value.code != 0


# sc-23: Missing KAFKA_BOOTSTRAP_SERVERS → exit code 1 + stderr referencing env var
def test_main_missing_bootstrap_servers_exits_1(monkeypatch, capsys):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    from tools.dlq_replay.__main__ import main
    with pytest.raises(SystemExit) as exc_info:
        main(argv=["--topic", "dlq.canonical.training_event"])
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "KAFKA_BOOTSTRAP_SERVERS" in captured.err


# sc-24: --help exits 0 and prints usage
def test_help_exits_zero(capsys):
    from tools.dlq_replay.__main__ import main
    with pytest.raises(SystemExit) as exc_info:
        main(argv=["--help"])
    assert exc_info.value.code == 0


# sc-9: parse_timestamp with epoch-ms integer string
def test_parse_timestamp_epoch_ms_integer():
    result = _parse_timestamp("1719619200000")
    assert result == 1719619200000
    assert isinstance(result, int)


# sc-9: parse_timestamp with ISO8601 string (UTC Z)
def test_parse_timestamp_iso8601_utc():
    result = _parse_timestamp("2024-06-29T00:00:00Z")
    # 2024-06-29 00:00:00 UTC → epoch ms
    import datetime
    dt = datetime.datetime(2024, 6, 29, 0, 0, 0, tzinfo=datetime.timezone.utc)
    expected = int(dt.timestamp() * 1000)
    assert result == expected


# sc-9: parse_timestamp with ISO8601 string (no timezone → assume UTC)
def test_parse_timestamp_iso8601_no_tz():
    result = _parse_timestamp("2024-07-01T00:00:00")
    assert isinstance(result, int)
    assert result > 0


# Triangulate: invalid timestamp raises ArgumentTypeError
def test_parse_timestamp_invalid_raises_argument_type_error():
    import argparse
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_timestamp("not-a-timestamp")


# sc-24: python -m tools.dlq_replay --help exits 0 via subprocess
def test_module_entrypoint_help_exits_zero():
    result = subprocess.run(
        [sys.executable, "-m", "tools.dlq_replay", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "dlq-replay" in result.stdout.lower() or "usage" in result.stdout.lower()
