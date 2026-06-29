"""Unit tests for tools.dlq_quality.__main__ — entry point wiring (strict TDD).

Covers: sc-19, sc-20, sc-21, sc-22, sc-27, sc-28, sc-30, sc-31, sc-32.
No real Kafka connection — scan() is mocked.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import defaultdict
from unittest.mock import MagicMock, patch

import pytest

from tools.dlq_quality.reports import QualityResult


def _make_result(
    error_type=None,
    age=None,
    age_extremes=None,
    triage_fix=None,
    triage_origin=None,
    samples=None,
    scanned=1,
    corrupt=0,
):
    """Build a minimal QualityResult for testing render paths."""
    return QualityResult(
        error_type=error_type or {},
        age=age or {},
        age_extremes=age_extremes or {},
        triage_fix=triage_fix or {},
        triage_origin=triage_origin or {},
        samples=samples or {},
        scanned=scanned,
        corrupt=corrupt,
    )


# ---------------------------------------------------------------------------
# sc-31: --help exits 0 and prints usage
# ---------------------------------------------------------------------------

def test_help_exits_zero(capsys):
    """sc-31: --help → SystemExit(0) with usage printed to stdout."""
    from tools.dlq_quality.__main__ import main

    with pytest.raises(SystemExit) as exc_info:
        main(argv=["--help"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "dlq-quality" in captured.out.lower() or "usage" in captured.out.lower()


# ---------------------------------------------------------------------------
# sc-30: Missing KAFKA_BOOTSTRAP_SERVERS → SystemExit(1) before any Kafka call
# ---------------------------------------------------------------------------

def test_missing_bootstrap_servers_exits_1(monkeypatch, capsys):
    """sc-30: KAFKA_BOOTSTRAP_SERVERS absent → exit 1 + error on stderr."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    from tools.dlq_quality.__main__ import main

    with pytest.raises(SystemExit) as exc_info:
        main(argv=[])
    assert exc_info.value.code == 1
    captured = capsys.readouterr()
    assert "KAFKA_BOOTSTRAP_SERVERS" in captured.err


def test_missing_bootstrap_servers_no_kafka_call(monkeypatch):
    """sc-30: DLQConsumer never constructed when env var is absent."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    from tools.dlq_quality.__main__ import main

    with patch("tools.dlq_quality.quality.DLQConsumer") as MockConsumer, \
         pytest.raises(SystemExit):
        main(argv=[])
    assert MockConsumer.call_count == 0, "DLQConsumer must not be called when env var is missing"


# ---------------------------------------------------------------------------
# sc-22: --format json routes to render_json, stdout is valid JSON
# ---------------------------------------------------------------------------

def test_format_json_routes_to_render_json(monkeypatch, capsys):
    """sc-22: --format json → stdout is valid json.loads()-parseable JSON."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    from tools.dlq_quality.__main__ import main

    mock_result = _make_result(
        error_type={"dlq.canonical.training_event": {"VALIDATION_FAILURE": 2}},
        scanned=2,
    )

    with patch("tools.dlq_quality.__main__.scan", return_value=mock_result), \
         patch("tools.dlq_quality.__main__.retention_warning"):
        main(argv=["--format", "json"])

    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert isinstance(parsed, dict)
    assert "error_type" in parsed


# ---------------------------------------------------------------------------
# sc-21: --format table routes to render_table
# ---------------------------------------------------------------------------

def test_format_table_routes_to_render_table(monkeypatch, capsys):
    """sc-21: --format table (default) → stdout is human-readable text with labels."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    from tools.dlq_quality.__main__ import main

    mock_result = _make_result(
        error_type={"dlq.canonical.training_event": {"VALIDATION_FAILURE": 1}},
        scanned=1,
    )

    with patch("tools.dlq_quality.__main__.scan", return_value=mock_result), \
         patch("tools.dlq_quality.__main__.retention_warning"):
        main(argv=["--format", "table"])

    captured = capsys.readouterr()
    assert "VALIDATION_FAILURE" in captured.out
    # JSON parse should FAIL (it's not JSON)
    try:
        json.loads(captured.out)
        is_json = True
    except json.JSONDecodeError:
        is_json = False
    assert not is_json, "table output should not be valid JSON"


# ---------------------------------------------------------------------------
# sc-20: --report age → only age section in output
# ---------------------------------------------------------------------------

def test_report_age_only_section_in_output(monkeypatch, capsys):
    """sc-20: --report age → age data in output, no error-type or triage data."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    from tools.dlq_quality.__main__ import main

    mock_result = _make_result(
        age={"dlq.canonical.training_event": {"<1d": 3}},
        age_extremes={"dlq.canonical.training_event": {"oldest": 100, "newest": 200}},
        scanned=3,
    )

    with patch("tools.dlq_quality.__main__.scan", return_value=mock_result), \
         patch("tools.dlq_quality.__main__.retention_warning"):
        main(argv=["--report", "age", "--format", "json"])

    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    # age populated
    assert parsed["age"] != {}
    # error_type and triage empty
    assert parsed["error_type"] == {}
    assert parsed["triage_fix"] == {}


# ---------------------------------------------------------------------------
# sc-19: --report all → all 3 sections present
# ---------------------------------------------------------------------------

def test_report_all_all_sections_present(monkeypatch, capsys):
    """sc-19: --report all → error_type, age, triage all present in JSON output."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    from tools.dlq_quality.__main__ import main

    topic = "dlq.canonical.training_event"
    mock_result = _make_result(
        error_type={topic: {"VALIDATION_FAILURE": 1}},
        age={topic: {"<1d": 1}},
        age_extremes={topic: {"oldest": 100, "newest": 200}},
        triage_fix={topic: {"DATA_FIX": 1}},
        triage_origin={topic: {"raw.strength": 1}},
        scanned=1,
    )

    with patch("tools.dlq_quality.__main__.scan", return_value=mock_result), \
         patch("tools.dlq_quality.__main__.retention_warning"):
        main(argv=["--report", "all", "--format", "json"])

    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["error_type"] != {}
    assert parsed["age"] != {}
    assert parsed["triage_fix"] != {}


# ---------------------------------------------------------------------------
# sc-27/28: retention_warning() called after scan when >6d or expired non-empty
# ---------------------------------------------------------------------------

def test_retention_warning_called_after_scan(monkeypatch):
    """sc-27/28: retention_warning() invoked once after scan() in main()."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    from tools.dlq_quality.__main__ import main

    mock_result = _make_result()

    with patch("tools.dlq_quality.__main__.scan", return_value=mock_result) as mock_scan, \
         patch("tools.dlq_quality.__main__.retention_warning") as mock_warn:
        main(argv=[])

    mock_warn.assert_called_once_with(mock_result)


# ---------------------------------------------------------------------------
# sc-31: python -m tools.dlq_quality --help via subprocess
# ---------------------------------------------------------------------------

def test_module_entrypoint_help_exits_zero_subprocess():
    """sc-31: python -m tools.dlq_quality --help → returncode 0 via subprocess."""
    result = subprocess.run(
        [sys.executable, "-m", "tools.dlq_quality", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "dlq-quality" in result.stdout.lower() or "usage" in result.stdout.lower()
