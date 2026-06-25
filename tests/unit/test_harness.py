"""Minimal harness smoke test (PR1, task 1.3).

Proves the pytest harness is wired and runnable without any external service.
This is intentionally trivial - it is NOT a substitute for real unit tests,
which arrive with PR2 (strength ingestion) onward once strict TDD is enabled.
"""

from __future__ import annotations

import importlib
import sys


def test_pytest_harness_collects_and_runs():
    """The harness itself must be importable and executable."""
    assert sys.version_info >= (3, 12), "AthleteOS requires Python >=3.12"


def test_scaffold_packages_importable():
    """Every scaffolded top-level package must be importable (markers present)."""
    for name in ("jobs", "ingestion", "schemas", "bootstrap", "storage", "api", "ui"):
        module = importlib.import_module(name)
        assert module is not None


def test_config_files_present():
    """Foundation config artifacts exist on disk."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    for filename in ("pyproject.toml", "pytest.ini", "docker-compose.yml", ".gitignore"):
        assert (repo_root / filename).exists(), f"missing foundation file: {filename}"


def test_canonical_schema_files_present():
    """All three canonical Avro schemas exist (PR1, task 2.1)."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parents[2]
    schemas = repo_root / "schemas" / "canonical"
    for name in ("TrainingEvent.avsc", "WellnessEvent.avsc", "PlanningBlock.avsc"):
        assert (schemas / name).exists(), f"missing canonical schema: {name}"