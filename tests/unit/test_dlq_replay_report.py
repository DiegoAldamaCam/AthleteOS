"""Unit tests for tools.dlq_replay.report (strict TDD — RED phase first)."""

from __future__ import annotations

import io

import pytest

from tools.dlq_replay.report import ReplayReport


# sc-21: ReplayReport counters increment correctly
def test_report_counters_start_at_zero():
    r = ReplayReport()
    assert r.replayed == 0
    assert r.skipped_oversized == 0
    assert r.skipped_unrecoverable == 0
    assert r.dry_run_would_replay == 0
    assert r.per_topic == {}


def test_report_counters_increment():
    r = ReplayReport()
    r.replayed += 2
    r.skipped_oversized += 1
    r.skipped_unrecoverable += 1
    assert r.replayed == 2
    assert r.skipped_oversized == 1
    assert r.skipped_unrecoverable == 1


# sc-22: dry_run_would_replay counter
def test_report_dry_run_counter():
    r = ReplayReport()
    r.dry_run_would_replay += 5
    assert r.dry_run_would_replay == 5
    assert r.replayed == 0


# sc-21, sc-22: print_summary contains all required fields
def test_print_summary_contains_all_required_fields(capsys):
    r = ReplayReport()
    r.replayed = 2
    r.skipped_oversized = 1
    r.skipped_unrecoverable = 1
    r.dry_run_would_replay = 0
    r.per_topic = {"raw.strength": {"replayed": 2}}
    r.print_summary()
    captured = capsys.readouterr()
    assert "replayed" in captured.out
    assert "skipped_oversized" in captured.out
    assert "skipped_unrecoverable" in captured.out
    assert "dry_run_would_replay" in captured.out
    assert "raw.strength" in captured.out


# sc-22: dry run summary shows would-replay not replayed
def test_print_summary_dry_run_shows_would_replay(capsys):
    r = ReplayReport()
    r.dry_run_would_replay = 5
    r.per_topic = {"dlq.canonical.training_event": {"dry_run_would_replay": 5}}
    r.print_summary()
    captured = capsys.readouterr()
    assert "5" in captured.out
    assert "dry_run_would_replay" in captured.out


# sc-21: per-topic breakdown with specific values
def test_print_summary_per_topic_breakdown(capsys):
    r = ReplayReport()
    r.replayed = 3
    r.per_topic = {
        "dlq.canonical.training_event": {"replayed": 2},
        "dlq.canonical.wellness_event": {"replayed": 1},
    }
    r.print_summary()
    captured = capsys.readouterr()
    assert "dlq.canonical.training_event" in captured.out
    assert "dlq.canonical.wellness_event" in captured.out
