"""Unit tests for bootstrap.register_schemas — DEFECT-5 guard.

Asserts that register_all() does NOT pre-register canonical value subjects,
letting the Flink avro-confluent sink own its own writer-schema registration
on first emission (user decision, G4 DEFECT-5 fix).

Canonical TOPICS must still exist (create_topics handles that separately);
only the Schema Registry subject pre-registration is suppressed here.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

from bootstrap._topology import CANONICAL_TOPICS
from bootstrap.register_schemas import register_all, _subject_for


class TestCanonicalSubjectsNotPreRegistered:
    """register_all() must NOT call set_compatibility or register_schema
    for any canonical value subject (NAME_MISMATCH prevention, DEFECT-5)."""

    def test_set_compatibility_not_called_for_canonical_training_event(self):
        """set_compatibility must NOT be called for canonical.training_event-value."""
        with patch("bootstrap.register_schemas.set_compatibility") as mock_compat, \
             patch("bootstrap.register_schemas.register_schema") as mock_reg:
            mock_reg.return_value = 1
            register_all("http://localhost:8081")

        called_subjects = [c.args[1] for c in mock_compat.call_args_list]
        assert "canonical.training_event-value" not in called_subjects, (
            "set_compatibility was called for canonical.training_event-value — "
            "Flink sink will get NAME_MISMATCH on first write (DEFECT-5)"
        )

    def test_register_schema_not_called_for_canonical_training_event(self):
        """register_schema must NOT be called for canonical.training_event-value."""
        with patch("bootstrap.register_schemas.set_compatibility"), \
             patch("bootstrap.register_schemas.register_schema") as mock_reg:
            mock_reg.return_value = 1
            register_all("http://localhost:8081")

        called_subjects = [c.args[1] for c in mock_reg.call_args_list]
        assert "canonical.training_event-value" not in called_subjects, (
            "register_schema was called for canonical.training_event-value — "
            "Flink sink will conflict with pre-registered BACKWARD-compat schema (DEFECT-5)"
        )

    def test_no_canonical_value_subject_is_pre_registered(self):
        """None of the canonical value subjects must be pre-registered.

        All three canonical sinks (training_event, wellness_event, planning_block)
        use Flink Table avro-confluent DDL, which infers its own writer schema with
        a Flink-generated record name. Pre-registering ANY of them under BACKWARD
        causes NAME_MISMATCH rejection on the first write.
        """
        with patch("bootstrap.register_schemas.set_compatibility") as mock_compat, \
             patch("bootstrap.register_schemas.register_schema") as mock_reg:
            mock_reg.return_value = 1
            register_all("http://localhost:8081")

        compat_subjects = {c.args[1] for c in mock_compat.call_args_list}
        reg_subjects = {c.args[1] for c in mock_reg.call_args_list}
        all_called = compat_subjects | reg_subjects

        canonical_value_subjects = {
            _subject_for(topic) for topic in CANONICAL_TOPICS
        }
        # No canonical value subject must appear in any registry call
        overlap = canonical_value_subjects & all_called
        assert overlap == set(), (
            f"Bootstrap pre-registered canonical value subjects: {overlap}. "
            "These must be left for the Flink sink to register on first write."
        )

    def test_register_all_returns_empty_dict_when_canonical_subjects_skipped(self):
        """register_all() returns an empty dict when no subjects are registered."""
        with patch("bootstrap.register_schemas.set_compatibility"), \
             patch("bootstrap.register_schemas.register_schema") as mock_reg:
            mock_reg.return_value = 1
            result = register_all("http://localhost:8081")

        assert result == {}, (
            f"Expected empty dict (no subjects registered), got {result!r}"
        )


class TestSubjectHelperAndTopology:
    """Verify the _subject_for helper and CANONICAL_TOPICS structure are intact
    (structural guard — ensures the topology constants used by tests are correct)."""

    def test_subject_for_training_event(self):
        assert _subject_for("canonical.training_event") == "canonical.training_event-value"

    def test_subject_for_wellness_event(self):
        assert _subject_for("canonical.wellness_event") == "canonical.wellness_event-value"

    def test_subject_for_planning_block(self):
        assert _subject_for("canonical.planning_block") == "canonical.planning_block-value"

    def test_canonical_topics_has_three_entries(self):
        """CANONICAL_TOPICS must still enumerate all 3 topics (topology unchanged)."""
        assert len(CANONICAL_TOPICS) == 3
        assert "canonical.training_event" in CANONICAL_TOPICS
        assert "canonical.wellness_event" in CANONICAL_TOPICS
        assert "canonical.planning_block" in CANONICAL_TOPICS


class TestMainLogsHonestSubjectCount:
    """main() must report the ACTUAL number of pre-registered subjects, not
    len(CANONICAL_TOPICS). After DEFECT-5 the real count is 0; logging 3 would
    be a misleading ops signal (R3 review finding)."""

    def test_main_reports_zero_subjects_not_topic_count(self, capsys):
        from bootstrap.register_schemas import main

        with patch("bootstrap.register_schemas.set_compatibility"), \
             patch("bootstrap.register_schemas.register_schema"):
            rc = main()

        out = capsys.readouterr().out
        assert rc == 0
        # Must NOT claim it registered 3 (len(CANONICAL_TOPICS)) subjects.
        assert "registered 3" not in out, (
            "main() logged a subject count of 3 while register_all() pre-registers 0 "
            "(misleading bootstrap log)"
        )
        assert "pre-registered 0 subject(s)" in out
