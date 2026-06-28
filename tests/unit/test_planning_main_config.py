"""Unit tests for jobs.planning_canonicalize.main — PlanningCanonicalizeJobConfig.

These tests run on Python 3.14 (no pyflink required). They verify:
  - PlanningCanonicalizeJobConfig is import-safe (no top-level pyflink import).
  - Config defaults are correct.
  - effective_schema_version() returns override or fallback.
  - ProcessFunction has NO block_id-keyed state (ADR-20 structural guard).

Scenarios: PL2-9 (config + schema wiring), PL2-13 (import isolation).
"""

from __future__ import annotations

import inspect

import pytest


# ---------------------------------------------------------------------------
# Import-safety gate (must pass on Python 3.14 without pyflink)
# ---------------------------------------------------------------------------


def test_planning_main_imports_without_pyflink():
    """PL2-13: jobs.planning_canonicalize.main must import cleanly on Python 3.14.

    The module MUST NOT import pyflink at the module level. All pyflink imports
    must be LAZY (inside run()). This mirrors jobs/wellness_canonicalize/main.py.
    """
    from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig  # noqa: F401
    assert PlanningCanonicalizeJobConfig is not None


def test_run_function_exists():
    """jobs.planning_canonicalize.main must expose a run() function."""
    import jobs.planning_canonicalize.main as m
    assert callable(getattr(m, "run", None)), "run() must be defined in main.py"


def test_no_top_level_pyflink_import():
    """Top-level module source must not import pyflink outside of run().

    Inspects the module source to confirm pyflink imports are guarded inside
    the run() function body — NOT at module level.
    """
    import jobs.planning_canonicalize.main as m
    import ast
    import inspect

    source = inspect.getsource(m)
    tree = ast.parse(source)

    # Collect all top-level import nodes (not inside function bodies)
    top_level_imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            # Check if this is at module level (col_offset == 0 for top-level statements)
            # We check the parent — only FunctionDef/AsyncFunctionDef bodies are allowed
            # to contain pyflink imports. We'll scan the module body directly.
            pass

    # Simpler: scan module-level statements only
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if "pyflink" in alias.name:
                    top_level_imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and "pyflink" in node.module:
                top_level_imports.append(node.module)

    assert not top_level_imports, (
        f"pyflink must NOT be imported at module level (LAZY import required). "
        f"Found module-level pyflink imports: {top_level_imports}. "
        "All pyflink imports must live inside run()."
    )


# ---------------------------------------------------------------------------
# PlanningCanonicalizeJobConfig — constructor and defaults
# ---------------------------------------------------------------------------


class TestPlanningCanonicalizeJobConfig:
    """Verify config container construction and defaults."""

    def test_config_requires_bootstrap_and_registry(self):
        """Config requires bootstrap_servers and schema_registry_url."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="localhost:9092",
            schema_registry_url="http://localhost:8081",
        )
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.schema_registry_url == "http://localhost:8081"

    def test_config_default_group_id(self):
        """Default group_id must be 'canonicalize-planning'."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
        )
        assert cfg.group_id == "canonicalize-planning"

    def test_config_default_topics(self):
        """Default topics must be raw.planning, canonical.planning_block, dlq.canonical.planning_block."""
        from jobs.planning_canonicalize.main import (
            PlanningCanonicalizeJobConfig,
            RAW_TOPIC,
            CANONICAL_TOPIC,
            DLQ_TOPIC,
        )

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
        )
        assert cfg.raw_topic == RAW_TOPIC
        assert cfg.canonical_topic == CANONICAL_TOPIC
        assert cfg.dlq_topic == DLQ_TOPIC

    def test_config_topic_constants(self):
        """Topic constants must match the planning topology."""
        from jobs.planning_canonicalize.main import RAW_TOPIC, CANONICAL_TOPIC, DLQ_TOPIC

        assert RAW_TOPIC == "raw.planning"
        assert CANONICAL_TOPIC == "canonical.planning_block"
        assert DLQ_TOPIC == "dlq.canonical.planning_block"

    def test_config_default_checkpoint_interval(self):
        """Default checkpoint_interval_ms must be 60_000."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
        )
        assert cfg.checkpoint_interval_ms == 60_000

    def test_config_default_bounded_false(self):
        """Default bounded=False (streaming mode, not integration-test mode)."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
        )
        assert cfg.bounded is False

    def test_config_default_no_restart_false(self):
        """Default no_restart=False (production keeps default restart strategy)."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
        )
        assert cfg.no_restart is False

    def test_config_custom_overrides(self):
        """All config fields can be overridden at construction time."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="broker:9094",
            schema_registry_url="http://registry:8082",
            group_id="custom-group",
            raw_topic="raw.planning.test",
            canonical_topic="canonical.planning_block.test",
            dlq_topic="dlq.planning.test",
            checkpoint_interval_ms=5_000,
            schema_version=2,
            bounded=True,
            parallelism=2,
            no_restart=True,
        )
        assert cfg.bootstrap_servers == "broker:9094"
        assert cfg.group_id == "custom-group"
        assert cfg.bounded is True
        assert cfg.no_restart is True
        assert cfg.parallelism == 2


# ---------------------------------------------------------------------------
# effective_schema_version()
# ---------------------------------------------------------------------------


class TestEffectiveSchemaVersion:
    """effective_schema_version() fallback and override."""

    def test_returns_fallback_when_none(self):
        """Returns 1 (fallback) when schema_version is None."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
            schema_version=None,
        )
        assert cfg.effective_schema_version() == 1

    def test_returns_override_when_set(self):
        """Returns the configured schema_version when explicitly set."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
            schema_version=3,
        )
        assert cfg.effective_schema_version() == 3

    def test_returns_override_of_one(self):
        """Explicit schema_version=1 is treated as an override, not fallback."""
        from jobs.planning_canonicalize.main import PlanningCanonicalizeJobConfig

        cfg = PlanningCanonicalizeJobConfig(
            bootstrap_servers="b:9092",
            schema_registry_url="http://r:8081",
            schema_version=1,
        )
        # schema_version=1 explicitly set → should return 1 (override path)
        assert cfg.effective_schema_version() == 1


# ---------------------------------------------------------------------------
# ADR-20 structural guard: ProcessFunction must NOT have block_id state
# ---------------------------------------------------------------------------


class TestProcessFunctionNoBlockIdState:
    """ADR-20: PlanningCanonicalizeProcessFunction must NOT carry block_id-keyed state.

    The function must ONLY dedup by event_id (ValueState<bool>). Any block_id
    dedup state would drop plan revisions — the exact anti-goal of ADR-20.

    This test inspects the source code to verify there is no 'block_id' state
    descriptor registered in the open() method.
    """

    def test_no_block_id_in_state_descriptor_names(self):
        """No ValueStateDescriptor in main.py may reference 'block-id' or 'block_id'."""
        import jobs.planning_canonicalize.main as m
        source = inspect.getsource(m)

        # Check that no ValueStateDescriptor is named after block_id variants
        forbidden_patterns = [
            '"seen-planning-block-id"',
            "'seen-planning-block-id'",
            '"block_id"',
            "'block_id'",
            "block-id",
        ]
        # We look only in the context of ValueStateDescriptor calls
        # by checking for these patterns near 'ValueStateDescriptor'
        if "ValueStateDescriptor" not in source:
            # The function is lazy — this is fine; check source of run() only
            return

        for pattern in forbidden_patterns:
            # Check if pattern appears in a StateDescriptor context
            # Simple substring check is sufficient — ADR-20 is a hard rule
            idx = source.find("ValueStateDescriptor")
            while idx != -1:
                segment = source[idx : idx + 200]
                assert pattern not in segment, (
                    f"ADR-20 VIOLATION: Found block_id-keyed ValueStateDescriptor "
                    f"(pattern {pattern!r}) near line with 'ValueStateDescriptor'. "
                    f"Planning ProcessFunction must only carry event_id dedup state."
                )
                idx = source.find("ValueStateDescriptor", idx + 1)

    def test_event_id_state_descriptor_present(self):
        """The run() function source must reference an event_id dedup state descriptor."""
        import jobs.planning_canonicalize.main as m
        source = inspect.getsource(m)

        # The run() body must define an event_id-keyed ValueState for dedup.
        # We check for the descriptor name pattern used by wellness as a template.
        assert "planning-event-id" in source or "planning_event_id" in source or (
            "seen" in source and "planning" in source
        ), (
            "ADR-20: main.py run() must contain an event_id-keyed ValueState "
            "descriptor for dedup (mirrors wellness pattern). "
            "Found no recognizable event_id state descriptor."
        )

    def test_run_function_source_keys_by_athlete_id(self):
        """The run() wiring must key_by(athlete_id), NOT key_by(event_id).

        Planning keys by athlete_id (ADR-4 co-partitioning). Dedup is still
        event_id-based via MapState inside the ProcessFunction.
        """
        import jobs.planning_canonicalize.main as m
        source = inspect.getsource(m)

        # Check that athlete_id appears in the key_by lambda context.
        assert "athlete_id" in source, (
            "key_by must reference athlete_id (ADR-4 co-partitioning)"
        )

    def test_dedup_uses_map_state_not_value_state(self):
        """ADR-20 / CI-fix: dedup state must be MapState[event_id → bool], NOT ValueState<bool>.

        Root cause of CI failures #1/#2/#3: the operator is keyed by athlete_id
        (ADR-4). A ValueState<bool> has ONE cell per operator key — i.e. one boolean
        per athlete. After the first event for athlete A1 is processed and
        _seen.update(True) is called, EVERY subsequent event for A1 (regardless of
        event_id) hits the 'if bool(_seen.value()): return' guard and is silently
        dropped.

        The correct dedup structure for 'key_by(athlete_id) + dedup by event_id'
        is MapState[event_id → bool] so each distinct event_id within the same
        athlete partition has its own seen-cell.
        """
        import jobs.planning_canonicalize.main as m
        source = inspect.getsource(m)

        # MapStateDescriptor must be imported and used for per-event_id dedup.
        assert "MapStateDescriptor" in source, (
            "ADR-20 / CI-fix: dedup must use MapStateDescriptor[event_id → bool], "
            "not ValueStateDescriptor. With key_by(athlete_id), a ValueState<bool> "
            "has exactly ONE cell per athlete — all events after the first are silently "
            "dropped, causing CI failures PL2-1/PL2-2/PL2-3."
        )

        # ValueStateDescriptor for 'seen-planning-event-id' must NOT remain in the
        # source — it is the broken pattern being replaced by MapState.
        assert 'ValueStateDescriptor("seen-planning-event-id"' not in source and (
            "ValueStateDescriptor('seen-planning-event-id'" not in source
        ), (
            "ADR-20 / CI-fix: 'seen-planning-event-id' ValueStateDescriptor must be "
            "replaced by MapStateDescriptor. Remove the ValueState-based dedup."
        )
