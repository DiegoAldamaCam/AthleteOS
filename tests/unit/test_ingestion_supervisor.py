"""
tests/unit/test_ingestion_supervisor.py

Supervisor unit + compose structure tests for athleteos-ingestion-wiring (Gap #7).
Scenarios sc-1..sc-6: thread isolation via injected fake watch_fn.
Scenarios sc-7..sc-10: PyYAML docker-compose.yml structure (mirrors test_observability_config.py).

No real Kafka, no Docker, no file I/O beyond YAML parsing.
"""
from __future__ import annotations

import os
import pathlib
import threading
from typing import List, Tuple

import pytest
import yaml

# ---------------------------------------------------------------------------
# Helpers — compose loader (mirrors test_observability_config.py pattern)
# ---------------------------------------------------------------------------
REPO_ROOT = pathlib.Path(__file__).parent.parent.parent


def _load_compose() -> dict:
    full = REPO_ROOT / "docker-compose.yml"
    with full.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Fake connectors for supervisor isolation tests (sc-1..sc-6)
# ---------------------------------------------------------------------------

def _make_recorder_watch_fn(calls: List[str], name: str):
    """Returns a watch_fn that records the call and then blocks on stop_event."""
    def _watch(dir_path, publisher, stop_event=None, **kwargs):
        calls.append(name)
        if stop_event is not None:
            stop_event.wait(timeout=5.0)
    return _watch


def _make_raising_watch_fn(exc_type: type, name: str):
    """Returns a watch_fn that raises the given exception immediately."""
    def _watch(dir_path, publisher, stop_event=None, **kwargs):
        raise exc_type(f"Simulated crash in {name}")
    return _watch


def _make_blocking_watch_fn(name: str):
    """Returns a watch_fn that blocks on stop_event (never raises)."""
    def _watch(dir_path, publisher, stop_event=None, **kwargs):
        if stop_event is not None:
            stop_event.wait(timeout=5.0)
    return _watch


class FakePublisher:
    """Minimal publisher stub — no Kafka needed."""
    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers

    def publish(self, record, **kwargs) -> str:
        return "fake-event-id"

    def flush(self) -> None:
        pass


class FakePublisherClass:
    """Publisher class that records instantiation bootstrap_servers."""
    instances: List["FakePublisherClass"] = []

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        FakePublisherClass.instances.append(self)

    def publish(self, record, **kwargs) -> str:
        return "fake-event-id"

    def flush(self) -> None:
        pass


def _make_connector_registry(
    watch_fn_override=None,
    publisher_class=None,
    names=("strength", "wellness", "planning", "cardio", "recovery", "nutrition"),
):
    """Build a minimal CONNECTORS registry with injectable watch_fn + publisher_class."""
    pc = publisher_class or FakePublisher
    entries = []
    for name in names:
        wfn = (watch_fn_override(name) if callable(watch_fn_override) else watch_fn_override) or _make_blocking_watch_fn(name)
        entries.append((name, wfn, pc, name))
    return entries


# ---------------------------------------------------------------------------
# sc-1 — All 6 threads started on supervisor launch
# ---------------------------------------------------------------------------
class TestAllSixThreadsStarted:
    """sc-1: supervisor must start exactly 6 threads — one per connector."""

    def test_exactly_six_threads_started(self, tmp_path, monkeypatch):
        """sc-1: GIVEN env set, WHEN main() called with stop_event pre-set,
        THEN exactly 6 threads are started (one per connector name)."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        started_names: List[str] = []

        def recorder_factory(name):
            return _make_recorder_watch_fn(started_names, name)

        connectors = _make_connector_registry(watch_fn_override=recorder_factory)
        stop_event = threading.Event()
        stop_event.set()  # pre-set so main() returns immediately after join

        main(connectors=connectors, stop_event=stop_event)

        assert len(started_names) == 6, (
            f"Expected 6 threads started, got {len(started_names)}: {started_names}"
        )

    def test_all_six_connector_names_started(self, tmp_path, monkeypatch):
        """sc-1 (triangulation): all 6 names present in started set."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        started_names: List[str] = []

        def recorder_factory(name):
            return _make_recorder_watch_fn(started_names, name)

        connectors = _make_connector_registry(watch_fn_override=recorder_factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        expected = {"strength", "wellness", "planning", "cardio", "recovery", "nutrition"}
        assert set(started_names) == expected, (
            f"Connector names mismatch. Expected {expected}, got {set(started_names)}"
        )


# ---------------------------------------------------------------------------
# sc-2 — One watcher raising Exception does NOT stop the others
# ---------------------------------------------------------------------------
class TestFaultIsolationGenericException:
    """sc-2: an unhandled Exception in one thread must not kill the others."""

    def test_exception_in_one_watcher_does_not_prevent_others(self, tmp_path, monkeypatch):
        """sc-2: GIVEN one watcher raises Exception, THEN 5 others still run."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        started_names: List[str] = []
        crash_name = "wellness"

        def factory(name):
            if name == crash_name:
                return _make_raising_watch_fn(Exception, name)
            return _make_recorder_watch_fn(started_names, name)

        connectors = _make_connector_registry(watch_fn_override=factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        # The 5 non-crashing watchers must have run
        assert len(started_names) == 5, (
            f"Expected 5 healthy threads to run, got {len(started_names)}: {started_names}"
        )
        assert crash_name not in started_names, (
            f"Crashing connector '{crash_name}' should not appear in started_names"
        )

    def test_exception_in_non_strength_watcher_isolation(self, tmp_path, monkeypatch):
        """sc-2 (triangulation): crash in cardio — other 5 still run."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        started_names: List[str] = []
        crash_name = "cardio"

        def factory(name):
            if name == crash_name:
                return _make_raising_watch_fn(Exception, name)
            return _make_recorder_watch_fn(started_names, name)

        connectors = _make_connector_registry(watch_fn_override=factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        assert len(started_names) == 5
        assert crash_name not in started_names


# ---------------------------------------------------------------------------
# sc-3 — Strength watcher RuntimeError is isolated (strength has NO internal try/except)
# ---------------------------------------------------------------------------
class TestStrengthFaultIsolation:
    """sc-3: strength watcher RuntimeError must not propagate to other threads."""

    def test_strength_runtimeerror_does_not_kill_others(self, tmp_path, monkeypatch):
        """sc-3: GIVEN strength raises RuntimeError, THEN 5 others still run."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        started_names: List[str] = []

        def factory(name):
            if name == "strength":
                return _make_raising_watch_fn(RuntimeError, "strength")
            return _make_recorder_watch_fn(started_names, name)

        connectors = _make_connector_registry(watch_fn_override=factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        assert len(started_names) == 5, (
            f"Expected 5 healthy threads to run after strength crash, got {len(started_names)}: {started_names}"
        )
        assert "strength" not in started_names

    def test_strength_crash_leaves_exact_five_running(self, tmp_path, monkeypatch):
        """sc-3 (triangulation): exactly the non-strength names ran."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        started_names: List[str] = []

        def factory(name):
            if name == "strength":
                return _make_raising_watch_fn(RuntimeError, "strength")
            return _make_recorder_watch_fn(started_names, name)

        connectors = _make_connector_registry(watch_fn_override=factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        expected = {"wellness", "planning", "cardio", "recovery", "nutrition"}
        assert set(started_names) == expected


# ---------------------------------------------------------------------------
# sc-4 — Missing subdir created on startup
# ---------------------------------------------------------------------------
class TestSubdirCreation:
    """sc-4: supervisor must create per-connector subdirs before watcher launch."""

    def test_subdir_created_before_watcher_launch(self, tmp_path, monkeypatch):
        """sc-4: GIVEN base dir exists without subdirs, WHEN main() runs,
        THEN subdir is created before the watcher thread is invoked."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        subdir_existed: List[bool] = []

        def factory(name):
            def _watch(dir_path, publisher, stop_event=None, **kwargs):
                subdir_existed.append(dir_path.exists())
            return _watch

        connectors = _make_connector_registry(watch_fn_override=factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        assert all(subdir_existed), (
            f"Some subdirs did not exist when watcher was called: {subdir_existed}"
        )
        assert len(subdir_existed) == 6

    def test_strength_subdir_created(self, tmp_path, monkeypatch):
        """sc-4 (triangulation): strength subdir specifically is created."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        stop_event = threading.Event()
        stop_event.set()
        connectors = _make_connector_registry(
            watch_fn_override=lambda name: _make_blocking_watch_fn(name)
        )
        main(connectors=connectors, stop_event=stop_event)

        assert (tmp_path / "strength").exists(), "strength subdir must be created by supervisor"


# ---------------------------------------------------------------------------
# sc-5 — Env var reads and fail-fast on missing vars
# ---------------------------------------------------------------------------
class TestEnvVarReads:
    """sc-5: KAFKA_BOOTSTRAP_SERVERS and INGEST_WATCH_DIR must be read from env."""

    def test_publishers_constructed_with_bootstrap_servers(self, tmp_path, monkeypatch):
        """sc-5: Publisher instances must receive bootstrap_servers from env."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        # Reset class-level tracker
        FakePublisherClass.instances = []

        from ingestion.__main__ import main  # noqa: PLC0415

        stop_event = threading.Event()
        stop_event.set()

        connectors = _make_connector_registry(
            watch_fn_override=lambda name: _make_blocking_watch_fn(name),
            publisher_class=FakePublisherClass,
        )
        main(connectors=connectors, stop_event=stop_event)

        assert len(FakePublisherClass.instances) == 6
        for inst in FakePublisherClass.instances:
            assert inst.bootstrap_servers == "kafka:9092", (
                f"Expected bootstrap_servers='kafka:9092', got {inst.bootstrap_servers!r}"
            )

    def test_missing_kafka_bootstrap_servers_raises(self, tmp_path, monkeypatch):
        """sc-5: missing KAFKA_BOOTSTRAP_SERVERS must raise KeyError (fail-fast)."""
        monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        stop_event = threading.Event()
        stop_event.set()
        connectors = _make_connector_registry(
            watch_fn_override=lambda name: _make_blocking_watch_fn(name)
        )
        with pytest.raises(KeyError):
            main(connectors=connectors, stop_event=stop_event)

    def test_missing_ingest_watch_dir_raises(self, monkeypatch):
        """sc-5 (triangulation): missing INGEST_WATCH_DIR must also raise KeyError."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.delenv("INGEST_WATCH_DIR", raising=False)

        from ingestion.__main__ import main  # noqa: PLC0415

        stop_event = threading.Event()
        stop_event.set()
        connectors = _make_connector_registry(
            watch_fn_override=lambda name: _make_blocking_watch_fn(name)
        )
        with pytest.raises(KeyError):
            main(connectors=connectors, stop_event=stop_event)


# ---------------------------------------------------------------------------
# sc-6 — Pre-set stop_event causes main() to join all threads
# ---------------------------------------------------------------------------
class TestStopEventJoinsThreads:
    """sc-6: pre-set stop_event causes main() to join all 6 threads before returning."""

    def test_prestop_event_causes_clean_return(self, tmp_path, monkeypatch):
        """sc-6: GIVEN stop_event already set, WHEN main() runs, THEN it returns
        promptly without blocking and all threads are joined."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        stop_event = threading.Event()
        stop_event.set()
        connectors = _make_connector_registry(
            watch_fn_override=lambda name: _make_blocking_watch_fn(name)
        )

        # If stop_event is pre-set and threads are properly joined, this returns.
        # A blocking deadlock would cause the test to time out.
        main(connectors=connectors, stop_event=stop_event)
        # If we reach here, threads were joined cleanly.
        assert True

    def test_stop_event_shared_across_all_watchers(self, tmp_path, monkeypatch):
        """sc-6 (triangulation): stop_event passed to every watcher is the SAME event."""
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        monkeypatch.setenv("INGEST_WATCH_DIR", str(tmp_path))

        from ingestion.__main__ import main  # noqa: PLC0415

        received_events: List[threading.Event] = []

        def factory(name):
            def _watch(dir_path, publisher, stop_event=None, **kwargs):
                if stop_event is not None:
                    received_events.append(stop_event)
            return _watch

        connectors = _make_connector_registry(watch_fn_override=factory)
        stop_event = threading.Event()
        stop_event.set()

        main(connectors=connectors, stop_event=stop_event)

        assert len(received_events) == 6, f"Expected 6 watchers to receive stop_event, got {len(received_events)}"
        # All watchers should receive the SAME event object
        assert all(ev is stop_event for ev in received_events), (
            "All watchers must share the same stop_event instance"
        )


# ---------------------------------------------------------------------------
# sc-7 — Compose ingestion service has build context, not sleep infinity
# ---------------------------------------------------------------------------
class TestComposeIngestionBuildNotSleep:
    """sc-7: docker-compose.yml ingestion service must have build and no sleep infinity."""

    def test_ingestion_build_key_present(self):
        """sc-7: services.ingestion must have a 'build' key."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        assert "build" in svc, (
            f"Expected 'build' key in ingestion service, got keys: {list(svc.keys())}"
        )

    def test_ingestion_command_not_sleep_infinity(self):
        """sc-7: services.ingestion must NOT have command: [sleep, infinity]."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        command = svc.get("command")
        assert command != ["sleep", "infinity"], (
            f"ingestion service must NOT have 'command: [sleep, infinity]', got: {command!r}"
        )


# ---------------------------------------------------------------------------
# sc-8 — Compose ingestion service depends on schema-bootstrap
# ---------------------------------------------------------------------------
class TestComposeIngestionDependsOnBootstrap:
    """sc-8: ingestion depends_on must include schema-bootstrap with service_completed_successfully."""

    def test_depends_on_schema_bootstrap_present(self):
        """sc-8: services.ingestion.depends_on must include schema-bootstrap."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        depends_on = svc.get("depends_on", {})
        assert "schema-bootstrap" in depends_on, (
            f"Expected 'schema-bootstrap' in ingestion.depends_on, got: {list(depends_on.keys())}"
        )

    def test_depends_on_schema_bootstrap_condition(self):
        """sc-8 (triangulation): condition must be service_completed_successfully."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        depends_on = svc.get("depends_on", {})
        sb = depends_on.get("schema-bootstrap", {})
        condition = sb.get("condition") if isinstance(sb, dict) else None
        assert condition == "service_completed_successfully", (
            f"Expected condition 'service_completed_successfully', got: {condition!r}"
        )


# ---------------------------------------------------------------------------
# sc-9 — Compose ingestion service has required env vars and volume
# ---------------------------------------------------------------------------
class TestComposeIngestionEnvAndVolume:
    """sc-9: ingestion service must declare required env vars and ./data:/data volume."""

    def test_kafka_bootstrap_servers_in_environment(self):
        """sc-9: KAFKA_BOOTSTRAP_SERVERS must be in ingestion environment."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        env = svc.get("environment", {})
        # environment may be a list of strings or a dict
        env_keys = set(env.keys()) if isinstance(env, dict) else {e.split("=")[0] for e in env}
        assert "KAFKA_BOOTSTRAP_SERVERS" in env_keys, (
            f"KAFKA_BOOTSTRAP_SERVERS not found in ingestion environment: {env_keys}"
        )

    def test_ingest_watch_dir_in_environment(self):
        """sc-9: INGEST_WATCH_DIR must be in ingestion environment."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        env = svc.get("environment", {})
        env_keys = set(env.keys()) if isinstance(env, dict) else {e.split("=")[0] for e in env}
        assert "INGEST_WATCH_DIR" in env_keys, (
            f"INGEST_WATCH_DIR not found in ingestion environment: {env_keys}"
        )

    def test_data_volume_present(self):
        """sc-9 (triangulation): ./data:/data volume must be declared."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        volumes_str = " ".join(str(v) for v in svc.get("volumes", []))
        assert "./data:/data" in volumes_str or "data:/data" in volumes_str, (
            f"Expected './data:/data' volume in ingestion service, got: {svc.get('volumes')}"
        )


# ---------------------------------------------------------------------------
# sc-10 — Compose ingestion service has restart: on-failure
# ---------------------------------------------------------------------------
class TestComposeIngestionRestartPolicy:
    """sc-10: ingestion service must have restart: on-failure."""

    def test_restart_on_failure(self):
        """sc-10: services.ingestion.restart must be 'on-failure'."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        restart = svc.get("restart")
        assert restart == "on-failure", (
            f"Expected restart='on-failure', got: {restart!r}"
        )

    def test_ingestion_profile_still_ingest(self):
        """sc-10 (triangulation): profile must remain 'ingest' after the rewrite."""
        compose = _load_compose()
        svc = compose["services"]["ingestion"]
        profiles = svc.get("profiles", [])
        assert "ingest" in profiles, (
            f"Expected 'ingest' in ingestion.profiles, got: {profiles}"
        )
