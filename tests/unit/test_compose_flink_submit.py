"""Unit tests for flink-job-submit service in docker-compose.yml.

Verifies sc-5 (depends_on), sc-6 (jobs profile), sc-13 (checkpoint dir)
by parsing docker-compose.yml directly — no Docker required.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml


@pytest.fixture(scope="module")
def compose() -> dict:
    """Load docker-compose.yml from the repo root."""
    repo_root = Path(__file__).resolve().parents[2]
    compose_file = repo_root / "docker-compose.yml"
    return yaml.safe_load(compose_file.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def submit_service(compose: dict) -> dict:
    """Return the flink-job-submit service block."""
    services = compose.get("services", {})
    assert "flink-job-submit" in services, (
        "flink-job-submit service not found in docker-compose.yml"
    )
    return services["flink-job-submit"]


class TestFlinkJobSubmitProfile:
    def test_service_has_jobs_profile(self, submit_service: dict):
        """sc-6: flink-job-submit must be gated behind the 'jobs' profile."""
        assert "jobs" in submit_service.get("profiles", [])

    def test_service_only_has_jobs_profile(self, submit_service: dict):
        """sc-6 (triangulation): profile list must be exactly ['jobs']."""
        assert submit_service.get("profiles") == ["jobs"]


class TestFlinkJobSubmitDependsOn:
    def test_depends_on_flink_jobmanager(self, submit_service: dict):
        """sc-5: depends on flink-jobmanager (service_healthy)."""
        depends = submit_service.get("depends_on", {})
        assert "flink-jobmanager" in depends
        assert depends["flink-jobmanager"]["condition"] == "service_healthy"

    def test_depends_on_postgres(self, submit_service: dict):
        """sc-5: depends on postgres (service_healthy)."""
        depends = submit_service.get("depends_on", {})
        assert "postgres" in depends
        assert depends["postgres"]["condition"] == "service_healthy"

    def test_depends_on_kafka(self, submit_service: dict):
        """sc-5: depends on kafka (service_healthy)."""
        depends = submit_service.get("depends_on", {})
        assert "kafka" in depends
        assert depends["kafka"]["condition"] == "service_healthy"

    def test_depends_on_schema_bootstrap_completed_successfully(self, submit_service: dict):
        """sc-5: depends on schema-bootstrap (service_completed_successfully)."""
        depends = submit_service.get("depends_on", {})
        assert "schema-bootstrap" in depends
        assert depends["schema-bootstrap"]["condition"] == "service_completed_successfully"

    def test_cross_profile_deps_have_required_false(self, submit_service: dict):
        """W4 (design-gate): cross-profile deps must have required:false so
        '--profile jobs' alone renders a valid project without core/bootstrap profiles.
        """
        depends = submit_service.get("depends_on", {})
        cross_profile_deps = ["schema-bootstrap", "kafka", "postgres", "flink-jobmanager"]
        for dep in cross_profile_deps:
            if dep in depends:
                assert depends[dep].get("required") is False, (
                    f"depends_on.{dep} must have required: false (W4 cross-profile fix)"
                )


class TestFlinkJobSubmitCheckpointDir:
    def test_metrics_checkpoint_dir_env(self, submit_service: dict):
        """sc-13: METRICS_CHECKPOINT_DIR must be 'file:///flink-checkpoints'."""
        env = submit_service.get("environment", {})
        assert env.get("METRICS_CHECKPOINT_DIR") == "file:///flink-checkpoints"

    def test_flink_jm_env(self, submit_service: dict):
        """sc-5 (triangulation): FLINK_JM must point to flink-jobmanager:8082."""
        env = submit_service.get("environment", {})
        assert env.get("FLINK_JM") == "flink-jobmanager:8082"


class TestFlinkCheckpointVolume:
    def test_checkpoint_volume_declared(self, compose: dict):
        """W6: flink-checkpoints named volume must exist at top level."""
        volumes = compose.get("volumes", {})
        assert "flink-checkpoints" in volumes

    def test_checkpoint_volume_mounted_on_taskmanager(self, compose: dict):
        """W6: flink-checkpoints volume must be mounted on flink-taskmanager."""
        tm = compose["services"].get("flink-taskmanager", {})
        mounts = tm.get("volumes", [])
        assert any("flink-checkpoints" in str(m) for m in mounts), (
            "flink-checkpoints volume not mounted on flink-taskmanager"
        )

    def test_checkpoint_volume_mounted_on_jobmanager(self, compose: dict):
        """W6 (triangulation): flink-checkpoints volume mounted on flink-jobmanager too."""
        jm = compose["services"].get("flink-jobmanager", {})
        mounts = jm.get("volumes", [])
        assert any("flink-checkpoints" in str(m) for m in mounts), (
            "flink-checkpoints volume not mounted on flink-jobmanager"
        )
