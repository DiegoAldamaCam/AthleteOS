"""
tests/unit/test_observability_config.py

Structure tests for PR-OBS2 observability configuration.
Scenarios: sc-8, sc-9, sc-10, sc-10b, sc-11, sc-11b, sc-11c

No Docker, no prometheus_client import — pure PyYAML / json.loads parsing.
Runs on Python 3.14 (current CI interpreter).
"""
import json
import pathlib

import pytest
import yaml

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
REPO_ROOT = pathlib.Path(__file__).parent.parent.parent


def _load_yaml(rel_path: str) -> dict:
    """Load and parse a YAML file relative to the repo root."""
    full = REPO_ROOT / rel_path
    with full.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _load_compose() -> dict:
    return _load_yaml("docker-compose.yml")


def _parse_flink_properties(props_str: str) -> dict:
    """Parse a FLINK_PROPERTIES multiline string value as YAML."""
    return yaml.safe_load(props_str)


# ---------------------------------------------------------------------------
# sc-8 — docker-compose has prometheus + grafana services under observability
# ---------------------------------------------------------------------------
class TestComposePrometheusGrafanaServices:
    """Scenario 8: docker-compose parses with prometheus + grafana services."""

    def test_prometheus_service_exists_with_profile(self):
        """prometheus service must exist with profiles: [observability]."""
        compose = _load_compose()
        services = compose["services"]
        assert "prometheus" in services, "prometheus service not found in docker-compose.yml"
        svc = services["prometheus"]
        assert "observability" in svc.get("profiles", []), (
            "prometheus service must have 'observability' profile"
        )

    def test_prometheus_port_9090(self):
        """prometheus service must publish 9090:9090."""
        compose = _load_compose()
        svc = compose["services"]["prometheus"]
        ports_str = " ".join(str(p) for p in svc.get("ports", []))
        assert "9090" in ports_str, f"Expected 9090 port in prometheus service, got: {svc.get('ports')}"

    def test_prometheus_volume_mount_references_observability_yml(self):
        """prometheus service must mount ./observability/prometheus.yml."""
        compose = _load_compose()
        svc = compose["services"]["prometheus"]
        volumes_str = " ".join(str(v) for v in svc.get("volumes", []))
        assert "observability/prometheus.yml" in volumes_str, (
            f"Expected observability/prometheus.yml volume in prometheus service, got: {svc.get('volumes')}"
        )

    def test_grafana_service_exists_with_profile(self):
        """grafana service must exist with profiles: [observability]."""
        compose = _load_compose()
        services = compose["services"]
        assert "grafana" in services, "grafana service not found in docker-compose.yml"
        svc = services["grafana"]
        assert "observability" in svc.get("profiles", []), (
            "grafana service must have 'observability' profile"
        )

    def test_grafana_port_3000(self):
        """grafana service must publish 3000:3000."""
        compose = _load_compose()
        svc = compose["services"]["grafana"]
        ports_str = " ".join(str(p) for p in svc.get("ports", []))
        assert "3000" in ports_str, f"Expected 3000 port in grafana service, got: {svc.get('ports')}"

    def test_grafana_volume_mount_references_provisioning(self):
        """grafana service must mount observability/grafana/provisioning."""
        compose = _load_compose()
        svc = compose["services"]["grafana"]
        volumes_str = " ".join(str(v) for v in svc.get("volumes", []))
        assert "observability/grafana/provisioning" in volumes_str, (
            f"Expected observability/grafana/provisioning volume in grafana service, got: {svc.get('volumes')}"
        )


# ---------------------------------------------------------------------------
# sc-9 — observability/prometheus.yml has 3 scrape jobs with correct targets
# ---------------------------------------------------------------------------
class TestPrometheusThreeScrapeJobs:
    """Scenario 9: prometheus.yml has three scrape jobs with correct targets."""

    def test_exactly_three_scrape_jobs(self):
        """prometheus.yml must have exactly 3 scrape_configs entries."""
        prom = _load_yaml("observability/prometheus.yml")
        jobs = prom.get("scrape_configs", [])
        assert len(jobs) == 3, f"Expected 3 scrape jobs, got {len(jobs)}: {[j.get('job_name') for j in jobs]}"

    def test_fastapi_job_target_and_metrics_path(self):
        """fastapi scrape job must target fastapi:8000 with metrics_path /metrics."""
        prom = _load_yaml("observability/prometheus.yml")
        jobs = {j["job_name"]: j for j in prom["scrape_configs"]}
        assert "fastapi" in jobs, f"Expected 'fastapi' job, found: {list(jobs.keys())}"
        job = jobs["fastapi"]
        assert job.get("metrics_path") == "/metrics", (
            f"fastapi job metrics_path must be /metrics, got: {job.get('metrics_path')}"
        )
        targets_str = " ".join(
            str(t) for group in job.get("static_configs", []) for t in group.get("targets", [])
        )
        assert "fastapi:8000" in targets_str, (
            f"fastapi job must target fastapi:8000, got: {targets_str}"
        )

    def test_flink_jobmanager_job_target(self):
        """flink-jobmanager scrape job must target flink-jobmanager:9249."""
        prom = _load_yaml("observability/prometheus.yml")
        jobs = {j["job_name"]: j for j in prom["scrape_configs"]}
        assert "flink-jobmanager" in jobs, f"Expected 'flink-jobmanager' job, found: {list(jobs.keys())}"
        job = jobs["flink-jobmanager"]
        targets_str = " ".join(
            str(t) for group in job.get("static_configs", []) for t in group.get("targets", [])
        )
        assert "flink-jobmanager:9249" in targets_str, (
            f"flink-jobmanager job must target flink-jobmanager:9249, got: {targets_str}"
        )

    def test_flink_taskmanager_job_target(self):
        """flink-taskmanager scrape job must target flink-taskmanager:9250."""
        prom = _load_yaml("observability/prometheus.yml")
        jobs = {j["job_name"]: j for j in prom["scrape_configs"]}
        assert "flink-taskmanager" in jobs, f"Expected 'flink-taskmanager' job, found: {list(jobs.keys())}"
        job = jobs["flink-taskmanager"]
        targets_str = " ".join(
            str(t) for group in job.get("static_configs", []) for t in group.get("targets", [])
        )
        assert "flink-taskmanager:9250" in targets_str, (
            f"flink-taskmanager job must target flink-taskmanager:9250, got: {targets_str}"
        )


# ---------------------------------------------------------------------------
# sc-10 — flink-jobmanager FLINK_PROPERTIES: modern key, no deprecated key
# ---------------------------------------------------------------------------
class TestFlinkJmModernKeyNoDeprecated:
    """Scenario 10: Modern factory.class key present, deprecated .class absent (jobmanager)."""

    def _get_jm_flink_props(self) -> dict:
        compose = _load_compose()
        env = compose["services"]["flink-jobmanager"]["environment"]
        props_str = env["FLINK_PROPERTIES"]
        return _parse_flink_properties(props_str)

    def test_modern_factory_class_key_present(self):
        """metrics.reporter.prom.factory.class must be present in jm FLINK_PROPERTIES."""
        props = self._get_jm_flink_props()
        key = "metrics.reporter.prom.factory.class"
        assert key in props, f"Expected '{key}' in jm FLINK_PROPERTIES, got keys: {list(props.keys())}"

    def test_modern_factory_class_value_correct(self):
        """metrics.reporter.prom.factory.class must be PrometheusReporterFactory."""
        props = self._get_jm_flink_props()
        key = "metrics.reporter.prom.factory.class"
        expected = "org.apache.flink.metrics.prometheus.PrometheusReporterFactory"
        assert props.get(key) == expected, (
            f"Expected '{expected}', got: {props.get(key)}"
        )

    def test_jm_reporter_port_9249(self):
        """metrics.reporter.prom.port must be 9249 for jobmanager."""
        props = self._get_jm_flink_props()
        port_val = props.get("metrics.reporter.prom.port")
        assert str(port_val) == "9249", (
            f"Expected jm reporter port 9249, got: {port_val!r}"
        )

    def test_deprecated_class_key_absent(self):
        """metrics.reporter.prom.class (deprecated) must NOT be present in jm FLINK_PROPERTIES."""
        props = self._get_jm_flink_props()
        deprecated_key = "metrics.reporter.prom.class"
        assert deprecated_key not in props, (
            f"Deprecated key '{deprecated_key}' found in jm FLINK_PROPERTIES — must be removed (ADR-27)"
        )


# ---------------------------------------------------------------------------
# sc-10b — flink-taskmanager FLINK_PROPERTIES: modern key, port 9250
# ---------------------------------------------------------------------------
class TestFlinkTmModernKeyPort:
    """Scenario 10b: Taskmanager also has modern key on correct port."""

    def _get_tm_flink_props(self) -> dict:
        compose = _load_compose()
        env = compose["services"]["flink-taskmanager"]["environment"]
        props_str = env["FLINK_PROPERTIES"]
        return _parse_flink_properties(props_str)

    def test_tm_modern_factory_class_key_present(self):
        """metrics.reporter.prom.factory.class must be present in tm FLINK_PROPERTIES."""
        props = self._get_tm_flink_props()
        key = "metrics.reporter.prom.factory.class"
        assert key in props, f"Expected '{key}' in tm FLINK_PROPERTIES, got keys: {list(props.keys())}"

    def test_tm_reporter_port_9250(self):
        """metrics.reporter.prom.port must be 9250 for taskmanager."""
        props = self._get_tm_flink_props()
        port_val = props.get("metrics.reporter.prom.port")
        assert str(port_val) == "9250", (
            f"Expected tm reporter port 9250, got: {port_val!r}"
        )

    def test_tm_deprecated_class_key_absent(self):
        """metrics.reporter.prom.class (deprecated) must NOT be present in tm FLINK_PROPERTIES."""
        props = self._get_tm_flink_props()
        deprecated_key = "metrics.reporter.prom.class"
        assert deprecated_key not in props, (
            f"Deprecated key '{deprecated_key}' found in tm FLINK_PROPERTIES — must be removed (ADR-27)"
        )


# ---------------------------------------------------------------------------
# sc-11 — Grafana datasource points to prometheus:9090
# ---------------------------------------------------------------------------
class TestGrafanaDatasourcePrometheus9090:
    """Scenario 11: Grafana datasource points to prometheus:9090."""

    def test_datasource_type_prometheus(self):
        """At least one datasource must have type=prometheus."""
        ds = _load_yaml("observability/grafana/provisioning/datasources/prometheus.yml")
        datasources = ds.get("datasources", [])
        assert any(d.get("type") == "prometheus" for d in datasources), (
            f"No datasource with type=prometheus found in datasources: {datasources}"
        )

    def test_datasource_url_contains_prometheus_9090(self):
        """Prometheus datasource url must contain 'prometheus:9090'."""
        ds = _load_yaml("observability/grafana/provisioning/datasources/prometheus.yml")
        datasources = ds.get("datasources", [])
        prom_ds = next((d for d in datasources if d.get("type") == "prometheus"), None)
        assert prom_ds is not None, "No prometheus datasource found"
        url = prom_ds.get("url", "")
        assert "prometheus:9090" in url, (
            f"Datasource url must contain 'prometheus:9090', got: {url!r}"
        )


# ---------------------------------------------------------------------------
# sc-11b — Dashboard provider references dashboards directory
# ---------------------------------------------------------------------------
class TestDashboardProviderPath:
    """Scenario 11b: Dashboard provider references dashboards directory."""

    def test_provider_path_references_dashboards_dir(self):
        """At least one provider must have a path pointing to the dashboards directory."""
        cfg = _load_yaml("observability/grafana/provisioning/dashboards/provider.yml")
        providers = cfg.get("providers", [])
        assert len(providers) > 0, "No providers found in dashboards provider.yml"
        # path must reference the grafana dashboards directory
        paths = [p.get("options", {}).get("path", "") for p in providers]
        assert any("dashboards" in p for p in paths), (
            f"No provider with path referencing 'dashboards', found paths: {paths}"
        )


# ---------------------------------------------------------------------------
# sc-11c — Dashboard JSON has exactly 5 panels with required titles
# ---------------------------------------------------------------------------
class TestDashboardFivePanelsTitles:
    """Scenario 11c: Dashboard JSON parses with 5 required panels."""

    REQUIRED_TITLES = {
        "DLQ Depth",
        "Records Processed Rate",
        "API Request Rate",
        "API p95 Latency",
        "DLQ Error Rate by Type",
    }

    def _load_dashboard(self) -> dict:
        path = REPO_ROOT / "observability/grafana/dashboards/athleteos_pipeline.json"
        with path.open("r", encoding="utf-8") as fh:
            return json.loads(fh.read())

    def test_exactly_five_panels(self):
        """Dashboard must have exactly 5 panels."""
        dash = self._load_dashboard()
        panels = dash.get("panels", [])
        assert len(panels) == 5, (
            f"Expected 5 panels, got {len(panels)}: {[p.get('title') for p in panels]}"
        )

    def test_all_required_panel_titles_present(self):
        """Dashboard must contain all 5 required panel titles."""
        dash = self._load_dashboard()
        panels = dash.get("panels", [])
        actual_titles = {p.get("title") for p in panels}
        missing = self.REQUIRED_TITLES - actual_titles
        assert not missing, (
            f"Missing required panel titles: {missing}. Actual titles: {actual_titles}"
        )
