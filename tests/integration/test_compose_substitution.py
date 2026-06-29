"""Integration test: docker compose config resolves all ${VAR} tokens — sc-14.

Spec: sdd/athleteos-secrets-mgmt/spec (obs #328), scenario sc-14.
Design: sdd/athleteos-secrets-mgmt/design (obs #329), ADR-S1.

Requires Docker daemon. Skips cleanly when Docker is unavailable (CI sandboxes,
Docker Desktop not running). Uses the conftest requires_docker() gate which is
the same pattern as every other integration test in this suite.

Test strategy:
  Write a temp .env with test values for all 3 keys → run 'docker compose config'
  from repo root with that env → assert no literal ${VAR} tokens remain in stdout.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

import pytest

# Module-level Docker gate: skip the entire module if Docker is unavailable.
# This mirrors the pattern in other integration test files (requires_docker()
# at module level causes a collection-time skip via allow_module_level=True).
from tests.conftest import requires_docker

requires_docker()


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.integration
class TestComposeVariableSubstitution:
    """sc-14: docker compose config resolves all ${VAR} tokens.

    With a populated .env file, 'docker compose config' must render fully
    substituted values — no literal '${POSTGRES_PASSWORD}', '${API_KEY}',
    or '${GF_SECURITY_ADMIN_PASSWORD}' tokens remain in the output.
    """

    def test_compose_config_resolves_all_var_tokens(self, tmp_path):
        """sc-14: No unresolved ${VAR} tokens in 'docker compose config' output.

        Approach:
        - Write a temp .env with safe test values for all 3 secret keys.
        - Run 'docker compose config' from repo_root with COMPOSE_ENV_FILES pointing
          to the temp .env (avoids interfering with any real root .env).
        - Assert stdout contains NONE of the 3 ${VAR} token strings.
        - Assert returncode == 0 (compose parsed the config successfully).
        """
        # Build a temp .env with known test values
        test_env_content = (
            "POSTGRES_PASSWORD=test-pg-secret-value\n"
            "API_KEY=test-api-key-value\n"
            "GF_SECURITY_ADMIN_PASSWORD=test-grafana-secret\n"
        )
        temp_env = tmp_path / ".env"
        temp_env.write_text(test_env_content, encoding="utf-8")

        # Run 'docker compose config' with the temp .env as the env file source.
        # COMPOSE_ENV_FILES overrides the default .env lookup (compose >= v2.24).
        # Fallback: write to a standard location and pass --env-file.
        run_env = os.environ.copy()
        run_env["COMPOSE_ENV_FILES"] = str(temp_env)

        result = subprocess.run(
            ["docker", "compose", "--env-file", str(temp_env), "config"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            env=run_env,
        )

        assert result.returncode == 0, (
            f"'docker compose config' failed (rc={result.returncode}).\n"
            f"stderr: {result.stderr}"
        )

        rendered = result.stdout

        assert "${POSTGRES_PASSWORD}" not in rendered, (
            "Unresolved ${POSTGRES_PASSWORD} token found in 'docker compose config' output. "
            "The compose file must not have literal ${POSTGRES_PASSWORD} remaining after "
            "substitution with a populated .env."
        )
        assert "${API_KEY}" not in rendered, (
            "Unresolved ${API_KEY} token found in 'docker compose config' output."
        )
        assert "${GF_SECURITY_ADMIN_PASSWORD}" not in rendered, (
            "Unresolved ${GF_SECURITY_ADMIN_PASSWORD} token found in 'docker compose config' output."
        )

        # Positive assertion: confirm the substituted test value appears in output
        assert "test-pg-secret-value" in rendered, (
            "POSTGRES_PASSWORD test value not found in rendered config — "
            "substitution may have failed silently."
        )
