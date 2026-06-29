"""Static and grep tests for secrets hygiene — athleteos-secrets-mgmt.

Spec: sdd/athleteos-secrets-mgmt/spec (obs #328), scenarios sc-1..sc-5, sc-8..sc-12.
Design: sdd/athleteos-secrets-mgmt/design (obs #329), ADR-S1..S4.

All grep assertions target the exact plaintext strings that MUST NOT appear
after the secrets-mgmt change is applied. Tests are written first (RED) against
the unchanged codebase, then satisfied by the implementation (GREEN).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


def _repo_root() -> Path:
    """Absolute path to the repository root (file-based, no fixture dependency)."""
    return Path(__file__).resolve().parents[2]


class TestComposeSecretsStatic:
    """sc-1..sc-5: No plaintext secrets in tracked files.

    Each test uses 'git grep' to assert the plaintext value NO LONGER exists
    (exit code 1 = pattern not found = PASS after the edit).
    RED: these FAIL before docker-compose.yml / config.py edits are applied.
    GREEN: these PASS after the ${VAR} substitutions are in place.
    """

    def test_no_plaintext_postgres_password(self):
        """sc-1: POSTGRES_PASSWORD must not contain the literal value 'athleteos'.

        Scoped grep: 'POSTGRES_PASSWORD: athleteos' — targets the password assignment
        line only. Does NOT match POSTGRES_USER/POSTGRES_DB which legitimately
        retain 'athleteos' as a name (not a password).
        """
        repo = _repo_root()
        result = subprocess.run(
            ["git", "grep", "POSTGRES_PASSWORD: athleteos", "docker-compose.yml"],
            capture_output=True,
            cwd=repo,
        )
        assert result.returncode != 0, (
            "Found plaintext POSTGRES_PASSWORD in docker-compose.yml: "
            f"{result.stdout.decode()!r}"
        )

    def test_no_plaintext_database_url_password(self):
        """sc-2: DATABASE_URL must not contain the password-slot literal ':athleteos@'.

        ADR-S1: DATABASE_URL is built from ${POSTGRES_PASSWORD}, not a separate var.
        Pattern ':athleteos@' targets the password segment (before '@') specifically.
        After the edit, the DSN is 'postgresql://athleteos:${POSTGRES_PASSWORD}@...'
        — the user segment 'athleteos:' remains but ':athleteos@' (password) is gone.
        """
        repo = _repo_root()
        result = subprocess.run(
            ["git", "grep", "DATABASE_URL.*:athleteos@", "docker-compose.yml"],
            capture_output=True,
            cwd=repo,
        )
        assert result.returncode != 0, (
            "Found plaintext DATABASE_URL password ':athleteos@' in docker-compose.yml: "
            f"{result.stdout.decode()!r}"
        )

    def test_no_plaintext_api_key(self):
        """sc-3: API_KEY must not contain the literal value 'dev-replace-me'."""
        repo = _repo_root()
        result = subprocess.run(
            ["git", "grep", 'API_KEY: "dev-replace-me"', "docker-compose.yml"],
            capture_output=True,
            cwd=repo,
        )
        assert result.returncode != 0, (
            "Found plaintext API_KEY 'dev-replace-me' in docker-compose.yml: "
            f"{result.stdout.decode()!r}"
        )

    def test_no_plaintext_grafana_password(self):
        """sc-4: GF_SECURITY_ADMIN_PASSWORD must not contain the literal 'athleteos'."""
        repo = _repo_root()
        result = subprocess.run(
            ["git", "grep", 'GF_SECURITY_ADMIN_PASSWORD: "athleteos"', "docker-compose.yml"],
            capture_output=True,
            cwd=repo,
        )
        assert result.returncode != 0, (
            "Found plaintext GF_SECURITY_ADMIN_PASSWORD in docker-compose.yml: "
            f"{result.stdout.decode()!r}"
        )

    def test_no_database_url_default_in_config(self):
        """sc-5: api/config.py must not have a hardcoded default for database_url.

        ADR-S2: database_url is a REQUIRED field (bare 'database_url: str').
        Grep pattern 'database_url.*=.*postgresql' matches the old default line.
        After removal, grep returns exit 1 (no match) — this test passes.
        """
        repo = _repo_root()
        result = subprocess.run(
            ["git", "grep", "database_url.*=.*postgresql", "api/config.py"],
            capture_output=True,
            cwd=repo,
        )
        assert result.returncode != 0, (
            "Found hardcoded database_url default in api/config.py: "
            f"{result.stdout.decode()!r}"
        )


class TestEnvExampleStatic:
    """sc-10, sc-11: .env.example content assertions.

    RED: sc-10 and sc-11 fail before .env.example is created (file does not exist).
    GREEN: pass after .env.example is created with the 3 change-me-* keys.
    """

    def test_env_example_exists(self, repo_root):
        """sc-10 (existence): .env.example must exist at the repo root."""
        env_example = repo_root / ".env.example"
        assert env_example.exists(), (
            f".env.example does not exist at {env_example}. "
            "Run: create root .env.example with 3 change-me-* keys."
        )

    def test_env_example_has_three_required_keys(self, repo_root):
        """sc-10 (content): .env.example must contain exactly the 3 required keys.

        ADR-S4: DATABASE_URL is NOT included (computed by compose from POSTGRES_PASSWORD).
        All 3 values must start with 'change-me-'.
        """
        content = (repo_root / ".env.example").read_text(encoding="utf-8")

        assert "POSTGRES_PASSWORD=" in content, (
            "POSTGRES_PASSWORD= key missing from .env.example"
        )
        assert "API_KEY=" in content, "API_KEY= key missing from .env.example"
        assert "GF_SECURITY_ADMIN_PASSWORD=" in content, (
            "GF_SECURITY_ADMIN_PASSWORD= key missing from .env.example"
        )
        assert "DATABASE_URL=" not in content, (
            "DATABASE_URL= must NOT appear in .env.example — "
            "it is computed by compose from POSTGRES_PASSWORD (ADR-S1)"
        )

    def test_env_example_change_me_prefix(self, repo_root):
        """sc-10 (values): all .env.example values must start with 'change-me-'."""
        content = (repo_root / ".env.example").read_text(encoding="utf-8")
        for line in content.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            assert value.startswith("change-me-"), (
                f"Value for {key!r} must start with 'change-me-', got {value!r}"
            )

    def test_env_example_no_real_credentials(self, repo_root):
        """sc-11: .env.example must not contain known plaintext credential values.

        ADR-S4: grep-distinct from all prior plaintext values.
        """
        content = (repo_root / ".env.example").read_text(encoding="utf-8")
        assert "athleteos" not in content, (
            "Found 'athleteos' credential in .env.example — use change-me-* values only"
        )
        assert "dev-replace-me" not in content, (
            "Found 'dev-replace-me' credential in .env.example — use change-me-* values only"
        )


class TestGitignoreStatic:
    """sc-12: .gitignore covers .env (excludes) and .env.example (includes).

    This class is expected to be GREEN even before implementation
    (the .gitignore already has the correct rules from a prior commit).
    """

    def test_gitignore_covers_dot_env(self, repo_root):
        """sc-12: .gitignore must have .env rule and !.env.example exception."""
        gitignore = (repo_root / ".gitignore").read_text(encoding="utf-8")
        lines = [line.strip() for line in gitignore.splitlines()]

        assert ".env" in lines, ".gitignore must contain '.env' rule"
        assert "!.env.example" in lines, (
            ".gitignore must contain '!.env.example' exception to track the template"
        )
