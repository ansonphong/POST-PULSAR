"""Contract checks for the executable continuous-integration workflow."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")


def _workflow_text() -> str:
    """Load the CI workflow as the repository-level contract under test."""
    return WORKFLOW.read_text(encoding="utf-8")


def _run_commands(workflow: str) -> list[str]:
    """Extract single-line shell commands from the deliberately simple workflow."""
    return [
        match.group("command").strip()
        for match in re.finditer(
            r"^\s+run:\s*(?P<command>\S.*)$", workflow, re.MULTILINE
        )
    ]


def test_ci_workflow_enforces_release_quality_and_security_gates() -> None:
    """Keep CI executable, reproducible, credential-free, and network-denied."""
    workflow = _workflow_text()
    commands = _run_commands(workflow)
    combined_commands = "\n".join(commands)

    versions = re.search(r"python-version:\s*\[([^]]+)]", workflow)
    assert versions is not None
    assert {value.strip(" '\"") for value in versions.group(1).split(",")} == {
        "3.12",
        "3.13",
        "3.14",
    }

    action_references = re.findall(
        r"^\s+uses:\s*([^@\s]+)@([^\s#]+)", workflow, re.MULTILINE
    )
    assert action_references
    assert all(FULL_SHA.fullmatch(revision) for _, revision in action_references)
    assert all("# v" in line for line in workflow.splitlines() if "uses:" in line)

    assert all(command for command in commands)
    assert "uv sync --frozen" in combined_commands
    assert "uv lock --check" in combined_commands
    assert "uv run --frozen ruff format --check" in combined_commands
    assert "uv run --frozen ruff check" in combined_commands
    assert "uv run --frozen mypy" in combined_commands

    pytest_commands = [command for command in commands if " pytest " in f" {command} "]
    assert len(pytest_commands) == 1
    pytest_command = pytest_commands[0]
    assert "--disable-socket" in pytest_command
    assert "--cov=post_pulsar" in pytest_command
    assert "--cov-report=" in pytest_command
    assert "tests" in pytest_command

    assert "uv run --frozen python -m build" in combined_commands
    assert "uv run --frozen pip-audit --locked ." in combined_commands
    assert "secrets." not in workflow.lower()
    assert not re.search(
        r"(?i)(instagram|x|twitter).*(token|secret|password)", workflow
    )
