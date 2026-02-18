"""Test fixtures for g-gremlin-hubspot-mcp."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from g_gremlin_hubspot_mcp.runner import RunResult

GOLDEN_DIR = Path(__file__).parent / "golden"


def golden_stdout(name: str) -> str:
    """Load golden stdout fixture."""
    path = GOLDEN_DIR / f"{name}.json"
    if path.exists():
        return path.read_text(encoding="utf-8")
    # Fallback to .txt
    txt_path = GOLDEN_DIR / f"{name}.txt"
    if txt_path.exists():
        return txt_path.read_text(encoding="utf-8")
    raise FileNotFoundError(f"No golden fixture for {name}")


def make_run_result(
    stdout: str = "",
    stderr: str = "",
    exit_code: int = 0,
) -> RunResult:
    """Create a RunResult for testing."""
    return RunResult(stdout=stdout, stderr=stderr, exit_code=exit_code)


def success_result(stdout: str = '{"ok": true}') -> RunResult:
    return make_run_result(stdout=stdout)


def error_result(stderr: str = "Error: something went wrong", exit_code: int = 1) -> RunResult:
    return make_run_result(stderr=stderr, exit_code=exit_code)


@pytest.fixture
def mock_run_gremlin():
    """Patch run_gremlin to return controlled RunResults."""
    with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock_read, \
         patch("g_gremlin_hubspot_mcp.tools.analyze.run_gremlin", new_callable=AsyncMock) as mock_analyze, \
         patch("g_gremlin_hubspot_mcp.tools.mutate.run_gremlin", new_callable=AsyncMock) as mock_mutate:
        yield {
            "read": mock_read,
            "analyze": mock_analyze,
            "mutate": mock_mutate,
        }


@pytest.fixture
def tmp_csv(tmp_path: Path) -> Path:
    """Create a temporary CSV file for testing."""
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("id,email,firstname\n1,a@b.com,Alice\n2,c@d.com,Bob\n", encoding="utf-8")
    return csv_path


@pytest.fixture
def tmp_plan(tmp_path: Path) -> Path:
    """Create a temporary merge plan JSON for testing."""
    plan_path = tmp_path / "merge_plan.json"
    plan = {
        "object_type": "contacts",
        "groups": [
            {
                "key": "alice@example.com",
                "primary": "101",
                "secondaries": ["102", "103"],
            }
        ],
    }
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    return plan_path
