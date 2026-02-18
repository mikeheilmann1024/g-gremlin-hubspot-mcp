"""Tests for Tier 2 analyze tools — dedupe plan golden tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from g_gremlin_hubspot_mcp.envelope import compute_plan_hash
from g_gremlin_hubspot_mcp.runner import RunResult
from g_gremlin_hubspot_mcp.tools.analyze import (
    hubspot_dedupe_plan,
    hubspot_props_drift,
    hubspot_snapshot_diff,
)

GOLDEN_DIR = Path(__file__).parent / "golden"


class TestDedupePlan:
    @pytest.mark.asyncio
    async def test_success_includes_plan_hash(self, tmp_path: Path):
        golden = (GOLDEN_DIR / "merge_plan.json").read_text(encoding="utf-8")

        # Mock run_gremlin and also mock the temp file creation
        with patch("g_gremlin_hubspot_mcp.tools.analyze.run_gremlin", new_callable=AsyncMock) as mock, \
             patch("g_gremlin_hubspot_mcp.tools.analyze.create_temp_dir") as mock_dir, \
             patch("g_gremlin_hubspot_mcp.tools.analyze.cleanup_run_dir"):

            mock_dir.return_value = tmp_path
            mock.return_value = RunResult(stdout=golden, stderr="", exit_code=0)

            # Write a plan file that the tool would expect
            plan_data = {"groups": [{"key": "a@b.com", "primary": "1", "secondaries": ["2"]}]}
            plan_path = tmp_path / "merge_plan.json"
            plan_path.write_text(json.dumps(plan_data), encoding="utf-8")

            result = await hubspot_dedupe_plan(
                object_type="contacts",
                key_column="email",
            )
            parsed = json.loads(result)

            assert parsed["ok"] is True
            assert parsed["safety"]["impact"] == "analyze"
            # Plan hash should be present
            assert parsed["safety"]["plan_hash"].startswith("sha256:")
            # Data should include plan info
            assert "plan_hash" in parsed["data"]

    @pytest.mark.asyncio
    async def test_passes_key_column_and_keep(self):
        with patch("g_gremlin_hubspot_mcp.tools.analyze.run_gremlin", new_callable=AsyncMock) as mock, \
             patch("g_gremlin_hubspot_mcp.tools.analyze.create_temp_dir") as mock_dir, \
             patch("g_gremlin_hubspot_mcp.tools.analyze.cleanup_run_dir"):

            mock_dir.return_value = Path("/tmp/test")
            mock.return_value = RunResult(stdout="{}", stderr="", exit_code=1)

            await hubspot_dedupe_plan(
                object_type="companies",
                key_column="domain",
                keep="newest-activity",
                where=["lifecyclestage=customer"],
            )

            args = mock.call_args[0][0]
            assert "--key-column" in args
            assert "domain" in args
            assert "--keep" in args
            assert "newest-activity" in args
            assert "--where" in args
            assert "lifecyclestage=customer" in args


class TestPropsDrift:
    @pytest.mark.asyncio
    async def test_passes_spec_path(self):
        with patch("g_gremlin_hubspot_mcp.tools.analyze.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout='{"drifts": []}', stderr="", exit_code=0)
            await hubspot_props_drift("/path/to/spec.yaml")

            args = mock.call_args[0][0]
            assert "/path/to/spec.yaml" in args


class TestSnapshotDiff:
    @pytest.mark.asyncio
    async def test_passes_both_paths(self):
        with patch("g_gremlin_hubspot_mcp.tools.analyze.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout='{"changes": []}', stderr="", exit_code=0)
            await hubspot_snapshot_diff("/snap/a", "/snap/b")

            args = mock.call_args[0][0]
            assert "/snap/a" in args
            assert "/snap/b" in args
