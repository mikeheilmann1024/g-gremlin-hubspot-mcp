"""Tests for Tier 3 mutation tools — safety rejection tests.

Critical tests:
- apply=true without plan_hash is rejected
- plan_hash mismatch is rejected
- dry-run returns plan_hash
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from g_gremlin_hubspot_mcp.envelope import compute_plan_hash
from g_gremlin_hubspot_mcp.runner import RunResult
from g_gremlin_hubspot_mcp.tools.mutate import (
    hubspot_dedupe_apply,
    hubspot_objects_upsert,
)


class TestUpsertSafety:
    @pytest.mark.asyncio
    async def test_apply_without_hash_rejected(self):
        """apply=true without plan_hash must be rejected."""
        result = await hubspot_objects_upsert(
            object_type="contacts",
            csv_path="/path/to/data.csv",
            id_column="email",
            apply=True,
            plan_hash=None,
        )
        parsed = json.loads(result)
        assert parsed["ok"] is False
        assert "plan_hash" in parsed["summary"].lower()

    @pytest.mark.asyncio
    async def test_dry_run_returns_hash(self):
        """Dry-run should return a plan_hash for two-phase apply."""
        with patch("g_gremlin_hubspot_mcp.tools.mutate.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(
                stdout='{"preview": "5 records to upsert"}',
                stderr="",
                exit_code=0,
            )
            result = await hubspot_objects_upsert(
                object_type="contacts",
                csv_path="/path/to/data.csv",
                id_column="email",
                apply=False,
            )
            parsed = json.loads(result)

            assert parsed["ok"] is True
            assert parsed["safety"]["dry_run"] is True
            assert parsed["safety"]["requires_apply"] is True
            assert parsed["safety"]["plan_hash"].startswith("sha256:")
            assert parsed["safety"]["impact"] == "write"

    @pytest.mark.asyncio
    async def test_apply_with_hash_proceeds(self):
        """apply=true with plan_hash should proceed to execution."""
        with patch("g_gremlin_hubspot_mcp.tools.mutate.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(
                stdout='{"applied": true, "count": 5}',
                stderr="",
                exit_code=0,
            )
            result = await hubspot_objects_upsert(
                object_type="contacts",
                csv_path="/path/to/data.csv",
                id_column="email",
                apply=True,
                plan_hash="sha256:abc123",
            )
            parsed = json.loads(result)

            assert parsed["ok"] is True
            assert parsed["safety"]["dry_run"] is False


class TestDedupeApplySafety:
    @pytest.mark.asyncio
    async def test_apply_without_hash_rejected(self):
        """apply=true without plan_hash must be rejected."""
        result = await hubspot_dedupe_apply(
            plan_file="/path/to/plan.json",
            apply=True,
            plan_hash=None,
        )
        parsed = json.loads(result)
        assert parsed["ok"] is False
        assert "plan_hash" in parsed["summary"].lower()

    @pytest.mark.asyncio
    async def test_hash_mismatch_rejected(self, tmp_path: Path):
        """apply with wrong plan_hash must be rejected."""
        plan_data = {"groups": [{"key": "a@b.com", "primary": "1", "secondaries": ["2"]}]}
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(plan_data), encoding="utf-8")

        correct_hash = compute_plan_hash(plan_data)
        wrong_hash = "sha256:0000000000000000000000000000000000000000000000000000000000000000"
        assert correct_hash != wrong_hash

        result = await hubspot_dedupe_apply(
            plan_file=str(plan_path),
            apply=True,
            plan_hash=wrong_hash,
        )
        parsed = json.loads(result)
        assert parsed["ok"] is False
        assert "mismatch" in parsed["summary"].lower()

    @pytest.mark.asyncio
    async def test_correct_hash_proceeds(self, tmp_path: Path):
        """apply with correct plan_hash should proceed."""
        plan_data = {"groups": [{"key": "a@b.com", "primary": "1", "secondaries": ["2"]}]}
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(plan_data), encoding="utf-8")

        correct_hash = compute_plan_hash(plan_data)

        with patch("g_gremlin_hubspot_mcp.tools.mutate.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(
                stdout='{"merged": 1}',
                stderr="",
                exit_code=0,
            )
            result = await hubspot_dedupe_apply(
                plan_file=str(plan_path),
                apply=True,
                plan_hash=correct_hash,
            )
            parsed = json.loads(result)

            assert parsed["ok"] is True
            assert parsed["safety"]["dry_run"] is False
            assert parsed["safety"]["impact"] == "merge"

    @pytest.mark.asyncio
    async def test_dry_run_returns_hash(self, tmp_path: Path):
        """Dry-run should return the plan_hash for two-phase."""
        plan_data = {"groups": [{"key": "test@test.com", "primary": "10", "secondaries": ["11"]}]}
        plan_path = tmp_path / "plan.json"
        plan_path.write_text(json.dumps(plan_data), encoding="utf-8")

        expected_hash = compute_plan_hash(plan_data)

        with patch("g_gremlin_hubspot_mcp.tools.mutate.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(
                stdout='{"preview": "1 merge planned"}',
                stderr="",
                exit_code=0,
            )
            result = await hubspot_dedupe_apply(
                plan_file=str(plan_path),
                apply=False,
            )
            parsed = json.loads(result)

            assert parsed["ok"] is True
            assert parsed["safety"]["dry_run"] is True
            assert parsed["safety"]["plan_hash"] == expected_hash
