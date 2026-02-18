"""Tests for Tier 1 read tools — golden output tests."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from g_gremlin_hubspot_mcp.runner import RunResult
from g_gremlin_hubspot_mcp.tools.read import (
    hubspot_auth_doctor,
    hubspot_auth_whoami,
    hubspot_objects_query,
    hubspot_schema_get,
    hubspot_schema_list,
)

GOLDEN_DIR = Path(__file__).parent / "golden"


def _golden(name: str) -> str:
    return (GOLDEN_DIR / f"{name}.json").read_text(encoding="utf-8")


class TestWhoami:
    @pytest.mark.asyncio
    async def test_success(self):
        golden = _golden("whoami")
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout=golden, stderr="", exit_code=0)
            result = await hubspot_auth_whoami()
            parsed = json.loads(result)

            assert parsed["$schema"] == "GremlinMCPResponse/v1"
            assert parsed["ok"] is True
            assert parsed["safety"]["impact"] == "read"
            assert parsed["data"]["hub_id"] == 12345678

    @pytest.mark.asyncio
    async def test_failure(self):
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout="", stderr="Error: token expired", exit_code=1)
            result = await hubspot_auth_whoami()
            parsed = json.loads(result)

            assert parsed["ok"] is False
            assert "failed" in parsed["summary"].lower()


class TestDoctor:
    @pytest.mark.asyncio
    async def test_success(self):
        golden = _golden("doctor")
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout=golden, stderr="", exit_code=0)
            result = await hubspot_auth_doctor()
            parsed = json.loads(result)

            assert parsed["ok"] is True
            assert parsed["safety"]["impact"] == "read"


class TestSchemaList:
    @pytest.mark.asyncio
    async def test_success(self):
        golden = _golden("schema_ls")
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout=golden, stderr="", exit_code=0)
            result = await hubspot_schema_list()
            parsed = json.loads(result)

            assert parsed["ok"] is True
            # Data should contain the schema list
            assert "items" in parsed["data"] or isinstance(parsed["data"], list) or len(parsed["data"]) > 0


class TestSchemaGet:
    @pytest.mark.asyncio
    async def test_passes_object_type(self):
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout='{"name": "contacts"}', stderr="", exit_code=0)
            await hubspot_schema_get("contacts")

            args = mock.call_args[0][0]
            assert "contacts" in args


class TestObjectsQuery:
    @pytest.mark.asyncio
    async def test_passes_where_clauses(self):
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout='{"results": []}', stderr="", exit_code=0)
            await hubspot_objects_query(
                "contacts",
                where=["email=@acme.com", "lifecyclestage=customer"],
                limit=50,
            )

            args = mock.call_args[0][0]
            assert "--where" in args
            assert "email=@acme.com" in args
            assert "lifecyclestage=customer" in args
            assert "--limit" in args
            assert "50" in args

    @pytest.mark.asyncio
    async def test_default_limit_not_passed(self):
        with patch("g_gremlin_hubspot_mcp.tools.read.run_gremlin", new_callable=AsyncMock) as mock:
            mock.return_value = RunResult(stdout='{}', stderr="", exit_code=0)
            await hubspot_objects_query("contacts")

            args = mock.call_args[0][0]
            # Default limit (100) should not add --limit
            assert "--limit" not in args
