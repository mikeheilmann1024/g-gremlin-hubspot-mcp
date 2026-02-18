"""Tests for runner.py — subprocess execution and version gating."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from g_gremlin_hubspot_mcp import runner as runner_mod
from g_gremlin_hubspot_mcp.runner import (
    DEFAULT_TIMEOUT,
    RunResult,
    TIMEOUTS,
    check_gremlin_version,
    run_gremlin,
)


class TestRunResult:
    def test_ok_on_zero_exit(self):
        r = RunResult(stdout="ok", stderr="", exit_code=0)
        assert r.ok is True

    def test_not_ok_on_nonzero_exit(self):
        r = RunResult(stdout="", stderr="err", exit_code=1)
        assert r.ok is False


class TestTimeouts:
    def test_whoami_timeout_is_30s(self):
        assert TIMEOUTS["whoami"] == 30

    def test_pull_timeout_is_15min(self):
        assert TIMEOUTS["objects.pull"] == 900

    def test_snapshot_timeout_is_10min(self):
        assert TIMEOUTS["snapshot.create"] == 600

    def test_default_timeout_is_120s(self):
        assert DEFAULT_TIMEOUT == 120


class TestCheckGremlinVersion:
    @pytest.mark.asyncio
    async def test_accepts_current_version(self):
        mock_result = RunResult(stdout="0.1.14\n", stderr="", exit_code=0)
        with patch("g_gremlin_hubspot_mcp.runner.run_raw", new_callable=AsyncMock, return_value=mock_result), \
             patch("g_gremlin_hubspot_mcp.runner._find_gremlin", return_value="g-gremlin"):
            version = await check_gremlin_version()
            assert version == "0.1.14"

    @pytest.mark.asyncio
    async def test_accepts_higher_version(self):
        mock_result = RunResult(stdout="0.2.0\n", stderr="", exit_code=0)
        with patch("g_gremlin_hubspot_mcp.runner.run_raw", new_callable=AsyncMock, return_value=mock_result), \
             patch("g_gremlin_hubspot_mcp.runner._find_gremlin", return_value="g-gremlin"):
            version = await check_gremlin_version()
            assert version == "0.2.0"

    @pytest.mark.asyncio
    async def test_rejects_old_version(self):
        mock_result = RunResult(stdout="0.1.0\n", stderr="", exit_code=0)
        with patch("g_gremlin_hubspot_mcp.runner.run_raw", new_callable=AsyncMock, return_value=mock_result), \
             patch("g_gremlin_hubspot_mcp.runner._find_gremlin", return_value="g-gremlin"):
            with pytest.raises(RuntimeError, match=">=0.1.14 required"):
                await check_gremlin_version()

    @pytest.mark.asyncio
    async def test_handles_prefixed_version(self):
        mock_result = RunResult(stdout="g-gremlin 0.1.14\n", stderr="", exit_code=0)
        with patch("g_gremlin_hubspot_mcp.runner.run_raw", new_callable=AsyncMock, return_value=mock_result), \
             patch("g_gremlin_hubspot_mcp.runner._find_gremlin", return_value="g-gremlin"):
            version = await check_gremlin_version()
            assert version == "0.1.14"

    @pytest.mark.asyncio
    async def test_raises_on_missing(self):
        with patch("g_gremlin_hubspot_mcp.runner._find_gremlin", side_effect=RuntimeError("not found")):
            with pytest.raises(RuntimeError, match="not found"):
                await check_gremlin_version()


class TestRunGremlin:
    @pytest.mark.asyncio
    async def test_uses_tool_timeout(self):
        mock_result = RunResult(stdout="ok", stderr="", exit_code=0)
        with patch("g_gremlin_hubspot_mcp.runner.run_raw", new_callable=AsyncMock, return_value=mock_result) as mock_raw, \
             patch("g_gremlin_hubspot_mcp.runner._find_gremlin", return_value="g-gremlin"):
            await run_gremlin(["hubspot", "whoami"], tool_name="whoami")
            # Verify the timeout used was 30s (whoami timeout)
            _, kwargs = mock_raw.call_args
            assert kwargs["timeout"] == 30

    @pytest.mark.asyncio
    async def test_uses_override_timeout(self):
        mock_result = RunResult(stdout="ok", stderr="", exit_code=0)
        with patch("g_gremlin_hubspot_mcp.runner.run_raw", new_callable=AsyncMock, return_value=mock_result) as mock_raw, \
             patch("g_gremlin_hubspot_mcp.runner._find_gremlin", return_value="g-gremlin"):
            await run_gremlin(["hubspot", "whoami"], tool_name="whoami", timeout=999)
            _, kwargs = mock_raw.call_args
            assert kwargs["timeout"] == 999


class TestRunRaw:
    @pytest.mark.asyncio
    async def test_uses_devnull_for_stdin(self):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b"ok", b""))
        mock_proc.returncode = 0

        with patch(
            "g_gremlin_hubspot_mcp.runner.asyncio.create_subprocess_exec",
            new_callable=AsyncMock,
            return_value=mock_proc,
        ) as mock_exec:
            result = await runner_mod.run_raw(["g-gremlin", "--version"], timeout=10)

        assert result.ok is True
        _, kwargs = mock_exec.call_args
        assert kwargs["stdin"] == runner_mod.DEVNULL
