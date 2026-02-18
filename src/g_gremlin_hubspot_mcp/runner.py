"""Async subprocess runner for g-gremlin CLI commands.

Handles:
- Async subprocess execution with per-tool timeouts
- Version gating against MIN_GREMLIN_VERSION
- Structured output capture (stdout, stderr, exit code)
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import sys
from asyncio.subprocess import PIPE
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from packaging.version import Version, InvalidVersion

from g_gremlin_hubspot_mcp import MIN_GREMLIN_VERSION

logger = logging.getLogger(__name__)

# Cache the resolved g-gremlin path for the lifetime of the process
_gremlin_path: str | None = None

# Per-tool timeout defaults (seconds)
TIMEOUTS = {
    "whoami": 30,
    "doctor": 60,
    "schema.list": 60,
    "schema.get": 60,
    "props.list": 60,
    "objects.query": 60,
    "objects.pull": 900,  # 15 min — auto-windowing can be slow
    "engagements.pull": 900,
    "dedupe.plan": 900,
    "props.drift": 60,
    "snapshot.create": 600,  # 10 min
    "snapshot.diff": 600,
    "objects.upsert": 900,
    "dedupe.apply": 900,
}

DEFAULT_TIMEOUT = 120


@dataclass(frozen=True)
class RunResult:
    """Raw result from a g-gremlin subprocess call."""

    stdout: str
    stderr: str
    exit_code: int

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def _find_gremlin() -> str:
    """Locate the g-gremlin executable, preferring the current venv.

    Resolution order:
    1. Same-venv Scripts/bin directory (avoids PATH weirdness on Windows/Mac)
    2. ``python -m g_gremlin`` via sys.executable (guaranteed same environment)
    3. PATH lookup via shutil.which (fallback)
    """
    global _gremlin_path
    if _gremlin_path is not None:
        return _gremlin_path

    # 1. Check the Scripts/bin dir next to sys.executable
    exe_dir = Path(sys.executable).parent
    for candidate in ("g-gremlin", "g-gremlin.exe"):
        full = exe_dir / candidate
        if full.is_file():
            _gremlin_path = str(full)
            logger.debug("Found g-gremlin in same venv: %s", _gremlin_path)
            return _gremlin_path

    # 2. PATH fallback
    path = shutil.which("g-gremlin")
    if path:
        _gremlin_path = path
        logger.debug("Found g-gremlin on PATH: %s", _gremlin_path)
        return _gremlin_path

    raise RuntimeError(
        "g-gremlin not found. Install with: pipx install g-gremlin"
    )


async def run_raw(cmd: Sequence[str], *, timeout: int = DEFAULT_TIMEOUT) -> RunResult:
    """Execute a command and return raw RunResult."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=PIPE,
        stderr=PIPE,
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        return RunResult(
            stdout="",
            stderr=f"Command timed out after {timeout}s: {' '.join(cmd)}",
            exit_code=-1,
        )

    return RunResult(
        stdout=stdout_bytes.decode("utf-8", errors="replace"),
        stderr=stderr_bytes.decode("utf-8", errors="replace"),
        exit_code=proc.returncode or 0,
    )


async def run_gremlin(
    args: Sequence[str],
    *,
    tool_name: str = "",
    timeout: int | None = None,
) -> RunResult:
    """Run a g-gremlin command with appropriate timeout.

    Args:
        args: CLI arguments after 'g-gremlin' (e.g., ["hubspot", "whoami", "--json"])
        tool_name: MCP tool name for timeout lookup (e.g., "whoami")
        timeout: Override timeout in seconds
    """
    gremlin = _find_gremlin()
    cmd = [gremlin, *args]

    effective_timeout = timeout or TIMEOUTS.get(tool_name, DEFAULT_TIMEOUT)
    logger.debug("Running: %s (timeout=%ds)", " ".join(cmd), effective_timeout)

    return await run_raw(cmd, timeout=effective_timeout)


async def check_gremlin_version() -> str:
    """Verify g-gremlin is installed and meets minimum version.

    Returns the detected version string.
    Raises RuntimeError if missing or too old.
    """
    gremlin = _find_gremlin()
    result = await run_raw([gremlin, "--version"], timeout=10)

    if not result.ok:
        raise RuntimeError(
            f"g-gremlin --version failed (exit {result.exit_code}): {result.stderr}"
        )

    version_str = result.stdout.strip()
    # Handle output like "g-gremlin 0.1.13" or just "0.1.13"
    parts = version_str.split()
    version_str = parts[-1] if parts else version_str

    try:
        detected = Version(version_str)
        required = Version(MIN_GREMLIN_VERSION)
    except InvalidVersion as exc:
        raise RuntimeError(
            f"Could not parse g-gremlin version '{version_str}': {exc}"
        ) from exc

    if detected < required:
        raise RuntimeError(
            f"g-gremlin {detected} found, but >={MIN_GREMLIN_VERSION} required. "
            f"Run: pipx upgrade g-gremlin"
        )

    logger.info("g-gremlin %s detected (>=%s required)", detected, required)
    return str(detected)
