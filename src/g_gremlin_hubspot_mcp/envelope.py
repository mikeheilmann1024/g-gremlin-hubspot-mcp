"""MCP response envelope builder.

Wraps g-gremlin CLI output into a consistent GremlinMCPResponse/v1 shape
for reliable LLM tool chaining.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from g_gremlin_hubspot_mcp import MIN_GREMLIN_VERSION
from g_gremlin_hubspot_mcp.artifacts import Artifact  # noqa: F401 — re-exported
from g_gremlin_hubspot_mcp.runner import RunResult


@dataclass
class Safety:
    """Safety metadata for mutation tools."""

    dry_run: bool = False
    requires_apply: bool = False
    impact: str = "read"  # read | analyze | write | merge | schema
    plan_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "dry_run": self.dry_run,
            "requires_apply": self.requires_apply,
            "impact": self.impact,
        }
        if self.plan_hash:
            d["plan_hash"] = self.plan_hash
        return d


@dataclass
class Warning:
    """Structured warning."""

    code: str
    message: str
    severity: str = "warning"

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "severity": self.severity}


def compute_plan_hash(plan_data: Any) -> str:
    """SHA-256 hash of plan JSON for two-phase apply verification."""
    canonical = json.dumps(plan_data, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _extract_agentic_result(stdout: str) -> dict[str, Any] | None:
    """Extract AgenticResult JSON from g-gremlin stdout.

    g-gremlin emits AgenticResult as a JSON block (usually the last JSON
    object in stdout when --json-summary is used).
    """
    # Try to find JSON with AgenticResult schema marker
    for match in re.finditer(r'\{[^{}]*"\$schema"\s*:\s*"AgenticResult/v1"[^}]*\}', stdout, re.DOTALL):
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            continue

    # Fallback: try parsing the last JSON object in stdout
    lines = stdout.strip().splitlines()
    json_start = None
    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if stripped.startswith("{"):
            json_start = i
            break
    if json_start is not None:
        candidate = "\n".join(lines[json_start:])
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    return None


def _extract_json_output(stdout: str) -> Any | None:
    """Try to parse stdout as JSON (for --json flag commands)."""
    stripped = stdout.strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Try extracting just the JSON portion (skip non-JSON prefix lines)
    lines = stripped.splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith(("{", "[")):
            candidate = "\n".join(lines[i:])
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    return None


def build_envelope(
    *,
    run_result: RunResult,
    summary: str = "",
    artifact: Artifact | None = None,
    safety: Safety | None = None,
    extra_data: dict[str, Any] | None = None,
    extra_warnings: list[Warning] | None = None,
) -> str:
    """Build a GremlinMCPResponse/v1 JSON string from a RunResult."""
    agentic = _extract_agentic_result(run_result.stdout)
    json_output = _extract_json_output(run_result.stdout)

    # Determine ok status
    ok = run_result.ok

    # Build data from agentic result or raw JSON
    data: dict[str, Any] = {}
    if agentic:
        data = agentic.get("result", {})
        # Extract warnings from agentic result
        agentic_warnings = agentic.get("warnings", [])
    elif json_output:
        data = json_output if isinstance(json_output, dict) else {"items": json_output}
        agentic_warnings = []
    else:
        data = {"text": run_result.stdout}
        agentic_warnings = []

    if extra_data:
        data.update(extra_data)

    # Build summary
    if not summary:
        if agentic:
            status = agentic.get("status", "")
            cmd = agentic.get("command", "")
            summary = f"{cmd}: {status}" if cmd else status
        elif ok:
            summary = "Command completed successfully"
        else:
            summary = f"Command failed (exit {run_result.exit_code})"

    if not ok and not summary.startswith("Error"):
        stderr_snippet = run_result.stderr.strip()[:200] if run_result.stderr else ""
        if stderr_snippet:
            summary = f"{summary} — {stderr_snippet}"

    # Collect warnings
    warnings: list[dict[str, Any]] = []
    for w in agentic_warnings:
        if isinstance(w, dict):
            warnings.append(w)
    if extra_warnings:
        warnings.extend([w.to_dict() for w in extra_warnings])

    # Build envelope
    envelope: dict[str, Any] = {
        "$schema": "GremlinMCPResponse/v1",
        "ok": ok,
        "summary": summary,
        "data": data,
    }

    if artifact:
        envelope["artifact"] = artifact.to_dict()

    if warnings:
        envelope["warnings"] = warnings

    effective_safety = safety or Safety(impact="read")
    envelope["safety"] = effective_safety.to_dict()

    envelope["raw"] = {
        "agentic_result": agentic,
        "exit_code": run_result.exit_code,
        "stderr": run_result.stderr[:500] if run_result.stderr else "",
    }

    envelope["meta"] = {
        "requires_g_gremlin": f">={MIN_GREMLIN_VERSION}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return json.dumps(envelope, indent=2)


def error_envelope(summary: str, *, safety: Safety | None = None) -> str:
    """Build an error envelope without a RunResult."""
    envelope: dict[str, Any] = {
        "$schema": "GremlinMCPResponse/v1",
        "ok": False,
        "summary": summary,
        "data": {},
        "safety": (safety or Safety(impact="read")).to_dict(),
        "raw": {"agentic_result": None, "exit_code": -1, "stderr": ""},
        "meta": {
            "requires_g_gremlin": f">={MIN_GREMLIN_VERSION}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }
    return json.dumps(envelope, indent=2)
