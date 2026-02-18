"""Tier 2: Analyze & Plan tools — read-only analysis, generates plans.

Tools:
- hubspot.dedupe.plan     [ANALYZE] Scan for duplicates, generate merge plan
- hubspot.props.drift     [ANALYZE] Detect property drift vs spec
- hubspot.snapshot.create  [READ]    Capture CRM state
- hubspot.snapshot.diff    [ANALYZE] Diff two CRM snapshots
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from g_gremlin_hubspot_mcp.artifacts import (
    Artifact,
    cleanup_run_dir,
    create_temp_dir,
    file_metadata,
    should_inline,
    temp_file_path,
)
from g_gremlin_hubspot_mcp.envelope import (
    Safety,
    build_envelope,
    compute_plan_hash,
)
from g_gremlin_hubspot_mcp.runner import run_gremlin


async def hubspot_dedupe_plan(
    object_type: str,
    key_column: str,
    keep: str = "oldest-created",
    where: list[str] | None = None,
    limit: int = 1000,
    auto_window: bool = True,
    timeout_seconds: int | None = None,
) -> str:
    """[ANALYZE] Scan HubSpot for duplicate records and generate a merge plan.

    Groups records by key_column (e.g., email, domain) and identifies duplicates.
    Returns a merge plan JSON with primary/secondary record assignments.

    The plan includes a plan_hash — pass this to hubspot.dedupe.apply to execute.

    Uses auto-windowing to break the 10k ceiling when scanning large portals.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies").
        key_column: Property to group duplicates by (e.g., "email", "domain", "external_id").
        keep: Primary record selection strategy: "oldest-created" | "newest-activity" | "first".
        where: Filter clauses in property=value form (e.g., ["lifecyclestage=customer"]).
        limit: Max records to scan (0 = unlimited, up to API ceiling with auto-window).
        auto_window: Enable recursive date-range windowing past the 10k ceiling (default true).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    run_dir = create_temp_dir()
    plan_path = temp_file_path(run_dir, "merge_plan.json")

    args = [
        "hubspot", "merge-plan", object_type,
        "--key-column", key_column,
        "--keep", keep,
        "--limit", str(limit),
        "--out", str(plan_path),
        "--json-summary",
    ]
    if where:
        for clause in where:
            args.extend(["--where", clause])
    if auto_window:
        args.append("--auto-window-on-cap")
    else:
        args.append("--no-auto-window-on-cap")

    result = await run_gremlin(
        args,
        tool_name="dedupe.plan",
        timeout=timeout_seconds,
    )

    extra_data: dict[str, Any] = {}
    artifact = None
    plan_hash = ""

    if result.ok and plan_path.exists():
        try:
            plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
            plan_hash = compute_plan_hash(plan_data)

            if should_inline(plan_path):
                extra_data["plan"] = plan_data
                extra_data["plan_hash"] = plan_hash
                # Extract summary stats from plan
                groups = plan_data.get("groups", [])
                total_merges = sum(
                    len(g.get("secondaries", [])) for g in groups
                )
                extra_data["duplicate_groups"] = len(groups)
                extra_data["total_merges"] = total_merges
                cleanup_run_dir(run_dir)
            else:
                meta = file_metadata(plan_path)
                artifact = Artifact(
                    path=str(plan_path),
                    size_bytes=meta.get("size_bytes", 0),
                    mime="application/json",
                )
                extra_data["plan_hash"] = plan_hash
        except (json.JSONDecodeError, OSError):
            extra_data["plan_path"] = str(plan_path)
    elif not result.ok:
        cleanup_run_dir(run_dir)

    summary = (
        f"Found {extra_data.get('duplicate_groups', '?')} duplicate groups, "
        f"{extra_data.get('total_merges', '?')} merges planned"
        if result.ok else f"Dedupe plan failed for {object_type}"
    )

    return build_envelope(
        run_result=result,
        summary=summary,
        artifact=artifact,
        extra_data=extra_data if extra_data else None,
        safety=Safety(
            impact="analyze",
            plan_hash=plan_hash,
        ),
    )


async def hubspot_props_drift(
    spec_path: str,
    timeout_seconds: int | None = None,
) -> str:
    """[ANALYZE] Detect property drift between a YAML/JSON spec and the live HubSpot portal.

    Compares a local property specification file against the actual CRM properties
    and reports additions, removals, and modifications.

    Args:
        spec_path: Path to the property spec file (YAML or JSON).
        timeout_seconds: Override timeout (default 60s).
    """
    args = [
        "hubspot", "props", "drift", spec_path, "--json",
    ]

    result = await run_gremlin(
        args,
        tool_name="props.drift",
        timeout=timeout_seconds,
    )
    return build_envelope(
        run_result=result,
        summary="Property drift analysis" if result.ok else "Property drift check failed",
        safety=Safety(impact="analyze"),
    )


async def hubspot_snapshot_create(
    object_types: str | None = None,
    timeout_seconds: int | None = None,
) -> str:
    """[READ] Capture a snapshot of the current HubSpot CRM state.

    Saves schema, properties, and object counts to a local directory.
    Snapshots can be compared with hubspot.snapshot.diff to detect changes.

    Args:
        object_types: Comma-separated object types to snapshot (omit for all standard types).
        timeout_seconds: Override timeout (default 600s / 10 min).
    """
    run_dir = create_temp_dir()
    snapshot_dir = run_dir / "snapshot"
    snapshot_dir.mkdir(exist_ok=True)

    args = [
        "hubspot", "snapshot",
        "--out-dir", str(snapshot_dir),
        "--json",
    ]
    if object_types:
        args.extend(["--objects", object_types])

    result = await run_gremlin(
        args,
        tool_name="snapshot.create",
        timeout=timeout_seconds,
    )

    extra_data: dict[str, Any] = {}
    if result.ok and snapshot_dir.exists():
        files = list(snapshot_dir.rglob("*"))
        extra_data["snapshot_dir"] = str(snapshot_dir)
        extra_data["file_count"] = len([f for f in files if f.is_file()])
    elif not result.ok:
        cleanup_run_dir(run_dir)

    return build_envelope(
        run_result=result,
        summary=f"Snapshot captured ({extra_data.get('file_count', 0)} files)" if result.ok else "Snapshot failed",
        extra_data=extra_data if extra_data else None,
        safety=Safety(impact="read"),
    )


async def hubspot_snapshot_diff(
    snapshot_a: str,
    snapshot_b: str,
    timeout_seconds: int | None = None,
) -> str:
    """[ANALYZE] Compare two HubSpot CRM snapshots and show what changed.

    Args:
        snapshot_a: Path to the first (older) snapshot directory.
        snapshot_b: Path to the second (newer) snapshot directory.
        timeout_seconds: Override timeout (default 600s / 10 min).
    """
    args = [
        "hubspot", "compare-snapshots", snapshot_a, snapshot_b, "--json",
    ]

    result = await run_gremlin(
        args,
        tool_name="snapshot.diff",
        timeout=timeout_seconds,
    )
    return build_envelope(
        run_result=result,
        summary="Snapshot comparison" if result.ok else "Snapshot diff failed",
        safety=Safety(impact="analyze"),
    )
