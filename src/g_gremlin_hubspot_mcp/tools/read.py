"""Tier 1: Read & Discover tools — safe, no side effects.

Tools:
- hubspot.auth.whoami     [READ] Auth check, portal identity
- hubspot.auth.doctor     [READ] Health diagnostics
- hubspot.schema.list     [READ] List all CRM object types
- hubspot.schema.get      [READ] Object schema detail
- hubspot.props.list      [READ] Property introspection
- hubspot.objects.query   [READ] CRM search (capped at 10k)
- hubspot.objects.pull    [READ] Full extraction past the 10k ceiling
- hubspot.engagements.pull [READ] Engagement pull with async fallback
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from g_gremlin_hubspot_mcp.artifacts import (
    Artifact,
    cleanup_run_dir,
    create_temp_dir,
    read_csv_metadata,
    should_inline,
    temp_file_path,
)
from g_gremlin_hubspot_mcp.envelope import Safety, build_envelope
from g_gremlin_hubspot_mcp.runner import RunResult, run_gremlin


def _is_schema_cache_miss(result: RunResult) -> bool:
    """Detect first-run schema cache misses from CLI output."""
    text = f"{result.stderr}\n{result.stdout}".lower()
    return "no cached schema found" in text


async def _run_schema_with_auto_sync(
    args: list[str],
    *,
    tool_name: str,
) -> tuple[RunResult, bool]:
    """Run a schema command; auto-sync cache once on cache-miss errors."""
    result = await run_gremlin(args, tool_name=tool_name)
    if result.ok or not _is_schema_cache_miss(result):
        return result, False

    sync_result = await run_gremlin(
        ["hubspot", "schema", "sync", "--json"],
        tool_name="schema.list",
    )
    if not sync_result.ok:
        return result, False

    retry = await run_gremlin(args, tool_name=tool_name)
    return retry, True


async def hubspot_auth_whoami() -> str:
    """[READ] Check HubSpot authentication and show portal identity.

    Returns the connected HubSpot portal ID, hub name, and token scopes.
    Use this to verify that the MCP server can reach HubSpot.
    """
    result = await run_gremlin(
        ["hubspot", "whoami", "--json"],
        tool_name="whoami",
    )
    return build_envelope(
        run_result=result,
        summary="HubSpot auth check" if result.ok else "HubSpot auth check failed",
        safety=Safety(impact="read"),
    )


async def hubspot_auth_doctor() -> str:
    """[READ] Run HubSpot health diagnostics.

    Checks connectivity, token validity, scopes, and API accessibility.
    Returns a structured health report with pass/fail checks.
    """
    result = await run_gremlin(
        ["hubspot", "doctor", "--json"],
        tool_name="doctor",
    )
    return build_envelope(
        run_result=result,
        summary="HubSpot diagnostics" if result.ok else "HubSpot diagnostics failed",
        safety=Safety(impact="read"),
    )


async def hubspot_schema_list() -> str:
    """[READ] List all HubSpot CRM object types (contacts, companies, deals, custom objects, etc.).

    Returns object type names, labels, and whether they are standard or custom.
    """
    result, auto_synced = await _run_schema_with_auto_sync(
        ["hubspot", "schema", "ls", "--json"],
        tool_name="schema.list",
    )
    summary = "Listed CRM object types"
    if result.ok and auto_synced:
        summary = "Listed CRM object types (auto-synced schema cache)"
    return build_envelope(
        run_result=result,
        summary=summary if result.ok else "Schema list failed",
        safety=Safety(impact="read"),
    )


async def hubspot_schema_get(object_type: str) -> str:
    """[READ] Show the full schema for a HubSpot CRM object type.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies", "deals", or a custom object ID).

    Returns properties, associations, and metadata for the object type.
    """
    result, auto_synced = await _run_schema_with_auto_sync(
        ["hubspot", "schema", "show", object_type, "--json"],
        tool_name="schema.get",
    )
    summary = f"Schema for {object_type}"
    if result.ok and auto_synced:
        summary = f"Schema for {object_type} (auto-synced schema cache)"
    return build_envelope(
        run_result=result,
        summary=summary if result.ok else f"Schema get failed for {object_type}",
        safety=Safety(impact="read"),
    )


async def hubspot_props_list(
    object_types: str,
    match: str | None = None,
) -> str:
    """[READ] List properties for one or more HubSpot CRM object types.

    Args:
        object_types: Comma-separated object types (e.g., "contacts", "contacts,companies").
        match: Optional filter string to match property names/labels.

    Returns property names, types, labels, and group assignments.
    """
    args = ["hubspot", "props", "list", object_types, "--json"]
    if match:
        args.extend(["--match", match])

    result = await run_gremlin(args, tool_name="props.list")
    return build_envelope(
        run_result=result,
        summary=f"Properties for {object_types}" if result.ok else "Props list failed",
        safety=Safety(impact="read"),
    )


async def hubspot_objects_query(
    object_type: str,
    where: list[str] | None = None,
    properties: str | None = None,
    limit: int = 100,
) -> str:
    """[READ] Search HubSpot CRM objects with filters (Search API; capped at 10k).

    For full extraction past the 10k ceiling, use hubspot.objects.pull instead.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies", "deals").
        where: Filter clauses in property=value form (e.g., ["email=@acme.com", "lifecyclestage=customer"]).
        properties: Comma-separated properties to include in results.
        limit: Max records to return (default 100, max 10000).
    """
    args = ["hubspot", "query", object_type, "--json"]
    if where:
        for clause in where:
            args.extend(["--where", clause])
    if properties:
        args.extend(["--properties", properties])
    if limit != 100:
        args.extend(["--limit", str(limit)])

    result = await run_gremlin(args, tool_name="objects.query")
    return build_envelope(
        run_result=result,
        summary=f"Query {object_type}" if result.ok else f"Query failed for {object_type}",
        safety=Safety(impact="read"),
    )


async def hubspot_objects_pull(
    object_type: str,
    properties: str | None = None,
    associations: str | None = None,
    limit: int = 0,
    timeout_seconds: int | None = None,
) -> str:
    """[READ] Pull HubSpot CRM objects to CSV — breaks the 10k Search API ceiling.

    Uses recursive date-range windowing (splitting by createdate) to retrieve
    all records beyond the 10,000 Search API hard cap. Records are deduplicated
    across windows automatically.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies", "deals").
        properties: Comma-separated properties to include (e.g., "email,firstname,lastname").
        associations: Comma-separated associated object types to include (e.g., "contacts,deals").
        limit: Max records (0 = no limit, pulls everything).
        timeout_seconds: Override timeout (default 900s / 15 min).

    Returns an artifact reference to the output CSV file with row count and column metadata.
    """
    run_dir = create_temp_dir()
    output_path = temp_file_path(run_dir, f"{object_type}.csv")

    args = [
        "hubspot", "pull", object_type,
        "--output", str(output_path),
        "--json-summary",
    ]
    if properties:
        args.extend(["--properties", properties])
    if associations:
        args.extend(["--associations", associations])
    if limit > 0:
        args.extend(["--limit", str(limit)])

    result = await run_gremlin(
        args,
        tool_name="objects.pull",
        timeout=timeout_seconds,
    )

    artifact = None
    extra_data: dict[str, Any] = {}

    if result.ok and output_path.exists():
        meta = read_csv_metadata(output_path)

        if should_inline(output_path):
            # Small file — inline the data
            try:
                rows: list[dict[str, Any]] = []
                with output_path.open("r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        rows.append(dict(row))
                extra_data["records"] = rows
                extra_data["count"] = len(rows)
                extra_data["columns"] = meta.get("columns", [])
                cleanup_run_dir(run_dir)
            except Exception:
                artifact = Artifact(
                    path=str(output_path),
                    row_count=meta.get("row_count", 0),
                    columns=meta.get("columns", []),
                    size_bytes=meta.get("size_bytes", 0),
                )
        else:
            # Large file — artifact reference
            artifact = Artifact(
                path=str(output_path),
                row_count=meta.get("row_count", 0),
                columns=meta.get("columns", []),
                size_bytes=meta.get("size_bytes", 0),
            )
    elif not result.ok:
        cleanup_run_dir(run_dir)

    return build_envelope(
        run_result=result,
        summary=f"Pulled {extra_data.get('count', artifact.row_count if artifact else 0)} {object_type}" if result.ok else f"Pull failed for {object_type}",
        artifact=artifact,
        extra_data=extra_data if extra_data else None,
        safety=Safety(impact="read"),
    )


async def hubspot_engagements_pull(
    engagement_types: str | None = None,
    properties: str | None = None,
    limit: int = 0,
    auto_export_fallback: bool = True,
    timeout_seconds: int | None = None,
) -> str:
    """[READ] Pull HubSpot engagements (emails, calls, meetings, notes, tasks) to CSV.

    When results hit the 10k ceiling per engagement type, automatically falls back
    to HubSpot's async export API which has no record limit.

    Args:
        engagement_types: Comma-separated types (e.g., "emails,calls,meetings"). Omit for all.
        properties: Comma-separated properties to include.
        limit: Max records per type (0 = no limit).
        auto_export_fallback: Auto-switch to async export when hitting 10k ceiling (default true).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    run_dir = create_temp_dir()
    output_dir = run_dir / "engagements"
    output_dir.mkdir(exist_ok=True)

    args = [
        "hubspot", "engagements", "pull",
        "--out-dir", str(output_dir),
        "--json-summary",
    ]
    if engagement_types:
        args.extend(["--types", engagement_types])
    if properties:
        args.extend(["--properties", properties])
    if limit > 0:
        args.extend(["--limit", str(limit)])
    if auto_export_fallback:
        args.append("--auto-export-fallback")

    result = await run_gremlin(
        args,
        tool_name="engagements.pull",
        timeout=timeout_seconds,
    )

    # List output files
    artifact_files: list[dict[str, Any]] = []
    if result.ok and output_dir.exists():
        for csv_file in output_dir.glob("*.csv"):
            meta = read_csv_metadata(csv_file)
            artifact_files.append(meta)

    extra_data: dict[str, Any] = {}
    if artifact_files:
        extra_data["files"] = artifact_files
        extra_data["total_files"] = len(artifact_files)
        total_rows = sum(f.get("row_count", 0) for f in artifact_files)
        extra_data["total_rows"] = total_rows
    else:
        cleanup_run_dir(run_dir)

    return build_envelope(
        run_result=result,
        summary=f"Pulled {extra_data.get('total_rows', 0)} engagements across {extra_data.get('total_files', 0)} types" if result.ok else "Engagements pull failed",
        extra_data=extra_data if extra_data else None,
        safety=Safety(impact="read"),
    )
