"""FastMCP server for g-gremlin HubSpot tools.

Entry point: `g-gremlin-hubspot-mcp` (stdio transport)
"""

from __future__ import annotations

import asyncio
import logging
import sys

from mcp.server.fastmcp import FastMCP

from g_gremlin_hubspot_mcp import __version__
from g_gremlin_hubspot_mcp.runner import check_gremlin_version

# Import tool implementations
from g_gremlin_hubspot_mcp.tools.read import (
    hubspot_auth_doctor,
    hubspot_auth_whoami,
    hubspot_engagements_pull,
    hubspot_objects_pull,
    hubspot_objects_query,
    hubspot_props_list,
    hubspot_schema_get,
    hubspot_schema_list,
)
from g_gremlin_hubspot_mcp.tools.analyze import (
    hubspot_dedupe_plan,
    hubspot_props_drift,
    hubspot_snapshot_create,
    hubspot_snapshot_diff,
)
from g_gremlin_hubspot_mcp.tools.mutate import (
    hubspot_dedupe_apply,
    hubspot_objects_upsert,
)

logger = logging.getLogger(__name__)

# Create the FastMCP server
mcp = FastMCP(
    "g-gremlin-hubspot",
    version=__version__,
)

# ──────────────────────────────────────────────
# Tier 1: Read & Discover
# ──────────────────────────────────────────────

@mcp.tool(name="hubspot.auth.whoami")
async def tool_whoami() -> str:
    """[READ] [FREE] Check HubSpot authentication and show portal identity.
    Returns the connected HubSpot portal ID, hub name, and token scopes.
    Use this to verify that the MCP server can reach HubSpot."""
    return await hubspot_auth_whoami()


@mcp.tool(name="hubspot.auth.doctor")
async def tool_doctor() -> str:
    """[READ] [REQUIRES LICENSE] Run HubSpot health diagnostics.
    Checks connectivity, token validity, scopes, and API accessibility.
    Returns a structured health report with pass/fail checks.
    Requires: HubSpot Admin license or active trial."""
    return await hubspot_auth_doctor()


@mcp.tool(name="hubspot.schema.list")
async def tool_schema_list() -> str:
    """[READ] [FREE] List all HubSpot CRM object types (contacts, companies, deals, custom objects).
    Returns object type names, labels, and whether they are standard or custom."""
    return await hubspot_schema_list()


@mcp.tool(name="hubspot.schema.get")
async def tool_schema_get(object_type: str) -> str:
    """[READ] [FREE] Show the full schema for a HubSpot CRM object type.
    Returns properties, associations, and metadata for the object type.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies", "deals", or a custom object ID).
    """
    return await hubspot_schema_get(object_type)


@mcp.tool(name="hubspot.props.list")
async def tool_props_list(object_types: str, match: str | None = None) -> str:
    """[READ] [REQUIRES LICENSE] List properties for HubSpot CRM object types.
    Returns property names, types, labels, and group assignments.
    Requires: HubSpot Admin license or active trial.

    Args:
        object_types: Comma-separated object types (e.g., "contacts", "contacts,companies").
        match: Optional filter string to match property names/labels.
    """
    return await hubspot_props_list(object_types, match)


@mcp.tool(name="hubspot.objects.query")
async def tool_objects_query(
    object_type: str,
    where: list[str] | None = None,
    properties: str | None = None,
    limit: int = 100,
) -> str:
    """[READ] [FREE] Search HubSpot CRM objects (Search API; capped at 10k).
    Use hubspot.objects.pull for full extraction past the ceiling.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies", "deals").
        where: Filter clauses in property=value form (e.g., ["email=@acme.com"]).
        properties: Comma-separated properties to include.
        limit: Max records (default 100, max 10000).
    """
    return await hubspot_objects_query(object_type, where, properties, limit)


@mcp.tool(name="hubspot.objects.pull")
async def tool_objects_pull(
    object_type: str,
    properties: str | None = None,
    associations: str | None = None,
    limit: int = 0,
    timeout_seconds: int | None = None,
) -> str:
    """[READ] [FREE] Pull HubSpot CRM objects — breaks the 10k Search API ceiling.
    Uses recursive date-range windowing to retrieve all records beyond the
    10,000 hard cap. Records are deduplicated across windows automatically.
    Returns an artifact reference to the output CSV for large pulls.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies", "deals").
        properties: Comma-separated properties to include (e.g., "email,firstname,lastname").
        associations: Comma-separated associated object types (e.g., "contacts,deals").
        limit: Max records (0 = no limit, pulls everything).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    return await hubspot_objects_pull(object_type, properties, associations, limit, timeout_seconds)


@mcp.tool(name="hubspot.engagements.pull")
async def tool_engagements_pull(
    engagement_types: str | None = None,
    properties: str | None = None,
    limit: int = 0,
    auto_export_fallback: bool = True,
    timeout_seconds: int | None = None,
) -> str:
    """[READ] [FREE] Pull HubSpot engagements (emails, calls, meetings, notes, tasks).
    Auto-falls back to async export when hitting the 10k ceiling per type.

    Args:
        engagement_types: Comma-separated types (e.g., "emails,calls,meetings"). Omit for all.
        properties: Comma-separated properties to include.
        limit: Max records per type (0 = no limit).
        auto_export_fallback: Switch to async export when hitting 10k (default true).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    return await hubspot_engagements_pull(engagement_types, properties, limit, auto_export_fallback, timeout_seconds)


# ──────────────────────────────────────────────
# Tier 2: Analyze & Plan
# ──────────────────────────────────────────────

@mcp.tool(name="hubspot.dedupe.plan")
async def tool_dedupe_plan(
    object_type: str,
    key_column: str,
    keep: str = "oldest-created",
    where: list[str] | None = None,
    limit: int = 1000,
    auto_window: bool = True,
    timeout_seconds: int | None = None,
) -> str:
    """[ANALYZE] [FREE] Scan HubSpot for duplicates and generate a merge plan.
    Groups records by key_column and identifies duplicates. Returns a merge
    plan with a plan_hash — pass this to hubspot.dedupe.apply to execute.
    Uses auto-windowing to break the 10k ceiling.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies").
        key_column: Property to group duplicates by (e.g., "email", "domain").
        keep: Primary record strategy: "oldest-created" | "newest-activity" | "first".
        where: Filter clauses in property=value form.
        limit: Max records to scan (0 = unlimited with auto-window).
        auto_window: Enable date-range windowing past 10k (default true).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    return await hubspot_dedupe_plan(object_type, key_column, keep, where, limit, auto_window, timeout_seconds)


@mcp.tool(name="hubspot.props.drift")
async def tool_props_drift(spec_path: str, timeout_seconds: int | None = None) -> str:
    """[ANALYZE] [REQUIRES LICENSE] Detect property drift between a spec file and the live HubSpot portal.
    Reports additions, removals, and modifications.
    Requires: HubSpot Admin license or active trial.

    Args:
        spec_path: Path to the property spec file (YAML or JSON).
        timeout_seconds: Override timeout (default 60s).
    """
    return await hubspot_props_drift(spec_path, timeout_seconds)


@mcp.tool(name="hubspot.snapshot.create")
async def tool_snapshot_create(
    object_types: str | None = None,
    timeout_seconds: int | None = None,
) -> str:
    """[READ] [REQUIRES LICENSE] Capture a snapshot of the current HubSpot CRM state.
    Saves schema, properties, and object counts. Compare with hubspot.snapshot.diff.
    Requires: HubSpot Admin license or active trial.

    Args:
        object_types: Comma-separated types to snapshot (omit for all standard types).
        timeout_seconds: Override timeout (default 600s / 10 min).
    """
    return await hubspot_snapshot_create(object_types, timeout_seconds)


@mcp.tool(name="hubspot.snapshot.diff")
async def tool_snapshot_diff(
    snapshot_a: str,
    snapshot_b: str,
    timeout_seconds: int | None = None,
) -> str:
    """[ANALYZE] [REQUIRES LICENSE] Compare two HubSpot CRM snapshots and show changes.
    Requires: HubSpot Admin license or active trial.

    Args:
        snapshot_a: Path to the first (older) snapshot directory.
        snapshot_b: Path to the second (newer) snapshot directory.
        timeout_seconds: Override timeout (default 600s / 10 min).
    """
    return await hubspot_snapshot_diff(snapshot_a, snapshot_b, timeout_seconds)


# ──────────────────────────────────────────────
# Tier 3: Mutate (two-phase apply)
# ──────────────────────────────────────────────

@mcp.tool(name="hubspot.objects.upsert")
async def tool_objects_upsert(
    object_type: str,
    csv_path: str,
    id_column: str,
    apply: bool = False,
    plan_hash: str | None = None,
    batch_size: int | None = None,
    timeout_seconds: int | None = None,
) -> str:
    """[WRITE] [REQUIRES LICENSE] Bulk upsert HubSpot CRM records from CSV. Dry-run by default.
    Two-phase safety: (1) call with apply=false to preview + get plan_hash,
    then (2) call with apply=true and the plan_hash to execute.
    Requires: HubSpot Admin license or active trial.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies").
        csv_path: Path to CSV file with records.
        id_column: Column for matching (e.g., "hs_object_id", "email").
        apply: Execute the upsert (default false = dry-run).
        plan_hash: Required when apply=true. Must match dry-run hash.
        batch_size: Optional batch size override (max 100).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    return await hubspot_objects_upsert(object_type, csv_path, id_column, apply, plan_hash, batch_size, timeout_seconds)


@mcp.tool(name="hubspot.dedupe.apply")
async def tool_dedupe_apply(
    plan_file: str,
    apply: bool = False,
    plan_hash: str | None = None,
    timeout_seconds: int | None = None,
) -> str:
    """[MERGE] [REQUIRES LICENSE] Execute a merge plan from hubspot.dedupe.plan.
    Two-phase safety: requires plan_hash from the plan generation.
    Verifies the plan file hasn't changed since the hash was computed.
    Requires: HubSpot Admin license or active trial.

    Args:
        plan_file: Path to merge plan JSON (from hubspot.dedupe.plan).
        apply: Execute the merges (default false = dry-run review).
        plan_hash: Required when apply=true. Must match plan generation hash.
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    return await hubspot_dedupe_apply(plan_file, apply, plan_hash, timeout_seconds)


# ──────────────────────────────────────────────
# Server entry point
# ──────────────────────────────────────────────

def main() -> None:
    """CLI entry point for g-gremlin-hubspot-mcp."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stderr,  # MCP uses stdout for protocol; logs go to stderr
    )

    # Version check on startup
    try:
        version = asyncio.run(check_gremlin_version())
        logger.info("Starting g-gremlin-hubspot-mcp v%s (g-gremlin %s)", __version__, version)
    except RuntimeError as exc:
        logger.error("Startup failed: %s", exc)
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    # Run the MCP server (stdio transport)
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
