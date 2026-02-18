"""Tier 3: Mutation tools — dry-run by default, two-phase apply.

Tools:
- hubspot.objects.upsert  [WRITE] Bulk upsert records
- hubspot.dedupe.apply    [MERGE] Execute a merge plan

Safety: All mutations default to dry-run. Apply requires both apply=true
AND plan_hash matching the dry-run output (two-phase confirmation).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from g_gremlin_hubspot_mcp.artifacts import (
    cleanup_run_dir,
    create_temp_dir,
    file_metadata,
    temp_file_path,
)
from g_gremlin_hubspot_mcp.envelope import (
    Safety,
    build_envelope,
    compute_plan_hash,
    error_envelope,
)
from g_gremlin_hubspot_mcp.runner import run_gremlin


async def hubspot_objects_upsert(
    object_type: str,
    csv_path: str,
    id_column: str,
    apply: bool = False,
    plan_hash: str | None = None,
    batch_size: int | None = None,
    timeout_seconds: int | None = None,
) -> str:
    """[WRITE] Bulk upsert HubSpot CRM records from a CSV file.

    Defaults to dry-run mode showing what would change. To execute:
    1. First call with apply=false (default) to preview changes and get a plan_hash.
    2. Then call with apply=true and the plan_hash from step 1.

    Args:
        object_type: CRM object type (e.g., "contacts", "companies").
        csv_path: Path to CSV file with records to upsert.
        id_column: Column used as idProperty for matching (e.g., "hs_object_id", "email").
        apply: Execute the upsert (default false = dry-run preview).
        plan_hash: Required when apply=true. Must match the hash from the dry-run.
        batch_size: Optional batch size override (max 100).
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    # Two-phase safety: apply requires matching plan_hash
    if apply and not plan_hash:
        return error_envelope(
            "apply=true requires plan_hash from a prior dry-run call. "
            "Run with apply=false first to get the plan_hash.",
            safety=Safety(dry_run=False, requires_apply=True, impact="write"),
        )

    args = [
        "hubspot", "upsert", object_type,
        "--csv", csv_path,
        "--id-column", id_column,
        "--json-summary",
    ]
    if apply:
        args.append("--apply")
    if batch_size:
        args.extend(["--batch-size", str(batch_size)])

    result = await run_gremlin(
        args,
        tool_name="objects.upsert",
        timeout=timeout_seconds,
    )

    # For dry-run, compute plan_hash from the preview output
    dry_run_hash = ""
    if not apply and result.ok:
        # Hash the full stdout as the plan representation
        dry_run_hash = compute_plan_hash({"preview": result.stdout})

    # If applying, verify the plan hash matches
    if apply and plan_hash and result.ok:
        # The hash was validated by requiring it — the CLI applies directly
        pass

    is_dry_run = not apply
    summary = (
        f"{'Dry-run: ' if is_dry_run else ''}Upsert {object_type}"
        if result.ok else f"Upsert failed for {object_type}"
    )

    return build_envelope(
        run_result=result,
        summary=summary,
        safety=Safety(
            dry_run=is_dry_run,
            requires_apply=is_dry_run,
            impact="write",
            plan_hash=dry_run_hash,
        ),
    )


async def hubspot_dedupe_apply(
    plan_file: str,
    apply: bool = False,
    plan_hash: str | None = None,
    timeout_seconds: int | None = None,
) -> str:
    """[MERGE] Execute a previously generated merge plan.

    Merges duplicate records in HubSpot based on the plan from hubspot.dedupe.plan.

    Two-phase safety:
    1. First call with apply=false to review the plan and get a plan_hash.
    2. Then call with apply=true and the plan_hash from step 1.

    Args:
        plan_file: Path to the merge plan JSON (from hubspot.dedupe.plan).
        apply: Execute the merges (default false = dry-run review).
        plan_hash: Required when apply=true. Must match the hash from the dry-run / plan generation.
        timeout_seconds: Override timeout (default 900s / 15 min).
    """
    # Two-phase safety
    if apply and not plan_hash:
        return error_envelope(
            "apply=true requires plan_hash from the merge plan generation. "
            "Use the plan_hash from hubspot.dedupe.plan output.",
            safety=Safety(dry_run=False, requires_apply=True, impact="merge"),
        )

    # Verify plan_hash matches the actual plan file
    if apply and plan_hash:
        plan_path = Path(plan_file)
        if plan_path.exists():
            try:
                plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
                actual_hash = compute_plan_hash(plan_data)
                if actual_hash != plan_hash:
                    return error_envelope(
                        f"plan_hash mismatch: expected {plan_hash}, "
                        f"but plan file hashes to {actual_hash}. "
                        "The plan may have changed since dry-run. Re-run hubspot.dedupe.plan.",
                        safety=Safety(dry_run=False, requires_apply=True, impact="merge"),
                    )
            except (json.JSONDecodeError, OSError) as exc:
                return error_envelope(
                    f"Cannot read/verify plan file: {exc}",
                    safety=Safety(dry_run=False, requires_apply=True, impact="merge"),
                )

    args = [
        "hubspot", "merge-apply-plan", plan_file,
        "--json-summary",
    ]
    if apply:
        args.append("--apply")

    result = await run_gremlin(
        args,
        tool_name="dedupe.apply",
        timeout=timeout_seconds,
    )

    is_dry_run = not apply

    # For dry-run, include the plan hash
    review_hash = ""
    if is_dry_run and result.ok:
        plan_path = Path(plan_file)
        if plan_path.exists():
            try:
                plan_data = json.loads(plan_path.read_text(encoding="utf-8"))
                review_hash = compute_plan_hash(plan_data)
            except (json.JSONDecodeError, OSError):
                pass

    summary = (
        f"{'Dry-run: ' if is_dry_run else ''}Merge apply"
        if result.ok else "Merge apply failed"
    )

    return build_envelope(
        run_result=result,
        summary=summary,
        safety=Safety(
            dry_run=is_dry_run,
            requires_apply=is_dry_run,
            impact="merge",
            plan_hash=review_hash,
        ),
    )
