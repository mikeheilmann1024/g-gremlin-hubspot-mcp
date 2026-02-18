"""Temp file and artifact management for MCP tool outputs.

Handles:
- Managed temp directory for CSV/JSON outputs
- Cleanup policy (auto-delete vs --keep-files)
- Inline vs artifact decision based on file size
- Cross-platform path handling via pathlib
"""

from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Artifact:
    """File artifact produced by a tool (e.g., CSV from pull)."""

    type: str = "file"
    path: str = ""
    row_count: int = 0
    columns: list[str] = field(default_factory=list)
    size_bytes: int = 0
    mime: str = "text/csv"

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v}

# Default artifact directory
DEFAULT_ARTIFACT_DIR = Path.home() / ".g_gremlin" / "mcp_tmp"

# Files smaller than this are inlined in data; larger become artifacts
MAX_INLINE_BYTES = 64 * 1024  # 64 KB

# Whether to keep temp files after tool execution (debug mode)
_keep_files = os.environ.get("GREMLIN_MCP_KEEP_FILES", "").lower() in ("1", "true", "yes")


def get_artifact_dir() -> Path:
    """Get or create the managed artifact directory."""
    artifact_dir = Path(
        os.environ.get("GREMLIN_MCP_ARTIFACT_DIR", str(DEFAULT_ARTIFACT_DIR))
    )
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def create_temp_dir() -> Path:
    """Create a unique temp directory for a single tool invocation."""
    base = get_artifact_dir()
    run_dir = base / uuid.uuid4().hex[:12]
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def temp_file_path(run_dir: Path, filename: str) -> Path:
    """Get a path for a temp file within a run directory."""
    return run_dir / filename


def cleanup_run_dir(run_dir: Path) -> None:
    """Remove a run directory unless keep_files is enabled."""
    if _keep_files:
        return
    try:
        shutil.rmtree(run_dir, ignore_errors=True)
    except Exception:
        pass


def should_inline(file_path: Path) -> bool:
    """Decide whether a file should be inlined in data or referenced as an artifact."""
    try:
        return file_path.stat().st_size <= MAX_INLINE_BYTES
    except OSError:
        return True


def file_metadata(file_path: Path) -> dict:
    """Get metadata about a file for artifact responses."""
    try:
        stat = file_path.stat()
        size = stat.st_size
    except OSError:
        size = 0

    suffix = file_path.suffix.lower()
    mime_map = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".txt": "text/plain",
    }

    return {
        "path": str(file_path),
        "size_bytes": size,
        "mime": mime_map.get(suffix, "application/octet-stream"),
    }


def read_csv_metadata(file_path: Path) -> dict:
    """Read CSV file and extract column names and row count."""
    meta = file_metadata(file_path)
    try:
        with file_path.open("r", encoding="utf-8") as f:
            header = f.readline().strip()
            columns = [c.strip() for c in header.split(",") if c.strip()]
            row_count = sum(1 for _ in f)  # count remaining lines
        meta["columns"] = columns
        meta["row_count"] = row_count
    except Exception:
        meta["columns"] = []
        meta["row_count"] = 0
    return meta
