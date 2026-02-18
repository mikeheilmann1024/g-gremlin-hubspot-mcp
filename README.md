# g-gremlin-hubspot-mcp

> **Status: Public Beta** — 14 tools shipping, core workflows stable, feedback welcome. See [Known Gaps](#known-gaps--roadmap) below.

**The HubSpot MCP server for teams that hit the API ceiling.**

HubSpot's official MCP gives you read-only search capped at 10k records. This one gives you dedup merge plans, auto-windowing past the ceiling, property drift detection, and bulk upserts — all from Claude Desktop, Cursor, or Windsurf.

Powered by [g-gremlin](https://github.com/foundryops/g-gremlin), the CLI for Google Workspace and CRM automation.

## Quickstart

```bash
# 1. Install
pipx install g-gremlin
pipx install g-gremlin-hubspot-mcp

# 2. Connect to HubSpot (one-time)
g-gremlin hubspot connect --access-token YOUR_PRIVATE_APP_TOKEN

# 3. Add to your MCP client
```

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "g-gremlin-hubspot": {
      "command": "g-gremlin-hubspot-mcp",
      "env": {
        "G_GREMLIN_HUBSPOT_ACCESS_TOKEN": "YOUR_TOKEN"
      }
    }
  }
}
```

### Cursor / Windsurf

Add to your MCP settings:

```json
{
  "mcpServers": {
    "g-gremlin-hubspot": {
      "command": "g-gremlin-hubspot-mcp"
    }
  }
}
```

### OpenClaw (community-supported via mcporter)

OpenClaw can call MCP servers through `mcporter`.

1. Install mcporter:

```bash
npm install -g mcporter
```

2. Add this MCP server in your mcporter config:

```json
{
  "mcpServers": {
    "g-gremlin-hubspot": {
      "command": "g-gremlin-hubspot-mcp",
      "env": {
        "G_GREMLIN_HUBSPOT_ACCESS_TOKEN": "YOUR_TOKEN"
      }
    }
  }
}
```

3. Use OpenClaw with the mcporter skill/runtime to list and call tools from `g-gremlin-hubspot`.

## How it compares

| Capability | HubSpot Official MCP | peakmojo/mcp-hubspot | **g-gremlin-hubspot-mcp** |
|---|---|---|---|
| CRM search | 10k cap | cached/vector | **Auto-window past 10k** |
| Write operations | No | Create only | **Upsert + dry-run** |
| Duplicate detection | No | No | **Merge plans** |
| Schema introspection | No | No | **Full schema/props** |
| Engagement export | No | No | **Async fallback** |
| Snapshot diffing | No | No | **Yes** |
| Safety layer | N/A | None | **Dry-run + plan hash** |
| Impact classification | No | No | **Per-tool labels** |

## How we break the 10k ceiling

HubSpot's Search API has a hard cap of 10,000 total results — no cursor beyond that.

g-gremlin breaks this with **recursive date-range windowing**:

1. Initial query hits the 10k ceiling
2. Inspects `createdate` timestamps in returned records
3. Splits the time range into binary halves, re-queries each window
4. Repeats recursively (max depth 8, min window 7 days)
5. Deduplicates across windows by record key

**Required:** `createdate` (default) or any sortable date property. For objects without timestamps, engagements use an async export fallback with no ceiling.

## Available tools

**7 tools work with no account (FREE). 7 require a g-gremlin HubSpot Admin license or active trial.** A 30-day trial is available at first install.

### Tier 1: Read & Discover

| Tool | Impact | License | What it does |
|------|--------|---------|-------------|
| `hubspot.auth.whoami` | `[READ]` | FREE | Check auth, show portal identity |
| `hubspot.auth.doctor` | `[READ]` | Licensed | Health diagnostics (connectivity, scopes, API access) |
| `hubspot.schema.list` | `[READ]` | FREE | List all CRM object types (standard + custom) |
| `hubspot.schema.get` | `[READ]` | FREE | Full schema for an object type (properties, associations) |
| `hubspot.props.list` | `[READ]` | Licensed | Property introspection (names, types, labels) |
| `hubspot.objects.query` | `[READ]` | FREE | CRM search with filters (Search API, capped at 10k) |
| `hubspot.objects.pull` | `[READ]` | FREE | Full extraction past the 10k ceiling (auto-windowing) |
| `hubspot.engagements.pull` | `[READ]` | FREE | Engagement pull with async export fallback |

### Tier 2: Analyze & Plan

| Tool | Impact | License | What it does |
|------|--------|---------|-------------|
| `hubspot.dedupe.plan` | `[ANALYZE]` | FREE | Scan for duplicates, generate merge plan with plan_hash |
| `hubspot.props.drift` | `[ANALYZE]` | Licensed | Detect property drift between spec and live portal |
| `hubspot.snapshot.create` | `[READ]` | Licensed | Capture CRM state (schema, props, counts) |
| `hubspot.snapshot.diff` | `[ANALYZE]` | Licensed | Compare two snapshots, show what changed |

### Tier 3: Mutate

| Tool | Impact | License | What it does |
|------|--------|---------|-------------|
| `hubspot.objects.upsert` | `[WRITE]` | Licensed | Bulk upsert from CSV (dry-run default, two-phase apply) |
| `hubspot.dedupe.apply` | `[MERGE]` | Licensed | Execute a merge plan (requires plan_hash verification) |

> **The free tools are the strongest hooks:** pull past 10k records and generate dedup merge plans — no account needed. The paywall appears when you act on what you found (upsert, apply merges, snapshots).

## Safety model

All mutations use **two-phase confirmation**:

1. **Dry-run** (default): tool runs without making changes, returns a preview + `plan_hash`
2. **Apply**: caller passes `apply=true` AND the `plan_hash` from step 1. If the hash doesn't match (plan changed, wrong file), the tool rejects with a clear error.

Every tool response includes an impact classification: `[READ]`, `[ANALYZE]`, `[WRITE]`, or `[MERGE]`.

### Response envelope

Every tool returns a consistent `GremlinMCPResponse/v1` JSON:

```json
{
  "$schema": "GremlinMCPResponse/v1",
  "ok": true,
  "summary": "Pulled 47,231 contacts across 12 auto-window queries",
  "data": { ... },
  "artifact": { "type": "file", "path": "...", "row_count": 47231 },
  "warnings": [],
  "safety": { "dry_run": false, "impact": "read" },
  "raw": { "agentic_result": { ... }, "exit_code": 0 }
}
```

## Auth

This MCP server **never stores tokens**. It delegates to g-gremlin's credential chain:

1. `G_GREMLIN_HUBSPOT_ACCESS_TOKEN` env var (highest priority)
2. `g-gremlin hubspot connect --access-token <PAT>` (stored locally in `~/.g_gremlin/`)
3. `g-gremlin hubspot oauth connect` (browser-based OAuth)

### Troubleshooting

**"HubSpot not configured" error in Claude Desktop?**

Claude Desktop may run under a different user context than your terminal. Set the token in the MCP config's `env` block (see Quickstart above) or set `G_GREMLIN_HUBSPOT_ACCESS_TOKEN` as a system-level environment variable.

## Known gaps & roadmap

This is a **public beta**. Core read and analyze workflows are stable. Known gaps:

- **Remote MCP (SSE transport)** — currently stdio only; SSE/streamable HTTP planned for teams that don't want local installs
- **Workflow diffing** — g-gremlin has workflow comparison commands, not yet exposed as MCP tools
- **Association management** — create/delete associations between objects
- **List management** — HubSpot list creation and membership management
- **Pipeline management** — deal/ticket pipeline configuration

Found a bug or have a feature request? [Open an issue](https://github.com/mikeheilmann1024/g-gremlin-hubspot-mcp/issues).

## Requires

- Python 3.10+
- g-gremlin >= 0.1.14 (version checked at startup)
- A HubSpot Private App token with CRM scopes

## Development

```bash
git clone https://github.com/mikeheilmann1024/g-gremlin-hubspot-mcp
cd g-gremlin-hubspot-mcp
pip install -e ".[dev]"
pytest
```

## License

MIT
