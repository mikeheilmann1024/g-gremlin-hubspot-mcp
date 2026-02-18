# Changelog

## 0.1.0 (Public Beta)

Initial release. 14 HubSpot MCP tools across three tiers.

### Tools shipped

**Free (no account required):**
- `hubspot.auth.whoami` — check auth, show portal identity
- `hubspot.schema.list` / `hubspot.schema.get` — CRM object type introspection
- `hubspot.objects.query` — CRM search (Search API, 10k cap)
- `hubspot.objects.pull` — full extraction past the 10k ceiling via auto-windowing
- `hubspot.engagements.pull` — engagement pull with async export fallback
- `hubspot.dedupe.plan` — duplicate detection + merge plan generation

**Licensed (HubSpot Admin or trial):**
- `hubspot.auth.doctor` — health diagnostics
- `hubspot.props.list` / `hubspot.props.drift` — property introspection + drift detection
- `hubspot.snapshot.create` / `hubspot.snapshot.diff` — CRM state snapshots + comparison
- `hubspot.objects.upsert` — bulk upsert with two-phase safety
- `hubspot.dedupe.apply` — execute merge plans with plan_hash verification

### Safety

- Two-phase mutation safety (dry-run + plan_hash verification)
- Impact classification on every tool: `[READ]`, `[ANALYZE]`, `[WRITE]`, `[MERGE]`
- `GremlinMCPResponse/v1` envelope on all responses
- g-gremlin version gating at startup (>= 0.1.14)
