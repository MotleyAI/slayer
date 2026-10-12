## Why

We have no signal on how SLayer is used — active installs, which surfaces, dialects and query features, version adoption, failure rates. Comparable tools (dbt, Cube, Kaelio ktx, CrewAI) collect anonymous usage data; dbt-mcp's CVE-2026-44970 (raw MCP tool arguments sent in usage reports) shows the failure mode to design out from the start.

## What Changes

- New anonymous, aggregated, **opt-out** usage reporting for processes started by the `slayer` CLI (`slayer <cmd>`, `mcp`, `serve`, `flight-serve`, `pg-serve`). Library use never records, notifies or sends.
- Payload carries only SLayer-defined vocabulary: versions, enums, booleans, buckets, counts keyed by built-in names; anything else is `other` / `custom`. No SQL, names, arguments, messages, paths, IPs.
- Per-process in-memory counters, spooled locally at exit; sent to PostHog EU at most once per 24 h per install, only when SLayer runs anyway (no heartbeat), from a background thread.
- Controls: `DO_NOT_TRACK=1`, `SLAYER_USAGE_REPORTING=on|off`, auto-off in `CI`, editable installs and unwritable config dirs, and `slayer usage-reporting status|enable|disable|show`.
- One-time stderr notice; docs page with every field and a privacy notice; random install ID rotated every 13 months.
- Architecture: new precise node `usage_reporting` in the LikeC4 model; new `usage_reporting.arc42.md` (fail-silent, payload vocabulary, privacy principles).

## Capabilities

### New Capabilities
- `usage-reporting/collection`: when usage reporting is active and exactly what a usage report may contain.
- `usage-reporting/controls`: default, opt-out precedence, the `slayer usage-reporting` command, and the first-run notice.
- `usage-reporting/delivery`: local spooling, the 24 h send gate, destination, install-ID rotation, and the guarantee never to affect the host process.

### Modified Capabilities
(none)

## Impact

- New package `slayer/usage_reporting/` (stdlib `urllib`, no new dependency).
- Hooks in `slayer/cli.py`, `slayer/mcp/server.py` (`FastMCP.call_tool`), `slayer/api/server.py` (middleware), `slayer/flight/handlers.py` (`_execute_full`), `slayer/pg_facade/connection.py` (`_run_query`), and the query-executing surfaces (feature extraction).
- Architecture: `architecture/model.c4` (node + edges), `architecture/system.arc42.md` §2, new `architecture/usage_reporting.arc42.md`, regenerated landscape diagram.
- Docs: `docs/reference/usage-reporting.md` (+ `zensical.toml` nav), README, CLI reference, release notes.
- External prerequisite (release-blocking): PostHog EU project with "Discard client IP data", PostHog DPA, project API key.
