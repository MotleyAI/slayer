# Proposal: MCP query response row cap with truncation notice

## Why

The MCP `query` and `query_nested` tools have no response-side row cap: a query without a `limit` returns the full result set, which can flood the calling agent's context window. Storyline's MCP query tool already caps responses; SLayer should behave the same way.

## What Changes

- The MCP `query` tool caps returned rows at 20 when the caller passes no `limit`. An explicit `limit` is trusted verbatim — no ceiling, no truncation.
- `query_nested` applies the same rule keyed on the ROOT stage's `limit` (last entry of `queries`).
- When no explicit limit exists, the structured path pushes `LIMIT cap+1` (21) into the generated query so truncation is detectable without fetching the full result; a universal post-execution slice guards paths push-down cannot reach (run-by-name stored queries, DAG stages, explain plans).
- A truncated response carries a `ResponseTruncationWarning` (new kind `"truncated"` in the `AnySlayerWarning` union) stating the returned row count and how to get more rows; it renders through the existing warnings machinery in all three output formats (markdown `Warnings:` block, csv leading `#` comment, json `{"data", "warnings"}`).

## Capabilities

### New Capabilities

- `mcp/response-row-cap`: response-side row capping and truncation notices for the MCP query tools.

### Modified Capabilities

(none)

## Impact

- `slayer/core/warnings.py` — new `ResponseTruncationWarning` + union member (additive; the engine never emits it, REST unaffected).
- `slayer/mcp/server.py` — default-cap constant, cap/push-down and slice+notice helpers, wiring in `query` and `query_nested`, docstring updates.
- `docs/reference/mcp.md`, `.claude/skills/slayer-query.md` — document the cap, the notice, the root-stage rule, and the json shape change on truncation.
- REST API, Flight, PG facade, stored-query semantics: unchanged.
