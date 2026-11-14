## Why

A converted deprecated input (`strict`, an extension object in `source_model`) must be reported on every response that consumes it, but three 2.0 surfaces miss it: a REST `POST /query` with an explicit `"strict": null` reports nothing (the request's `exclude_none` dump strips the key before anything converts it), an MCP `query` dry run returns only the SQL and drops every warning of the run, and the spec asks MCP model saves for structured entries while they report messages, as the 2.0 guide documents.

## What Changes

- Fresh query input is converted by one converter, shared by `SlayerQuery` and the REST `POST /query` request: the request carries every deprecation of its body, so an explicit `"strict": null` reports one `deprecated_input` warning, and a run-by-name body carrying a converted input is still refused.
- The MCP `query` tool's dry run returns the run's warnings next to the SQL: a `{"sql", "warnings"?}` object under `format="json"`, leading `# warning:` lines before the SQL under `csv`, and the SQL followed by the `Warnings:` block under `markdown`. This also delivers the `library_name_shadow` warning `models/aggregation-names` already requires on MCP dry runs.
- MCP model saves keep reporting a deprecated input as its message; the removal-tripwire requirement is amended to that shape (structured entries stay on REST model saves).

## Capabilities

### New Capabilities

### Modified Capabilities
- `compatibility/removal-tripwire`: MCP model saves report a deprecated input as its message; new scenarios for an explicit REST `"strict": null`, a query carrying two deprecated forms, and an MCP dry run.
- `mcp/query-tool`: `dry_run` returns the generated SQL together with the run's warnings, shaped per `format`.
- `mcp/response-row-cap`: dry-run output is no longer "SQL only"; it still never carries a truncation notice.

## Impact

- `slayer/core/query.py` (`prepare_query_input` becomes the one fresh-input converter, returning every payload; `SlayerQuery`'s validator calls it), `slayer/api/server.py` (`QueryRequest` keeps every payload; the run-by-name gate refuses converted input), `slayer/mcp/server.py` (dry-run reply).
- Docs: `docs/reference/mcp.md` (`dry_run`), `docs/upgrading/2.0.md` Part 2 (checked).
- No API removals; the MCP dry-run reply under `format="json"` changes from `SQL:` text to a JSON object.
