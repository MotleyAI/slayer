## Why

The usage report cannot tell how many MCP sessions activate (run at least one query that succeeds) or when an install first activated, which EXP-0004 needs as its activation metric. It also counts an MCP query that failed in the database as a success, because the MCP tools return database failures as ordinary text.

## What Changes

- Report schema version 2 (`schema: 2`); the disclosure notice is shown again once, with text naming the new fields.
- New `mcp_sessions` histogram: MCP sessions bucketed by their successful and failed `query` calls (`0` / `1` / `2-5` / `6-20` / `21+`), each session reported once, at process end. Dry runs are not counted.
- New `first_query_ok`: the date of the install's first successful, non-dry-run query on any query surface (MCP, CLI, REST `/query`, Flight SQL, PostgreSQL wire), stored in the usage-reporting config next to the install ID and sent with every report; cleared on install-ID rotation and by `slayer usage-reporting disable`.
- **BREAKING (MCP clients):** the `query`, `create_model`, `validate_models` and `ingest_datasource_models` tools report a database failure as an MCP tool error (`isError: true`), still carrying the friendly "Database error: …" text; usage reporting counts it under its database error token.
- Spool files carry the schema version in their name, so an older SLayer sharing the config directory never claims (and deletes) a batch it cannot read.
- The notice is shown again only for a newer report schema, and the recorded notice schema is never lowered.
- The usage-reporting config keeps fields written by a newer SLayer version when it rewrites the file.
- `docs/reference/usage-reporting.md` documents the new fields.

## Capabilities

### New Capabilities

### Modified Capabilities
- `usage-reporting/collection`: per-session MCP query outcomes, the first-successful-query date, and schema version 2 in the report identity.
- `usage-reporting/delivery`: rotation clears the first-query date; schema-versioned spool file names; config fields of newer versions survive a rewrite.
- `usage-reporting/controls`: `disable` also clears the first-query date; the notice reappears only for a newer schema and the recorded schema is never lowered.
- `mcp/tool-surface`: database failures of the four tools are MCP tool errors.

## Impact

- `slayer/usage_reporting/` — `payload.py` (schema 2, `mcp_sessions`, `first_query_ok`), `recorder.py` (session counters, first-query date, notice rule, dry-run exclusion), `settings.py` (config field, `extra="allow"`, rotation clears the date), `spool.py` (versioned file names), `sender.py` / `control.py` (report field, disable).
- `slayer/mcp/server.py` — session keys to the recorder, database failures raised as tool errors, the counter's cause-chain unwrapping.
- `slayer/cli.py`, `slayer/api/server.py` — dry-run marking of counted query actions.
- `docs/reference/usage-reporting.md`; usage-reporting and MCP tests.
- PostHog events change shape (`schema: 2`); EXP-0004's readout SQL keys on the new fields.
