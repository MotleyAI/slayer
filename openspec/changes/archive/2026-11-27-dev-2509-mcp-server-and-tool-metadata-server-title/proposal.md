## Why

MCP clients and directories score servers by their handshake metadata and tool documentation. SLayer's MCP server
advertises only a name, version and instructions, and none of its 28 tools has a title or behaviour annotations, so
clients can neither display it well nor tell which tools only read, which overwrite, and which reach the database.

## What Changes

- The `initialize` response adds a server `title`, `description`, `websiteUrl` (`https://docs.motley.ai/slayer/`)
  and `icons`: the SLayer octopus mark as two embedded PNG data URIs, an ink variant for light themes and the white
  docs logo for dark themes.
- Every tool advertises a `title` and all four `ToolAnnotations` hints (`readOnlyHint`, `destructiveHint`,
  `idempotentHint`, `openWorldHint`), set from what the tool's implementation does under stated criteria.
- Registering a tool requires a title and the four hints, so a new tool cannot ship without them.
- Tool descriptions are corrected where they misstate behaviour: `ingest_datasource` (it can re-type an existing
  field), `delete_datasource` (its cascade), `delete_dataset` (its refusal), `edit_datasource` (where connection
  settings change) and `create_dataset`'s example (the current field key).
- Every tool keeps the SDK's wrapped `{"result": string}` output schema; no tool gains another.
- `docs/reference/mcp.md` documents the server metadata and the per-tool annotations.

## Capabilities

### New Capabilities

### Modified Capabilities
- `mcp/server-runtime`: the server identity requirement adds title, description, website URL and themed icons.
- `mcp/tool-surface`: every tool carries a title and behaviour annotations under stated criteria; read-only tools
  leave stored definitions unchanged; closed-world tools open no database connection; output schemas stay the
  wrapped result string.

## Impact

- `slayer/mcp/server.py` (`create_mcp_server`, the `tool()` registration wrapper, `_import_mcp_sdk`, five tool
  docstrings); new package data `slayer/mcp/icons/slayer-light.png` and `slayer-dark.png`.
- Tests: MCP server identity and tool-surface tests.
- Docs: `docs/reference/mcp.md`.
- No dependency change; no runtime behaviour change.
