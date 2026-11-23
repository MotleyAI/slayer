## Why

SLayer caps `mcp<2`, so any environment that needs mcp 2.x for another package cannot install SLayer.
mcp 2.x renames `FastMCP` to `MCPServer`, changes tool results, contexts and app builders, and since 2.1
hides the text of any tool exception that is not a `ToolError` — which would blind agents to every
engine error message.

## What Changes

- **BREAKING**: SLayer requires `mcp>=2.3,<3` (was `>=1.19,<2`); declared floors rise to what mcp 2 forces
  (`pydantic>=2.12`, `uvicorn>=0.31.1`). The `client` extra keeps `httpx`; mcp brings `httpx2`, a separate module.
- The MCP server is built on `mcp.server.mcpserver.MCPServer`; the import guard accepts only mcp 2.x and
  names the required range otherwise.
- The server reports SLayer's version through `MCPServer(version=...)`; the private-attribute write goes.
- Every tool failure keeps its message for the agent: `Error executing tool <name>: <message>`, on any
  supported mcp version.
- `slayer serve --host` reaches the mounted MCP SSE app: a non-loopback bind accepts any `Host` header
  (today it answers 421), a loopback bind keeps DNS-rebinding protection.
- Usage reporting, deprecation reporting and OpenTelemetry keep their behaviour on the new API.
- The deprecated `inspect_model` tool named in the tracker issue is already removed; nothing changes for it.

## Capabilities

### New Capabilities
- `mcp/server-runtime`: the MCP server's SDK binding, its reported identity, how tool failures reach the
  agent, and how the SSE mount honours the bind host.

### Modified Capabilities

## Impact

- Code: `slayer/mcp/server.py`, `slayer/api/server.py` (`create_app` gains `host`), `slayer/cli.py`
  (`_run_serve`), `slayer/usage_reporting/recorder.py` (error-class token).
- Dependencies: `pyproject.toml`, `poetry.lock`.
- Tests: every test driving the MCP server (result shape, snake_case fields, `ToolError` path, in-memory
  client), `tests/test_mcp_dependency_pin.py`.
- Scripts, examples, notebooks and docs that use the mcp SDK or name `mcp<2` / `FastMCP`.
