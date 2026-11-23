## Context

`slayer/mcp/server.py` builds a `FastMCP` subclass (`_counted`) whose `call_tool` override counts usage,
reports pre-2.0 argument spellings and is wrapped in an OpenTelemetry span. `slayer/api/server.py` mounts
`mcp.sse_app()` at `/mcp`; `slayer mcp` calls `mcp.run()` (stdio). Facts from the mcp 2.3.0 source that
shape the approach:

- `MCPServer.call_tool(name, arguments, context=None)` returns `CallToolResult` (no `(blocks, structured)`
  tuple) and raises `ToolError` on failure; `_handle_call_tool` turns that into an error result.
- A tool exception other than `ToolError`/`ResourceError`/`MCPError` becomes `UnexpectedToolError` whose
  text is only `Error executing tool <name>` (2.1+; 2.0 kept the message, as 1.x did).
- `get_context()` is gone; the `context` argument carries `.session`; `client_params.client_info` is
  snake_case. `create_connected_server_and_client_session` is gone; `mcp.client.Client` replaces it.
- `sse_app(host=...)` enables DNS-rebinding protection only for a loopback `host` (default `127.0.0.1`);
  1.x did the same from the constructor's default `host`, so `slayer serve --host 0.0.0.0` answers 421 to
  any non-localhost `Host` today. The SSE transport prefixes the message endpoint with the mount's
  `root_path`, so mounting at `/mcp` keeps working.
- mcp 2.x emits its own spans through the global OpenTelemetry tracer provider.
- `httpx2` installs a module named `httpx2`; it cannot clash with SLayer's `httpx`.

Architecture: every touched module is in the `python.surfaces` bucket, plus one string in
`slayer.usage_reporting.recorder`; no new internal import edge, no harness edit. The new spec group sits
under `openspec/specs/mcp/`, already mapped by the bucket's `specs ['mcp']`. Applicable principles
(`architecture/system.arc42.md` §3): 5 async-first (every tool stays `async def`, so 2.x's worker-thread
dispatch of sync handlers never applies), 6 Pydantic v2 / no dataclasses, 9 one writer of result values
(untouched), 1–2 claims and model-truth (checked by `la-arch-check`).

## Goals / Non-Goals

**Goals:**
- The advertised tool surface (names, descriptions, input schemas, `_meta`) is identical before and after.
- Agent-visible text of every tool result and failure is identical to today's.
- Usage-reporting tokens, deprecation reporting and OTel spans behave as today.

**Non-Goals:**
- Removing `inspect_model` (already shipped by DEV-2231; `tests/test_removed_2_0_forms.py` pins it).
  Test names and test docstrings still saying `inspect_model` stay.
- New transports (streamable HTTP), MCP auth, SDK middleware.
- Moving `slayer/client` from `httpx` to `httpx2`.

## Decisions

1. **`inspect_model` removal is out of scope** — DEV-2231 already removed the tool and its docs; the
   leftover mentions are test names/docstrings, exempt by DEV-2364's convention. (default)
2. **`slayer serve --host` reaches `sse_app(host=...)`** — options: pass it through / keep the
   localhost-only allow-list and leave the gap. Taken: pass through, via a new `create_app(host=...)`
   parameter defaulting to `"127.0.0.1"` (programmatic callers unchanged). This is the SDK's own
   `run_sse_async` rule; the Docker image binds `0.0.0.0` on purpose. (user)
3. **Tool-failure text kept by one wrapper at tool registration** — options: wrap every handler in the
   `tool()` registration helper, re-raising anything but `ToolError`/`MCPError` as
   `ToolError(str(exc)) from exc` / make `SlayerError` inherit `ToolError`. Taken: the wrapper.
   Inheritance misses every non-`SlayerError` failure (pydantic `ValidationError`, `SqlClientError`,
   `RegexPatternError`, `JsonPathError`, plain `ValueError`, `ToolFailure`, sqlglot/SQLAlchemy errors)
   and would couple `slayer.core` to the MCP SDK. The wrapper uses `functools.wraps` so the SDK still
   derives the schema and context parameter from the original signature. The SDK then logs at INFO, as
   1.x did. (user)
4. **Counting stays a public `call_tool` override** (`CountedMCPServer`), not SDK middleware — the
   middleware API is documented as provisional. The session comes from the `context` argument; a call
   without a request context counts with no session. `_counted_error` unwraps the cause chain (SDK
   `ToolError` → wrapper `ToolError` → `ToolFailure`) to the original exception, so tokens are unchanged.
   (default)
5. **mcp floor `>=2.3,<3`** — options: `>=2.0,<3` plus a CI job pinned to 2.0.0 / `>=2.3,<3`, the version
   CI tests / `>=2.0,<3` untested. Taken: `>=2.3,<3` — the declared range is exactly what CI verifies,
   and it still removes the resolver conflict with packages needing mcp 2.x. The `<3` cap follows
   DEV-1757. (user)
6. **Declared floors rise to what mcp 2 forces** — `pydantic>=2.12`, `uvicorn>=0.31.1`; the `client`
   extra keeps `httpx`. (default)
7. **SDK OpenTelemetry spans need no change** — they go to the global tracer provider; `slayer.otel` uses
   its own provider, so nothing is exported twice. (default)
8. **Tool surface verified identical** by a one-off diff of `tools/list` captured on mcp 1.27 before the
   switch against mcp 2.x after; any difference is eliminated or escalated. (default)
9. **Server version through `MCPServer(version=__version__)`**; `_set_server_version` is deleted (2.x
   would otherwise report an empty string). (default)
10. **The import guard rejects every installed version outside `>=2.3,<3`, 3.x included**, keeping the
    existing wrong-major and unparseable-version coverage. (codex)
11. **A CLI-level test proves `_run_serve` passes `args.host` to `create_app`**, through the existing
    `create_app`-kwargs seam in `tests/test_startup_ingest.py`; a `create_app` test alone cannot catch
    the caller omitting it. (codex)
12. **rejected (codex): migrate the `client` extra to `httpx2`.** The tracker names `httpx2` as a possible
    blast radius, not a requirement; `httpx2` is a separate module that coexists with `httpx`, and
    `slayer/client` uses `httpx` independently of mcp.
13. **Folded into 5 (codex): the floor was untested** — resolved by raising the floor to the tested
    version rather than adding a floor CI job. (user)

## Risks / Trade-offs

- [2.x generates different input schemas or result conversion for the same signatures] → decision 8's
  diff; the existing surface-budget and schema tests pin sizes and shapes.
- [The wrapper changes the cause chain usage reporting walks] → `_counted_error` unwraps it; spec
  scenario "Failure counted under its own token" plus the existing usage-reporting suites cover it.
- [Exception text of an unexpected crash (a bug) reaches the agent, unlike the 2.1+ SDK default] →
  accepted: identical to today's behaviour; the agent is SLayer's user.
- [A non-loopback bind loses DNS-rebinding protection on `/mcp`] → the SDK's own rule for non-loopback
  binds; operators bind `0.0.0.0` deliberately.
- [Environments pinned to mcp 2.0–2.2 must upgrade] → accepted (decision 5); stated in
  `docs/upgrading/2.0.md`.

## Migration Plan

Ships with SLayer 2.0. `docs/upgrading/2.0.md` states the `mcp>=2.3,<3` requirement and the `serve --host`
behaviour on `/mcp`. Rollback is reverting the change; no stored data is touched.
