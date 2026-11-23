## 1. Baseline (before any dependency change)

- [ ] 1.1 With mcp 1.27 still installed, dump `tools/list` (every tool's name, description, input schema and `_meta`, for both `always_load_query=False` and `True`) as sorted JSON to `openspec/changes/dev-1760-migrate-mcp-server-to-the-mcp-2x-mcpserver-api/tools-list-mcp1.json`; verify the file lists every registered tool. Task 8.1 diffs against it — it must exist before task 3.1 runs.

## 2. Tests (written failing first)

- [ ] 2.1 `tests/test_mcp_dependency_pin.py`: rewrite so the constraint guard asserts `>=2.3,<3` (admits 2.3.0 and later 2.x, excludes 2.2.x and 3.0.0), and the import-guard tests cover mcp missing, 1.x installed, 3.x installed and an unparseable version against the `mcp.server.mcpserver` module path; verify each fails on the current code.
- [ ] 2.2 Server identity test (spec "MCP server identity"): an in-memory `mcp.client.Client` session reports name `SLayer`, version = installed `motley-slayer`, SLayer's instructions; replaces the `_mcp_server.version` assertions.
- [ ] 2.3 Tool-failure tests (spec "Tool failures reach the agent"): over a real client session, a `query` failing with a SLayer engine error, a tool raising a plain `ValueError` (monkeypatched), and a schema-invalid argument each give an error result with text `Error executing tool <name>: <message>`; a usage-reporting test shows a database failure is counted under the database error token, not `tool_error`.
- [ ] 2.4 SSE host tests (spec "SSE mount follows the bind host"): through `create_app(host=...)` + `TestClient`, `host="0.0.0.0"` with `Host: slayer.example.com:5143` is not refused with 421, `host="127.0.0.1"` with `Host: evil.example:5143` is refused with 421.
- [ ] 2.5 CLI plumbing test (codex): via the `create_app`-kwargs seam in `tests/test_startup_ingest.py`, `slayer serve --host 0.0.0.0` passes `host="0.0.0.0"` to `create_app`.
- [ ] 2.6 Mechanically adapt every existing test driving the MCP server to the 2.x API — no logic change: `blocks, _ = await server.call_tool(...)` → the returned `CallToolResult`'s `.content` / `.structured_content`; `isError`/`inputSchema`/`clientInfo` → snake_case; `ToolError` from `mcp.server.mcpserver.exceptions`; `create_connected_server_and_client_session` → `mcp.client.Client`; `ClientSession`/`stdio_client`/`sse_client` sites (`tests/test_server_shutdown.py`, `tests/_usage_reporting_helpers.py`, `tests/integration/test_mcp_inspect.py`, …); `tests/fixtures/vocabulary_corpus` entries if their identifiers change. Find sites with LSP / `la-affected-tests`, not a fixed list; verify each adapted file imports cleanly under mcp 2.x.

## 3. Dependencies

- [ ] 3.1 `pyproject.toml`: `mcp = ">=2.3,<3"` (delete the `<2`-cap comment), `pydantic = ">=2.12"`, `uvicorn = ">=0.31.1"`; `httpx` / the `client` extra unchanged. `poetry lock` then `poetry install -E all`; verify `mcp` 2.3+ and `httpx` both installed.

## 4. MCP server (`slayer/mcp/server.py`)

- [ ] 4.1 Replace `_import_fastmcp()` / `_mcp_major()` with an `MCPServer` import guard accepting only `>=2.3,<3` (absent → install remedy; out of range, 3.x included → installed version + required range); update `_MCP_REMEDY`; the `TYPE_CHECKING` import moves to `mcp.server.mcpserver`. Verify 2.1.
- [ ] 4.2 Build the server as `_counted(MCPServer)("SLayer", instructions=SERVER_INSTRUCTIONS, version=__version__)`; delete `_set_server_version`. Verify 2.2.
- [ ] 4.3 `CountedFastMCP` → `CountedMCPServer`: override `call_tool(name, arguments, context=None)` returning `CallToolResult`; session from `context.session` (no `get_context()`), client from `client_params.client_info`; a context-less call counts with no session. Keep the `otel.request` span and `_tool_operation`. Verify the usage-reporting and otel suites.
- [ ] 4.4 `tool()` registration helper wraps each handler (`functools.wraps`) and re-raises anything but `ToolError` / `MCPError` as `ToolError(str(exc)) from exc`. Verify 2.3 and that `tools/list` schemas are unchanged (8.1).
- [ ] 4.5 `_counted_error` unwraps SDK `ToolError` → wrapper `ToolError` → `ToolFailure` to the original exception. Verify the usage-reporting token test in 2.3 and the existing usage-reporting suites.
- [ ] 4.6 `_with_deprecations` works on `CallToolResult` (first text block in `content`; `structured_content["result"]` when present). Verify the deprecated-input suites (`tests/test_deprecated_input_harvest.py`, `tests/test_dev2286_deprecated_input_surfaces.py`, …).
- [ ] 4.7 `slayer/usage_reporting/recorder.py`: error-class key `mcp.server.fastmcp.exceptions.ToolError` → `mcp.server.mcpserver.exceptions.ToolError`. Verify the usage-reporting payload tests.
- [ ] 4.8 Sweep remaining 1.x identifiers and comments in `slayer/` (`FastMCP` in comments, NOSONAR notes, `ALLOW(import-not-top)` reasons, docstrings); verify no `fastmcp` reference remains under `slayer/` outside upgrade-path text.

## 5. Callers

- [ ] 5.1 `slayer/api/server.py`: `create_app(..., host: str = "127.0.0.1")` passes `host` to `mcp.sse_app(host=host)`. Verify 2.4.
- [ ] 5.2 `slayer/cli.py::_run_serve` passes `args.host` to `create_app`; `_run_mcp` keeps `mcp.run()` (stdio). Verify 2.5 and the CLI MCP tests.

## 6. Scripts, examples, notebooks

- [ ] 6.1 Update `scripts/release_gate.py` and `examples/comparisons/slayer/run_slayer.py` to the 2.x client API; verify `tests/test_release_gate.py` and a dry import of the example.
- [ ] 6.2 Update `docs/examples/08_mcp_introspect/mcp_introspect_nb.ipynb` and `docs/examples/13_osi_import/osi_import_agent_nb.ipynb` to the 2.x API and re-execute both with saved outputs (`jupyter nbconvert --to notebook --execute --inplace`); verify the notebook test suite for them passes.

## 7. Docs

- [ ] 7.1 `docs/upgrading/2.0.md`: SLayer 2.0 requires `mcp>=2.3,<3`; `slayer serve --host` now applies to `/mcp` (non-loopback binds accept any `Host`).
- [ ] 7.2 `docs/interfaces/mcp.md` and `docs/reference/mcp.md`: the SSE mount's `Host` behaviour per bind address; replace any `FastMCP` / `mcp<2` mention across `docs/`, `README.md` and `.claude/skills/` (grep). Verify the docs guard tests.

## 8. Verification

- [ ] 8.1 Dump `tools/list` under mcp 2.x exactly as in 1.1 and diff against `tools-list-mcp1.json`; verify no difference (eliminate any, or escalate one that cannot be). Delete `tools-list-mcp1.json` once the diff is clean.
- [ ] 8.2 Run the local test scope (`la-affected-tests --base main` plus goldens/fixtures this change reaches); verify all pass.
- [ ] 8.3 `poetry run ruff check slayer/ tests/`, `la-typecheck` (no new errors vs baseline), `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture`; verify all green.
