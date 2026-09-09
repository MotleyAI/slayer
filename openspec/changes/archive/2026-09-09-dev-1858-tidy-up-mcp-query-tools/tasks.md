# Tasks — tidy up MCP query tools

## 1. Tests first (spec-tests stage)

- [x] 1.1 Add schema regression test: the `query` tool's input schema exposes exactly `query` (required, accepting string/object/array-of-objects), `variables`, `show_sql`, `dry_run`, `explain`, `format`; no `query_nested` tool is registered — RED on current code
- [x] 1.2 Add dispatch tests: dict runs a single query, list runs a two-stage DAG, query-backed string runs by name, non-query-backed string raises the engine's "not query-backed" error (naming the `source_model=` remedy), empty list raises, object honors order/limit — RED on current code
- [x] 1.3 Add in-query control-field tests: `"strict": true` inside the query json errors on a broadcast (lenient returns rows + warning) and `"distinct_dimension_values": false` inside the query json returns raw rows — RED on current code
- [x] 1.4 Add wrapper tests on the new shape: runtime `variables` override, `dry_run` returns SQL only (no rows), `explain` returns SQL + plan, `show_sql` prefixes, invalid `format` names json/csv/markdown, format case-insensitive, attributes block appended (and tails) on the run-by-name path — RED on current code
- [x] 1.5 Rewrite existing call sites to the new call shape (`tests/test_mcp_server.py`, `tests/test_distinct_dimension_values.py`, `tests/test_mcp_engine_teardown.py`); `query_nested` tests become `query(list)` tests; deleted the now-unrepresentable `test_run_by_name_rejects_strict` — RED on current code for the changed reason. (`tests/integration/test_dev1756_identifier_length_pg.py` needs NO change: its `source_model=` sites are `SlayerQuery(...)` on the engine, not the MCP tool — plan over-inclusion, confirmed by Codex.)

## 2. Implementation

- [x] 2.1 Rewrite the `query` tool in `slayer/mcp/server.py` as a thin `engine.execute` wrapper with the six-argument signature and one uniform output path; restructure the docstring per design.md preserving all agent-facing content — verify tasks 1.1–1.5 tests pass
- [x] 2.2 Delete the `query_nested` tool — verify the tool-list test passes and no source reference remains (`grep -rn query_nested slayer/`  returns only the comment sites updated in 3.3)
- [x] 2.3 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and fix any failures

## 3. Docs and comment mentions

- [x] 3.1 Update `docs/reference/mcp.md` and `docs/interfaces/mcp.md`: rewrite the `query` row, delete the `query_nested` row, convert the workflow example to json form — verify `grep query_nested` over both files is empty
- [x] 3.2 Update `docs/concepts/queries.md` (multi-stage surface lines and the run-by-name "MCP equivalent" paragraph) and `.claude/skills/slayer-query.md` — verify `grep query_nested` over both files is empty and the run-by-name paragraph shows the new call form
- [x] 3.3 Update comment/docstring mentions in `slayer/api/server.py`, `slayer/client/slayer_client.py`, `tests/test_api_server.py`; drop `query_nested` from the captured tool lists in `docs/examples/08_mcp_introspect/mcp_introspect_nb.ipynb` and `docs/examples/13_osi_import/osi_import_agent_nb.ipynb` — verify repo-wide `grep query_nested` hits only `DECISIONS.md`, `openspec/`, and the new test's absence-assertions
- [x] 3.4 Run `poetry run ruff check slayer/ tests/` and fix any issues
