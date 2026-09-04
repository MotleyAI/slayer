# Tidy up MCP query tools

## Why

The MCP surface exposes two overlapping query tools: `query` (multi-arg form — separate `source_model`/`measures`/`dimensions`/… arguments assembled into one query dict) and `query_nested` (a `queries` list for multi-stage DAGs). The engine's `execute()` already accepts the whole union (`str | dict | list`), so both special forms are redundant indirection that bloats the tool schema agents must read.

## What Changes

- **BREAKING** — the `query` MCP tool's main argument becomes the query itself: `query: str | dict | list[dict]` (model name for run-by-name, single query json, or multi-stage DAG list), keeping only the execution wrappers `variables`, `show_sql`, `dry_run`, `explain`, `format` as separate args. The per-field args (`source_model`, `measures`, `dimensions`, `filters`, `time_dimensions`, `order`, `limit`, `offset`, `whole_periods_only`, `strict`, `distinct_dimension_values`) are retired; `strict` and `distinct_dimension_values` are expressed inside the query json (they are `SlayerQuery` fields).
- **BREAKING** — the `query_nested` MCP tool is deleted outright (no stub or alias); its list semantics move into `query`.
- **BREAKING** — a bare model-name string now means run-by-name only (exact `engine.execute(str)` semantics): a non-query-backed model name raises the engine's "not query-backed" error instead of silently wrapping into `SlayerQuery(source_model=name)`. The MCP-side run-by-name shortcut block and the "strict not supported with run-by-name" check are deleted.
- Output handling is unified into one path — the run-by-name path now appends the attributes block whenever present, like every other path.
- Docs, skills, notebook tool lists, and comment mentions are updated to the new surface; REST API, CLI, Python client, engine, and core behavior are unchanged.

## Capabilities

### New Capabilities

- `mcp/query-tool`: the MCP `query` tool's input contract (str/dict/list dispatch mirroring `engine.execute`), its execution-wrapper arguments, and its output shaping (format validation, dry-run/explain/show-sql, attributes block, friendly DB errors).

### Modified Capabilities

None — existing corpus capabilities (queries/aggregations/models) describe engine behavior, which is untouched.

## Impact

- `slayer/mcp/server.py` — `query` tool rewritten as a thin `engine.execute` wrapper; `query_nested` deleted.
- Tests — ~15 call sites rewritten across `tests/test_mcp_server.py`, `tests/test_distinct_dimension_values.py`, `tests/test_mcp_engine_teardown.py`, `tests/integration/test_dev1756_identifier_length_pg.py`; `query_nested` tests become `query(list)` tests; new dispatch/schema coverage; comment fix in `tests/test_api_server.py`.
- Docs — `docs/reference/mcp.md`, `docs/interfaces/mcp.md`, `docs/concepts/queries.md`, `.claude/skills/slayer-query.md`, notebook output cells in `docs/examples/08_mcp_introspect/` and `docs/examples/13_osi_import/`, comment mentions in `slayer/api/server.py` and `slayer/client/slayer_client.py`.
- Downstream: Storyline verified unaffected (owns its MCP server; consumes slayer only as a library; inherited help topics don't mention the retired forms).
