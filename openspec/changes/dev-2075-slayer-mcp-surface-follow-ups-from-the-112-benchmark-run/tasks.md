## 1. Tests first (pr-tests stage; all must fail before implementation)

- [x] 1.1 `tests/_mcp_idiom_fixtures.py`: example constants for the new idioms (spine time dimension `{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": "2025"}`, `sum(returns.amount)`, `sum(orders.amount) / count(*)`, plus the prose markers of design D3) with `EXAMPLE_OWNERS` / marker rows; a second seeded DuckDB dataset (design D5): regions, customers incl. one with no region (seed flag to omit it), orders incl. one with NULL `customer_id` and one with a non-existent `customer_id`, a `returns` model joined to customers. Existing dataset and constants unchanged. Verify: imports cleanly; existing idiom tests still pass.
- [x] 1.2 `tests/test_mcp_surface_budget.py`: new markers in `MARKER_OWNERS` (design D3); a sole-owner guard over the `query` description, every schema description, `SERVER_INSTRUCTIONS` and every help topic body (from `slayer/memories/help_content`) for the D3 markers incl. `cannot fan out` → instructions; a test that the instructions carry the fan-out sentence. Verify: fails today (markers missing; `time_spine.timestamp` and "Project exactly" still in help topics).
- [x] 1.3 `tests/test_mcp_documented_idioms.py`: execute each new example text verbatim — spine: 12 rows for 2025, May `count(orders.id)` 0 and `sum(orders.amount)` NULL; several children: per-customer `sum(orders.amount)` / `sum(returns.amount)` equal hand sums over own rows, ungrouped totals too; parentless rows: rooted at customers by `regions.name`, NULL-region row = region-less customer's orders + both parentless orders, ungrouped total includes them, and on the dataset without the region-less customer no NULL-region row exists; ratio: equals orders' total (parentless included) / number of customers. Verify: the engine half passes today (pins behaviour); the description-text half lives in 1.2.
- [x] 1.4 New `tests/test_mcp_query_arg_errors.py` (spec `mcp/query-tool` › Invalid query argument reports only its own form's errors): via `create_mcp_server(...).call_tool("query", ...)` — dict with a measure named `rank` → exactly one validation error naming `rank` and "reserved transform name", no "valid string" / "valid list"; the same inside a two-stage list → one error located at stage 1, no "valid string" / "valid dictionary"; a string still reaches run-by-name; an int → exactly one error. Verify: fails today (three errors).

## 2. Query schema field descriptions

- [x] 2.1 `slayer/core/query.py`: `time_dimensions`, `measures` (ratio sentence; several-child + parentless-row sentences on the joined-aggregate bullet), `order`, `source_model` per design D2 wording. Verify: 1.2, 1.3, `tests/test_mcp_surface_budget.py::test_query_schema_within_size_budget` (if over budget: STOP and ask).

## 3. Help topics and instructions

- [x] 3.1 Remove `## Gaps` from `05_time.md`; remove judgment call 5 and "gaps" from the `help.time` deep-dive line in `00_intro.md`; remove "Project exactly the columns the question names." from `02_workflow.md`; remove ", gaps" from the "time" preview in `slayer/memories/help_seed.py` `_DESCRIPTIONS`. Verify: 1.2, `tests/test_help_seed.py`, `tests/test_functional_emission.py`.
- [x] 3.2 `SERVER_INSTRUCTIONS` in `slayer/mcp/server.py`: add the fan-out sentence (design D2). Verify: 1.2, instructions budget test.

## 4. Discriminated query argument

- [x] 4.1 `slayer/mcp/server.py`: `query` parameter becomes the callable-`Discriminator` union of design D4 (tags `name` / `object` / `stages`), description unchanged. Verify: 1.4, `tests/test_dev1858_mcp_query_tidy.py`, `tests/test_mcp_row_cap.py`, `tests/test_mcp_server.py`.

## 5. Docs, gates, verification

- [x] 5.1 `docs/reference/mcp.md`: one sentence — an invalid `query` argument reports only the errors of the form its JSON type selects. Verify: grep shows it; no new page.
- [x] 5.2 Full non-integration suite `poetry run pytest -m "not integration" -n auto`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`. Verify: all green.
- [x] 5.3 Before opening the PR: in Claude Code with this branch's `slayer mcp`, confirm no SLayer tool description or the instructions end in `[truncated]`; record on DEV-2075. Eval re-runs (N = 3 on the nine listed tasks, then the full set at N = 1) are driven from slayer-evals.
