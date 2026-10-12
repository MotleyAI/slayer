## 1. Tests first (pr-tests stage; all must fail before implementation unless noted)

- [x] 1.1 `tests/_mcp_idiom_fixtures.py`: `SHARE_OF_GROUP`, `NESTED`, `RANK_FILTER` switch to `customers.regions.name`, `STAGE_COLUMN` to `customers__regions__name` (design D2); add marker constants `STAGE_NOT_NEEDED`, `STAGE_NEEDED`, `READABLE_LABEL`, `ONE_QUERY`, `FAN_OUT` (design D5) with `EXAMPLE_OWNERS` rows where schema-owned. Verify: imports cleanly.
- [x] 1.2 `tests/test_mcp_documented_idioms.py`: rank-filter, share, nested and outer-stage tests group by `customers.regions.name` (keys North / South, values unchanged). New tests with the design's probe values: a parametrised time-filter test (Feb–Mar filter restricts `cumsum`, `lag`, `lead`, `consecutive_periods`, rank / `ntile` over months, `partition_by=[]`; `change`, `time_shift` ±1, `window='3m'` equal their unfiltered values); a parametrised peer-filter test (`customers.name == 'Alice'` → rank, dense_rank, ntile n=3 all 1, grand total 170, share of region 1.0, nested avg 170); `status == 'paid'` → 2025-01 `sum(amount, window='3m')` 195; same-stage `cumsum(sum(amount))` value filter `> 300` keeps 2025-03 325, -04 385, -06 490; two stages (inner rank over all customers, outer `customers__name == 'Alice'`) → 2. Verify: the engine-behaviour tests pass today (they pin behaviour); the regions.name tests pass once examples match (text half lives in 1.3).
- [x] 1.3 `tests/test_mcp_surface_budget.py`: `MARKER_OWNERS` gains the schema-owned markers; the sole-owner guard takes owner sets over every surface text (`ONE_QUERY` → instructions + `query` description, `FAN_OUT` → instructions, stage markers → `name`); retired phrases (design D5) checked over every surface text; `test_instructions_give_the_fan_out_reason` pins the new instructions paragraph verbatim; the query-description test asserts the description starts with "ONE QUESTION = ONE QUERY. Build the whole answer in a single `query` call." instead of the `prefer` stem, and that the look-back clause names changes and trailing windows, not running totals; a new no-`_id` test (design D5). Verify: fails today.

## 2. Query schema field descriptions

- [ ] 2.1 `slayer/core/query.py`: `name`, `dimensions`, `measures`, `filters`, `date_range` per design D2. Verify: 1.2, 1.3, `test_query_schema_within_size_budget` (if over budget: STOP and ask).

## 3. Instructions, query description, help topics

- [ ] 3.1 `slayer/mcp/server.py`: `SERVER_INSTRUCTIONS` second line and the `query` docstring opening + third bullet per design D2. Verify: 1.3, instructions and description budget tests.
- [ ] 3.2 Help topics per design D3: `07_queries.md`, `02_workflow.md`, `04_transforms.md`, `05_time.md`. Verify: 1.3, `tests/test_help_seed.py`, `tests/test_functional_emission.py`.

## 4. Gates and verification

- [ ] 4.1 Full non-integration suite `poetry run pytest -m "not integration" -n auto`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check`. Verify: all green.
- [ ] 4.2 Before opening the PR: in Claude Code with this branch's `slayer mcp`, confirm no SLayer tool description or the instructions end in `[truncated]`; record on DEV-2083. Eval re-runs (both SLayer profiles; single-query passes, share of trials with more than one query, stages where the reference needs one) are driven from slayer-evals.
