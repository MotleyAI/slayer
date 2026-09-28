## 1. Tests (pr-tests)

- [x] 1.1 Add `tests/_dev1994_fixtures.py` mirroring the issue repro — `regions` (East: no customers), `customers` → `regions` many-to-one with a DATE column for time-dimension cells (Frank: no orders), `orders` → `customers` many-to-one with DATE `order_date`; SQLite + DuckDB seeds; hand-computed expectations in the module docstring. Verify: fixture imports and seeds on both backends.
- [ ] 1.2 Add `tests/test_dev1994_empty_value.py` with one executed test per spec scenario (single hop incl. `count(orders.*)`, `count_distinct`, `count_distinct_approx` on DuckDB native and SQLite fallback; non-count controls; inferred population; multi-hop; associate; windowed; every position incl. computed dimension `count(orders.id, partition_by=[id]) == 0`; coarser `partition_by`; re-aggregation avg; `cumsum`/`change`/`time_shift` over count; custom-formula override NULL vs formula-less declaration 0, including an override declared on a model other than the producer root), each on SQLite and DuckDB. Verify: every test fails on the current code for the stated reason (NULL instead of 0), controls pass.
- [x] 1.3 Add an IR-level test that constructing `RegroupSubstitution` without `empty_value` raises a validation error. Verify: fails on current code.

## 2. Planner (typed decision)

- [ ] 2.1 Add the empty-value function next to `INTEGER_AGGREGATIONS` in `slayer/core/enums.py`. Verify: unit-covered via 1.2.
- [ ] 2.2 Lift the aggregation owner resolution (`_resolve_agg_owner`, `slayer/engine/binding.py`; mirrored in `slayer/sql/generator.py`) into one shared helper used by binding, rendering and the planner; add the "resolves a non-None `formula`" check. Verify: existing owner tests stay green.
- [ ] 2.3 Add required `empty_value: Optional[int]` to `RegroupSubstitution` (`slayer/ir/planned.py`) and stamp it at every construction site in `slayer/engine/compile/stages.py` (cross-model plain/windowed/associate/ranked, local regroup, re-aggregation producer + carrier, ORDER-BY wrap). Verify: 1.3 passes; basedpyright clean.
- [ ] 2.4 Fix the regroup attach grain mismatch when the counted column has the same name as a partition key (`count(orders.id, partition_by=[id])` in a computed dimension raises "Regroup attach join keys do not match the producer's grouping grain"). Verify: `TestPositions::test_computed_dimension` and `test_every_position_in_one_query` pass.

## 3. Renderer (one attach door)

- [ ] 3.1 Replace the combined-phase `(cte, col)` tuple in `placeholder_to_cm` (`slayer/sql/generator.py`) with a typed attached-value record (`cte_name`, `column_name`, `empty_value`, value-expression method building `COALESCE` as sqlglot AST); route projection value, outer composites (`_outer_wrapper_alias_facilities` → `render/value_expr.py`), outer WHERE, ORDER BY `cross_model_cte` and transform-chain inputs through the method; metadata consumers keep the names. Verify: combined-phase scenarios in 1.2 pass.
- [ ] 3.2 Build the row-phase `attached_env` entries (three sites) and nested producer attaches through the same record. Verify: row-phase scenarios (re-aggregation, windowed, computed dimension) pass.

## 4. Harness, docs, gates

- [ ] 4.1 Apply the three approved arc42 edits exactly as worded in design.md (semantics Axioms 4 and 10, sql P10). Verify: `uvx --no-build --from living-architecture==0.2.0 la-arch-check` green.
- [ ] 4.2 Generalise the empty-set sentence in `docs/concepts/formulas.md` (~line 98) and the MCP help text (`slayer/mcp/server.py` ~488) — one sentence each. Verify: docs render; grep shows no stale "empty trailing interval"-only phrasing where the general rule applies.
- [ ] 4.2b Add one sentence to `docs/concepts/queries.md` → "Filters and Auto-Joins": `not orders.status = 'bad'` keeps a customer with at least one non-bad order, and a customer with no orders fails it; include those customers with `or orders.id is null`. Verify: `TestNegatedJoinedFilter`.
- [ ] 4.3 Run the full unit suite (`poetry run pytest -m "not integration"`), the CI integration invocation, and `poetry run ruff check slayer/ tests/`; re-bless only tests that assert this exact NULL bug (with consent). Verify: all green.
