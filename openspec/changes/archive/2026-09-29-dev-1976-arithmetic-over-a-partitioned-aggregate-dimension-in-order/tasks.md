## 1. Tests (pr-tests stage — all fail before implementation)

- [x] 1.1 Create `tests/test_dev1976_dimension_values.py` (unit suite; `exec_engine` fixture `params=["sqlite", "duckdb"]` via `tests/_dev1847_fixtures.make_exec_engine`, as in `tests/test_dev1964_dual_phase_consumption.py`); verify it collects and its new cases fail on HEAD for the right reason (internal error / untyped `ValueError`, not a fixture bug)
- [x] 1.2 computed-dimensions scenarios: `P + 1`, `P * 2 + amount:sum`, `R + 1`, `R * 2 + amount:sum` as measures; order `P + 1` / `R + 1` desc; `rk = rank(P)` ordered by `rank(P) + 1` desc (assert descending `rk` sequence); `x = amount:sum(partition_by=[city, region])` with measures `C`, `C + 1` equal to `x`, `x + 1` per row — values per spec oracles
- [x] 1.3 positions / dimension-value scenarios: `quantity * count(*)` by `quantity` as measure (4, 12, 9, 4, 5), order desc (2, 3 first), filter `> 5` (keeps 2, 3); `iif(region == 'North', sum(amount), 0)` by region; `q2 = quantity * 2` with `quantity * 2 + count(*)`; joined on `corders` (`iif(customers.regions.name == 'North', sum(amount), 0)` → North 70 / South 0; `customers.region_id * sum(amount)` by `customers.region_id` → 70 / 200) as measure AND order AND measure-typed filter; stage-backed `iif(region == 'North', sum(tot), 0)`; aggregate-free `quantity + 1` by `quantity`
- [x] 1.4 D1 boundary: `rd = P` with measure `sum(P) + P` → North 180, South 280, East 360, Gap 40, Void NULL; `q2 = quantity * 2` with measure `sum(quantity * 2) + quantity * 2` equals the row-level sum plus `q2` per cell (oracle computed from `_SALES_ROWS_WIDE` in-test)
- [x] 1.5 Twin-query parity (Law 4) with values asserted: for `quantity * count(*)`, `iif(region == 'North', sum(amount), 0)`, `P + amount:sum` (dim `rd = P`) and `R + amount:sum` (dim `rd = R`) — the measure-typed filter `E > t` keeps exactly the rows where declared measure `E` exceeds `t`, and order by `E` equals sorting the declared measure's values (measure-typed HAVING path covered for plain, computed, partitioned and re-aggregation dimensions)
- [x] 1.6 Typed errors (plan-level, `plan_query`, no DB): `amount + sum(amount)`, `round(amount, 2)` (message contains "needs an aggregation inside an expression"), raw `ordered_at` + `count(*)` under a monthly time dimension (small seeded model, as in `test_dev1964_…::test_same_leaf_time_keys_on_different_paths`), `customers.region_id * sum(amount)` by `customers.regions.name` → `PositionTypingError`, `location` = `measure 'm'`, blocker named, no `REGROUP_LEAF_PREFIX` in `str(exc)`
- [x] 1.7 Error precedence: a measure whose existing rule already fails (e.g. `amount:sum(partition_by=city)` by `[region]`, and a banded-dimension `C` measure) still raises `PartitionKeyError` naming `city`, not `PositionTypingError`
- [x] 1.8 Plan structure: for measures `P + 1` / `R + 1` over `rd`, the attaches covering the producer are row-phase only (`regroup_attach_plans`), one producer CTE (`gen(..., dialect="duckdb")`), no `REGROUP_LEAF_PREFIX` in SQL, `assert_scope_closed` passes
- [x] 1.9 Codex-review the test suite against specs/ and design.md; resolve findings with the user

## 2. Checker (D2, D3)

- [x] 2.1 In `slayer/engine/elaborate_env.py`, type every declared non-dimension measure with `_measure_blockers` (forced measure verdict) at the position-typing checkpoint after the existing measure checks; raise `PositionTypingError` at `measure '<name>'` with the D2 wording (aggregate-free → keeps the "needs an aggregation inside an expression" remedy); verify 1.6, 1.7 pass

## 3. Discovery (D4)

- [x] 3.1 `PositionClasses.combined_admits(position="measure")`: also skip `dim_key` nodes; verify 1.2 `C` / `C + 1`, 1.8 row-only attaches, and `tests/test_dev1964_dual_phase_consumption.py`, `tests/test_dev1850_keyless_grain.py`, `tests/test_dev1824_remaining_guards.py` pass unchanged
- [x] 3.2 Flip `test_dev1953_partition_alias.py` `TestAttachCarryingKey` measure/parameter pins to executed values (fixes DEV-1960)

## 4. Generator (D5)

- [x] 4.1 Add the dimension-value facility to the render context (`slayer/sql/render/value_expr.py`): composite operands equal to a mapped key render as its GROUP BY expression; aggregate / transform internals never consult it; verify 1.4
- [x] 4.2 In `_build_base_select_for_planned`, render projected non-dimension composites and ROW-phase non-dimension hidden slots through the one grouped-SELECT context (dimension map + aggregate builder, no row scope); delete the ROW-branch "needs an aggregation" `ValueError`; verify 1.2, 1.3
- [x] 4.3 In `_build_where_having_from_planned`, render measure-typed (AGGREGATE-phase) masks through the same context; delete the HAVING "not in GROUP BY" `ValueError` guard; verify 1.3 filters and 1.5
- [x] 4.4 Run the golden-SQL suites; re-bless only diffs that are exactly the dropped combined attach, each with the user's OK, recorded in design.md › Approved golden divergences; any other diff is a STOP

## 5. Docs, architecture, gates

- [x] 5.1 `docs/concepts/formulas.md`: one sentence — a measure may combine query-dimension values with aggregates; a row-level column that is not a query dimension is a typing error; verify the page renders in `zensical.toml` nav (already linked)
- [x] 5.2 `architecture/semantics.arc42.md` Axiom 13: add `[enforced: test:tests/test_dev1976_dimension_values.py]` after the DEV-1865 tags (approved in pr-plan); verify `uvx --no-build --from living-architecture==0.2.0 la-arch-check` passes
- [x] 5.3 Full unit suite `poetry run pytest -m "not integration" -n auto`, the integration suite with the CI invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `openspec validate dev-1976-arithmetic-over-a-partitioned-aggregate-dimension-in-order --strict` — all green
