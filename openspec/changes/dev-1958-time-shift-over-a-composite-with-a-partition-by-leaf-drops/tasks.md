## 1. Failing tests first (spec-tests stage; SQLite + DuckDB via `tests/_dev1832_fixtures.py`; every executed case fails on main)

- [x] 1.1 `tests/test_dev1958_shifted_leaf_grain.py`: executed values for every "Composite-input time_shift" scenario over `monthly` — share, change/change_pct, trivial `/2`, non-time partition, re-aggregation by month, bare partitioned / ranked / windowed leaves under `date_range`, non-time partition under `date_range`, mixed conjunction frame bound, ranked leaf in a composite, windowed leaf in a composite keeping its window, two offsets sharing one relation (SQL has exactly one shifted producer CTE), unaligned `'day'` shift; verify each fails on main and passes after implementation (the mixed-conjunction frame bound already passes on main: regression guard)
- [x] 1.2 `tests/test_dev1958_shifted_population.py`: the association-restricted filter, the associate-mode fanning dimension, the attributable `customers.tier` partition key, and the fanning-hop partition key typed error on Graph A; re-derive every oracle from the raw rows in a smoke test (the `test_dev1832_fixtures_smoke.py` pattern) (the associate-mode dimension and the fanning-hop error already pass on main: regression guards)
- [x] 1.3 `tests/test_dev1958_stage_axis.py`: the two-stage share with a stage-level `date_range` (DEV-1471 style), executed on SQLite + DuckDB
- [x] 1.4 `tests/test_dev1958_row_leaf_ban.py`: the "Transforms reject grain-refining row-level leaves" shift scenarios — `time_shift(weight, -1)`, `change(weight)`, `change_pct(weight)`, `time_shift(hi_rev, -1)` rejected with the remedy text; `time_shift(store, -1)` with `store` projected legal with no extra rows; the DEV-1846 substring pins (`"time_shift"`, `"row"`, `"source_queries"`) still satisfied by the unified message
- [x] 1.5 `tests/test_dev1958_plan_structure.py`: a non-series `time_shift` slot has exactly one `attach_phase="shifted"` attach with `shift_of` = the slot, empty substitutions, join pairs covering the producer's complete grain; the nested `_cm_` for an axis-bearing partitioned leaf is keyed by that leaf's grain; a carried attach is the same object as the outer one; a series slot has no shifted attach; the mixed input (inline `amount:sum` + placeholder) classifies both; malformed plans (orphan / duplicate / series-targeting shifted attach, foreign `answer_slot_id`, non-empty substitutions) are rejected by the validators
- [x] 1.6 `tests/test_dev1958_cte_dependencies.py`: sql P6 ordering for a shifted producer over a carried nested producer and over an interned base producer (extend the `tests/test_dev1942_cte_dependencies.py` pattern); `_walk_regroup_attaches` yields a carried attach once and warnings are not duplicated
- [x] 1.7 `tests/test_dev1958_golden_sql.py` + `tests/golden/dev1958_sql_baseline.json`: the new shapes on the five golden dialects with `assert_scope_closed`; record the blessing procedure in `divergences.md` in this change folder
- [x] 1.8 Codex-review the test suite against this plan (spec-tests stage gate)

## 2. Core helpers

- [x] 2.1 `slayer/core/keys.py`: `shift_offset_of(key) -> (periods: int, granularity: Optional[str])` with the emitter's validation (integer periods, no bool, Decimal integral); unit tests in `tests/test_keys.py`; the emitter's inline parsing deleted
- [x] 2.2 `slayer/engine/compile/shift.py`: `_series_mode` moves here (one definition); `slayer/engine/compile/staging.py` imports it; existing staging tests unchanged

## 3. Plan model (`slayer/ir/planned.py`)

- [x] 3.1 `RegroupAttachPlan.attach_phase` accepts `"shifted"`; add `shift_of: Optional[SlotId]` and `answer_slot_id: Optional[SlotId]`; docstring updated
- [x] 3.2 `PlannedQuery` validators per design D4 (shift_of names a non-series time_shift slot, answer slot belongs to the producer, substitutions empty, no duplicate / orphan / series-targeting shifted attach); verified by task 1.5's malformed-plan tests
- [x] 3.3 `_validate_stage_order` and `regroup_producer_identity` cover the new phase without change — confirm with a test that a shifted producer's nested stage-order violation is caught

## 4. Planner (`slayer/engine/compile/`)

- [x] 4.1 `compile/shift.py::plan_shifted_producers(...)`: discover non-series `time_shift` keys from measures / order / filters (dedup by key); classify constituents per design D2 (bare aggregate → re-evaluate; placeholder → original → grain contains axis or ranked / windowed → re-evaluate, else carry); build the producer prebound via `_regroup_producer_prebound` (grain = projected dims + tds, consumer order / names, `main_time_key` = axis; measure = input with re-evaluated originals restored; inherited = stratum-0 masks minus date-range masks through `strip_frame_bounds` over the query's time-dimension raw columns); `compile_synthesized(..., population_filters=<parent>, carried_attaches=[...])`; return the shifted attaches — gotcha: check that `substitute_value_keys` descends `AggregateKey.partition_keys` when restoring originals
- [x] 4.2 `compile_synthesized` / `compile_prebound`: new `carried_attaches` keyword appended to the sub-plan's `regroup_attach_plans` (same objects); `compile_prebound` calls `plan_shifted_producers` after `_plan_regroups` and before `stage_slots`; verified by task 1.5
- [x] 4.3 Design D6 nesting rule in `_plan_regroups`'s `in_producer` filter (strict constituent of a composite answer, never the answer itself); regression tests over ordinary regroup and re-aggregation producers (`tests/test_dev1847_reaggregation_exec.py`, `tests/test_dev1928_*` stay green) plus the ranked-leaf composite scenario
- [x] 4.4 Design D5 answer naming so a bare-aggregate shift without a frame mask interns with the base producer; verified by the two-offsets scenario and a bare partitioned leaf without `date_range` reading the base `_cm_` (one producer CTE in the SQL)

## 5. Checker (`slayer/engine/elaborate_env.py`)

- [x] 5.1 `check_non_shift_transform_row_leaf` → `check_transform_row_leaf` (shift-family skip removed; `first` / `last` still exempt); `_check_shift_family_key` keeps only the boolean-input rule; one message with the three remedies; all call sites renamed; verified by task 1.4 and the existing DEV-1859 / DEV-1846 rejection tests

## 6. Generator (`slayer/sql/generator.py`)

- [x] 6.1 `_emit_time_shift_ctes_for_planned`: series path unchanged; re-aggregation path fetches the slot's shifted attach, renders it via `_render_producer_split` with `shifted_<alias>` pushed as the active split consumer (design D7), names the CTE with `cte_name_from_alias(prefix="shifted_")`, reads the answer alias, emits the sjoin with the lookup on the axis pair and equality elsewhere; the hidden-alias / public-alias bookkeeping unchanged
- [x] 6.2 Delete `_build_shifted_leaf_agg`, `_build_shifted_cte_where_parts`, `_shifted_where_part`, `_regroup_placeholder_map`, `_resolve_partition_expr`, the shifted `ScopeFrame`, the relabel arithmetic, the read-and-rebucket arm, and the `shifted_where_*` plumbing in `_run_transform_chain`; `_shift_preserves_bucket_starts` stays; the NOSONAR justification on the emitter rewritten or dropped
- [x] 6.3 `slayer/engine/query_engine.py`: identity-deduplicated `_walk_regroup_attaches`; `plan_has_semi_join_filters`, `_iter_plans_with_producers` and the warning collectors route through it (design D8); verified by task 1.6

## 7. Existing tests and goldens (consent per file before editing any existing test)

- [x] 7.1 `tests/test_dev1859_transform_row_leaf.py::TestShiftFamilyRegimeUnchanged` → rejection tests (the user's ban decision); ask before editing
- [x] 7.2 `tests/test_time_shift_period_boundary.py` — the two STRFTIME-count shape assertions re-pinned to the lookup form; the four value tests must pass unchanged; ask before editing
- [x] 7.3 Shape tests asserting the shifted bucket expression inside the shifted CTE (`tests/test_dev1474_time_shift_cross_model_partition.py`, `tests/test_dev1750_shifted_fragment_joins.py`, `tests/test_dev1750_guard_lift.py`, `tests/test_dev1837_filter_placement.py`, `tests/test_sql_generator.py`, dialect tests) — re-pin only those that fail, each with consent; the five DEV-1732 shift tests must pass unchanged
- [x] 7.4 Re-bless every moved time_shift golden (`dev1750`, `dev1800`, `dev1837`, `dev1846`, `dev1868`, `dev1747` baselines) with values re-verified by the executed suites; enumerate each movement class in `divergences.md`

## 8. Docs, lint, full suite

- [x] 8.1 `docs/concepts/formulas.md`: the composite-input paragraph states that a partitioned leaf keeps its own grain when shifted and that a row column is rejected like under any other transform; "bare-leaf or all-local composite" loses "bare-leaf"; grep docs for "read-and-rebucket" / "bare-leaf regime" leaves nothing stale
- [ ] 8.2 `poetry run ruff check slayer/ tests/` clean; `poetry run basedpyright` no new errors vs baseline; `poetry run python tools/arch_check.py` green (no arc42 edits needed)
- [ ] 8.3 `poetry run pytest -m "not integration" -n auto` fully green; then the CI integration invocation from `.github/workflows/ci.yml`
