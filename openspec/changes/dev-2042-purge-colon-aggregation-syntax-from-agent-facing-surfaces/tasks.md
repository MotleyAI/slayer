Note: routine bulk rewrites (the §6 comment/docstring sweep, the ~50 importer expected-value updates in
1.10) may be delegated to subagents; judgment items stay in the main session. Consented test-logic
changes are exactly 1.6, 1.10's helper rewrites and 1.11; every other test edit only flips an expected
value from colon to functional, keeping colon inputs.

## 1. Tests (pr-tests stage; each fails before the fix)

- [ ] 1.1 `functional_agg_text` unit tests: every D1 case (plain, star, path-star, kwargs, window, ranked positional, empty parens) plus the round trip `functional_agg_text(*split_agg_suffix(x))` parsing to the same `parse_expr` node as colon `x`; verify they fail (helper missing)
- [ ] 1.2 Guard test: each importer (dbt, Cube incl. view facades, OSI) over its existing test fixtures — every emitted `ModelMeasure.formula` has no colon aggregation suffix outside string literals and parses with `parse_expr`; verify it fails on main
- [ ] 1.3 Guard test: the MCP tool descriptions (rendered from the server's tool list) and help content contain no colon aggregation spelling; verify it fails on `create_model`'s ``column:sum_sq``
- [ ] 1.4 Per-emitter functional-output tests: Cube windowed `sum(amount, window='30d')` and `count(*, window=…)`, view-facade `count(orders.*)` and `sum(orders.x)`; dbt `sum_boolean`, percentile `percentile(col, p=…)`, mapped agg, filtered leaf incl. an arg-bearing `agg_call`; OSI `sum(a) / count(*)`, `nullif(count(*), 0)`, `count_distinct(x)`, `median(x)`, `percentile(x, p=…)`
- [ ] 1.5 D2: `parse_formula("count(customers.*)")` parses to the same ref as `customers.*:count`
- [ ] 1.6 D2 (consented logic inversion): `tests/test_formula.py` `test_emits_warning` → `test_emits_no_warning`, asserting `_rewrite_funcstyle_aggregations("sum(revenue)")` emits no warning; plus an importer run over a fixture records no warnings
- [ ] 1.7 D3: an agg measure over a dropped (unparseable-SQL) column is dropped with a `COMPLEX_MEASURE` report entry quoting the functional formula; a calc measure over it, directly and through another calc measure, is dropped and reported; a measure renamed by a namespace collision (`_measure` suffix) over a dropped column is still dropped
- [ ] 1.8 D4: each remedy names the functional form and contains no `:<agg>` — `DerivedColumnFanningError`, the cross-hop input-safety remedy (ledger row `tests/_dev1871_raise_ledger.py` updated in lockstep), the transform row-leaf remedy (`tests/test_dev1958_row_leaf_ban.py`, `tests/test_dev1859_transform_row_leaf.py` assert the functional remedy), `param_binding` / `sql/generator` kwarg remedy, `split_entity_agg_ref` error, `_NO_COLON` reason, `parse_formula` bare-measure errors
- [ ] 1.9 D5: `recommend_root_model` with functional, colon and arg-bearing inputs (`percentile(…, p=…)`, `last(…, ordered_at)`, `weighted_avg(…, weight=…)`) returns functional paths, local and cross-model (`sum(customers.regions.population)`); update the expected paths in `tests/test_recommend_root_model.py` `TestAggSuffix`
- [ ] 1.10 Expected-value updates (colon inputs stay): `test_osi_expression.py`, `test_osi_converter.py` (consented: `.split(":")` / `.endswith(":sum")` helpers become functional-aware via `split_agg_suffix` / `parse_expr`), `test_cube_converter.py` (consented: `split(":")` helper and `endswith`), `test_cube_views.py`, `test_dbt_converter.py`, `test_dbt_metricflow_strengthen.py`
- [ ] 1.11 Consented: `tests/test_dbt_converter.py` vacuous negatives `"subtotal:sum" not in formula` / `"total:sum_orders" not in formula` → `"sum(subtotal)" not in formula` / `"sum(total)_orders" not in formula`
- [ ] 1.12 Spec scenario "Cross-spelling rename and filter-form matching" (functional-form): a filter `SUM(revenue)` resolves to the measure declared `{"formula": "sum(revenue)", "name": "rev"}` — confirm a test covers it (add one if none); if it fails, stop and raise it (suspected gap), do not narrow the scenario
- [ ] 1.13 Run the new tests; confirm each fails for the intended reason; existing colon-input parity tests in `tests/test_functional_aggregations.py` stay unchanged and green

## 2. Renderer and legacy validator

- [ ] 2.1 Add `functional_agg_text(*, source, suffix)` to `slayer/core/refs.py` next to `split_agg_suffix`; verify 1.1 passes
- [ ] 2.2 `core/formula.py` `_rewrite_funcstyle_aggregations`: remove the warning, accept `<ident-or-path>.*` first argument; verify 1.5–1.6 pass and `tests/test_formula.py` / `tests/test_dev1576_heals.py` stay green

## 3. Emitters (D1, D5)

- [ ] 3.1 `query_engine.py` `_emit_recommend_path` via `functional_agg_text`; verify 1.9
- [ ] 3.2 dbt `converter.py` sum_boolean, percentile, mapped agg, filtered leaf via `functional_agg_text`; verify dbt parts of 1.2, 1.4, 1.10–1.11
- [ ] 3.3 Cube `converter.py` `_STAR_COUNT` → `count(*)`, windowed agg, view-facade star-count and agg re-exports via `functional_agg_text`; verify Cube parts of 1.2, 1.4, 1.10
- [ ] 3.4 OSI `expression.py` agg / star count / count_distinct / count / median / percentile via `functional_agg_text`; verify OSI parts of 1.2, 1.4, 1.10

## 4. Cube validation (D3)

- [ ] 4.1 `_validate_offline` takes the `_MeasureInfo` map, indexes by `emitted_name`, drops + reports measures over dropped columns, fixpoints `known` / `_formula_parses` for dependent calc measures, deletes `split(":")`; verify 1.7

## 5. Remedies (D4)

- [ ] 5.1 Rewrite the remedies in `core/errors.py`, `engine/elaborate_env.py` (both), `engine/param_binding.py`, `sql/generator.py`, `engine/syntax.py`, `core/models.py`, `core/formula.py` (both), `mcp/server.py`; verify 1.3 and 1.8

## 6. Comments and docstrings (D7)

- [ ] 6.1 Sweep `slayer/` per D7 (files listed in the issue §4); verify a grep for colon aggregation examples in comments/docstrings shows only legacy-machinery descriptions, each labelled "legacy colon spelling"

## 7. Docs and architecture

- [ ] 7.1 `docs/dbt/dbt_import.md` mapping row → `<agg>(col)`; verify a colon-spelling grep over `docs/` is empty
- [ ] 7.2 Apply arc42 edits E1 and E2 verbatim from design.md (approved); verify `la-arch-check` (pinned uvx form) passes

## 8. Verification

- [ ] 8.1 `poetry run pytest -m "not integration"` green
- [ ] 8.2 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs baseline) green
- [ ] 8.3 `openspec validate dev-2042-purge-colon-aggregation-syntax-from-agent-facing-surfaces --strict` green
- [ ] 8.4 Residual-colon scan (broad pattern incl. custom aggregation names) over `slayer/` agent-visible strings, `docs/`, `architecture/` and the change's deltas: only the legacy-acceptance requirement and legacy-machinery comments remain
