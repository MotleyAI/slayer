## 1. Tests first (pr-tests)

- [ ] 1.1 `tests/test_dev2388_own_anchor.py` (SQLite + DuckDB, executed values), reusing `tests/_dev2212_fixtures.py` and the two-route schema of `tests/test_dev2212_home_routing.py`: every scenario of `queries/semantics` › An aggregation binds from its own anchor — two-route population in every consumer position (measure, filter, order, computed dimension with `partition_by=`, attached parameter), through another anchor (EAST 350 / WEST 300; + returns NORTH NULL/10; no dimension 650), the a16-shaped schema (item, date_dim, store_sales, store_returns with saved metrics: Acme 2023 125/10 · Acme 2024 200/NULL · Beta 2024 90/5), multi-anchor (order_lines home from customers / products / inferred; orders–tickets tie refused naming both spellings in canonical order and the first spelling executing; unconnected anchors → input-safety error; diamond, inherited edge, list child, keyless home), ranked and FILTER aggregations, the anchored spine with an empty month, metric-order invariance. Verify: fails for the right reason before implementation.
- [ ] 1.2 Same file: the re-aggregation operand scenarios of `queries/semantics` › Second-order aggregation over attached values — the carrier probe (`count(count(returns.*, partition_by=[customers.region, customers.segment]))` by region under `orders`, `customers`, inferred, inferred + `orders.total_revenue`), mixed grains with no full-grain constituent (EU 2 / Asia 1), an undefined union-grain anchor failing closed. Verify: fails before implementation.
- [ ] 1.3 Same file: restriction cases (codex finding 3) — model-filtered `customers` population by `orders.status` (completed 150, open 500) and by `customers.region` (EAST 300, NORTH NULL, WEST 300), RLS-hidden population rows, a filtered stage and a metric-filtered stage as population, a home reaching a restricted stage through an inherited reverse edge (`queries/multi-stage` rewritten scenario). Verify: fails before implementation where the value changes.
- [ ] 1.4 Same file: reference identity (codex finding 4) — two spellings in one expression, saved metrics composed through two frames, a definition default, a `FILTER (WHERE …)` and a ranking key each spelled through another anchor; per-home filters (codex finding 6) — a two-home filter, `OR` / `NOT`; association identity (codex finding 7) — repeated and NULL list elements, keyless duplicate rows; datasource (codex finding 11) — a nested saved metric's anchor in the population's datasource.
- [ ] 1.5 Remove the strict xfails in `tests/test_dev2212_home_routing.py` (dimension order) and `tests/test_dev2212_row_supply.py` (metric through another anchor); the anchored `population: "regions"` cases there stay as they are and must pass with `anchored_rows` deleted.
- [ ] 1.6 Existing tests contradicting the plan: ask the owner's per-file approval before aligning — tests pinning `_host_membership` (search `test_dev1909_population_pushdown.py`, `test_dev2219_*`, multi-stage restricted-stage tests for "restricts what is reached through it"), host-row carrier counts (`test_dev1847_reaggregation_exec.py`, `test_dev1942_*`, `test_dev1824_partitioned_execution.py`), the rewritten `queries/references` scenarios ("A unique fanning route is taken", "Several fanning routes are refused", "A single direct inherited relationship is taken first" in `test_dev2277_routing.py` / `test_dev2277_qualified_references.py`), and any test asserting the old "Explicit population unchanged" values.
- [ ] 1.7 Codex review of the tests against this change. Verify: findings resolved with the owner.

## 2. Architecture harnesses

- [x] 2.1 `architecture/semantics.arc42.md` edits applied verbatim (design.md decision 3). Verify: `poetry run la-arch-check` passes once `tests/test_dev2388_own_anchor.py` exists (its `[enforced: test:…]` tag needs the file).

## 3. Binding (engine.binding, engine.bind_inputs, core.keys, ir)

- [ ] 3.1 Bottom-up aggregate binding: resolve each aggregation's source anchor (one shared anchor; else the smallest dataset determining every leaf's anchor over provably to-one hops; tie → `UnresolvableDimensionRelationshipError` with sorted explicit spellings; none → input-safety error, raised in the checker per engine P9), then bind source, parameters, defaults, `FILTER`, ranking keys in that anchor's scope. Verify: 1.1 binding cases.
- [ ] 3.2 Key identity = anchor + canonical route for every query reference; the population's and each home's reach derived from it; retire `prebound.reference_anchors` and `_source_anchor`. Verify: 1.4 identity cases, `test_dev2212_home_routing.py` dimension order both ways, result-key tests unchanged.
- [ ] 3.3 Home resolution (`population/home.py`) from the anchor, not from host paths; `row_supply._home` agrees (one rule). Verify: `tests/test_dev1832_home.py`, `test_dev1931_default_home.py`, `test_dev1908_default_cancellation.py` green.

## 4. Producers (engine.compile)

- [ ] 4.1 `_synthesize_cross_dataset_producer` rooted at the home without `target_path`; grain and filter keys read from the home by their anchored spelling; join-back on grain values only. Verify: 1.1, `tests/test_dev1995_virtual_model.py`, `test_dev1836_producer_execution.py`.
- [ ] 4.2 Association arm: private home-row identity (PK / per-query row number / parent + element), dedup on (grain, identity), grain-value join-back. Verify: `test_dev1841_association_exec.py`, `test_dev1910_home_rooted_association.py`, `test_dev2216_keyless_association.py`, `test_dev2367_list_elements_across_joins.py`, `test_dev1914_ranked_windowed_association.py`, 1.4 association cases.
- [ ] 4.3 Per-home filter disposition in anchored queries too; delete `_host_membership`. Verify: 1.3, 1.4 filter cases, `test_dev1840_execution.py`, `test_dev2210_join_target_model_filters.py`.
- [ ] 4.4 Ranked / windowed / first-last: ordering keys from the anchor, axis from the home; product hops via `crosses_product_hop` re-expressed from the home. Verify: `test_dev1945_ranking_key_safety.py`, `test_dev1914_*`, `test_unrelated_datasets.py`.

## 5. Re-aggregation operand

- [ ] 5.1 `_synthesize_reaggregation_producer`: operand rows = the row supply of the inner query (union grain × constituents), coarser constituents broadcast, lacking cells take their empty value; outer aggregate joined on grain values. Verify: 1.2, `test_dev1847_reaggregation_exec.py`, `test_dev1824_partitioned_execution.py`, `test_reagg_outer_grain_attribution.py`.

## 6. Population planning (engine.population, query_engine)

- [ ] 6.1 Delete `anchored_rows`, `_anchored_rows`, `_StageInference.anchored` and their callers. Verify: `test_dev2212_home_routing.py` anchored cases (datasource case included) green.
- [ ] 6.2 `plan_rows`: drop hosting at a supplying home for the carrier; host = the dimension anchor else a full-grain home, binder scope only. Verify: 1.1 metric-order invariance, `test_dev2212_row_supply.py`, law harness files (`test_law_measure_monotonicity.py`, `test_law_home_row_preservation.py`, `test_law_unit.py`, `test_law_population_invariance.py`).

## 7. Docs

- [ ] 7.1 `docs/concepts/queries.md` (population decides rows only), `docs/concepts/references.md` (an aggregation reads from its own anchor; multi-anchor rule), `docs/upgrading/2.0.md` (breaking: restricted populations no longer narrow off-path homes; re-aggregations count their constituents' cells; multi-anchor ties refused; remedies). Verify: docs guard tests pass; `zensical.toml` nav unchanged (no new page).

## 8. Gates

- [ ] 8.1 Re-bless golden SQL only where the join-back, association or carrier shape changed and the executed twin is unchanged. Verify: golden suites green.
- [ ] 8.2 Local test scope per `/la:pr`, `poetry run ruff check slayer/ tests/`, `la-typecheck` (no new errors), `poetry run la-arch-check`, `npx likec4 validate`. Verify: all green.
