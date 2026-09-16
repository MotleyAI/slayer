## 1. Fixtures and failing tests (spec-tests stage)

- [x] 1.1 `tests/_dev1900_fixtures.py`: add oracles — partitioned gold 190 / silver 230, windowed April 420, `*:count` gold 3 / silver 2, raw rows 5, two-branch 280, association same-branch new 100 / ok 150, nested avg 210, out-of-scope dims-only tiers {bronze, gold, silver}. Verify: fixture smoke loads both engines.
- [x] 1.2 `tests/test_dev1909_population_pushdown.py` (SQLite + DuckDB): every scenario of the two spec deltas — structural 420 and derived 120 in all three modes (no `orders` join in the dry-run SQL, `semi_join_pushed` entry with `measure` `None`, no Python warning); partitioned 190/230 with both entries; windowed April 420; `*:count` 3/2; first/last over a resolvable time axis; association same-branch 100/150; broadcast same-branch one `new` cell; two-customer two-`ok`-orders same-status cell (decision 5); two branches 280 with two entries; one filter string with a pushed and an inline conjunct; dims-only values unchanged; raw-row 5 rows; producer-only 82 and zero rows for a predicate nobody passes (empty-base spine); fanning-axis window equals its unbounded buckets; nested `avg(sum(spend, partition_by=tier))` 210 with the EXISTS in the producer body; host→target nesting body inspection; to-one filter 50 byte-identical SQL; out-of-scope conjunct → dims-only applies, partitioned producer drops + warns, inline aggregate typed error, error mode errors; unanalyzable typed error with and without an aggregate; ambiguous correlation from the host fails closed in every mode; `assert_scope_closed` on every dry run. Verify: all fail (or raise the guard) before implementation.
- [x] 1.3 `tests/test_dev1909_golden_sql.py` + `tests/golden/dev1909_sql_baseline.json`: the matrix across the seven Tier-1 dialects, blessed pre-implementation as today's SQL / recorded raises. Verify: golden harness green pre-implementation.
- [x] 1.4 Re-points: remove the six `strict` xfails (`test_dev1747_reroot_filter_routing.py` ×4, `test_dev1748_first_last_matrix.py` — its `FAN_RUSH_MULTIPLIED_SUM` oracle flips to the unmultiplied sum, `test_dev1840_execution.py`); remove the `reroot/unreachable_filter::` carve-out in `test_dev1747_golden_sql.py`; ALLOWED_DELTAS with reasons for `dev1747 reroot/unreachable_filter::*` and `dev1900 fanning/pop_filter_derived::*`; delete `tests/test_dev1900_population_guard.py`. Verify: the re-pointed tests fail before implementation.
- [x] 1.5 `tests/_dev1871_raise_ledger.py`: replace the two `check_population_filter_no_fanout` rows with `check_population_filter_in_pushdown_scope` and `check_filter_dependencies_analyzable`. Verify: raise-parity red until the checker rules land.
- [x] 1.6 Test-impact audit (design decision 10): list every customers-rooted fanning-filter test and every dropped-filter assertion; classify each as unchanged / re-blessed / newly `semi_join_pushed`; record the classification in this file. Verify: the list is complete against `grep`.

### 1.6 audit result (spec-tests stage)

Grep basis: `dropped_filter_warnings|UnreachableFilterDroppedWarning|unreachable_filter_dropped`
over `tests/`, plus customers-rooted queries whose `filters=` cross `orders.` /
`order_tags.` / `regions.region_events`. Full non-integration run classifies each: only
the files below fail (the intended reds), everything else stays green.

**Newly `semi_join_pushed` (added this change)**
- `tests/test_dev1909_population_pushdown.py` — population + host-producer suite (SQLite+DuckDB).
- `tests/test_dev1909_golden_sql.py` + `tests/golden/dev1909_sql_baseline.json` — 7-dialect matrix.
- `tests/_dev1900_fixtures.py` — added the executed-value oracle constants.

**Re-blessed / re-pointed (existing, changed this change)**
- `tests/test_dev1747_reroot_filter_routing.py` — 4 `strict` xfails removed; the vacuity,
  tags-narrows, no-warning and host-no-fanout tests now assert the association result.
- `tests/test_dev1748_first_last_matrix.py` + `tests/_dev1748_fixtures.py` — xfail removed,
  test renamed, `FAN_RUSH_MULTIPLIED_SUM` (215) → `FAN_RUSH_SUM` (144, the unmultiplied sum).
- `tests/test_dev1840_execution.py` — `TestBranchIndependence` xfail removed.
- `tests/test_dev1747_golden_sql.py` — `reroot/unreachable_filter::` carve-out removed;
  ALLOWED_DELTAS for its 5 keys (flip recorded raise → re-rooted SQL with the EXISTS).
- `tests/test_dev1900_golden_sql.py` — `fanning/pop_filter_derived` out of `FAIL_CLOSED`;
  ALLOWED_DELTAS for its 7 keys (raise → real SQL).
- `tests/_dev1871_raise_ledger.py` — the 2 `check_population_filter_no_fanout` rows swapped
  for `check_population_filter_in_pushdown_scope` + `check_filter_dependencies_analyzable`.
- `tests/test_dev1900_population_guard.py` — deleted (replaced by the pushdown suite).

**Unchanged (verified green; DEV-1909 does not alter their shapes)**
- Dropped-filter assertions in `test_dev1745_warning_contract`, `test_dev1769_routed_filter_path_validation`,
  `test_dev1836_filter_inheritance`, `test_dev1836_warning_collector`, `test_dev1838_interning`,
  `test_dev1840_disposition`, `test_dev1840_grouping`, `test_dev1840_strict_metadata`,
  `test_dev1841_association_filters`, `test_dev1841_warnings`, `test_dev1853_pushdown`,
  `test_dev1865_plan_shape`, `test_error_messages`, `test_sql_generator`,
  `test_xdist_warning_serialization`, `perf/compare/test_logic`, and the `_dev1836/1838/1840/1841` fixtures.
- Customers-rooted fanning-filter shapes in `test_dev1853_pushdown` (producer/associate pushdown,
  already handled by DEV-1840/1853) and `test_dev1866_execution` (population inference).

## 2. IR

- [x] 2.1 `slayer/ir/prebound.py`: `PreboundQuery.semi_join_filters`; `slayer/ir/planned.py`: `SemiJoinFilter.root_relation`, `EmptyBaseGrainPlan.host_gated` (+ `RegroupAttachPlan.semi_join_measure` for the producer push's public name); `SemiJoinHop`/`SemiJoinFilter` already share `ir.planned`, no cycle. Verify: `arch_check` + `basedpyright` clean (baseline unchanged).

## 3. Checker

- [x] 3.1 `slayer/engine/elaborate_env.py`: `check_population_filter_in_pushdown_scope(filter_text, reason)` and `check_filter_dependencies_analyzable(filter_text, column)` added, `check_population_filter_no_fanout` deleted; `first_unanalyzable_filter_column` added to `reference_closure.py`. Verify: 1.5 raise-parity green.

## 4. Population disposition

- [x] 4.1 `slayer/engine/compile/stages.py`: `PopulationFilters` + `dispose_population_filters` (decision 1), `producer_view` / `host_split` by grain paths (decision 2); `_conjunct_disposition` raises the analyzability error on a `None` closure. Verify: 1.2 structural / derived / two-branch green.
- [x] 4.2 Host base consumer in `compile_prebound`: `host_split` drops the pushed conjuncts from the masks (`_drop_pushed_population_conjuncts`, combined filters preserved), copies groups to the plan with the root assertion (decision 3), residue check (decision 4), backstop assertion (decision 5); `_assert_population_filters_no_fanout` deleted. Verify: 1.2 same-branch, raw-row, residue, unanalyzable green.
- [x] 4.3 Producer consumers via `_producer_filter_view`: local regroups, `_synthesize_wrap_attach`, the reaggregation carrier + outer, the broadcast-local (host-rooted) cross-model synthesis and the population-rooted association consume it, setting `semi_join_filters` on the producer prebound; excluded conjuncts land on `dropped_filter_warnings`; the two post-hoc `model_copy` sites removed. **Deviation:** `_regroup_inherited_filters` is KEPT as the sub-plan fallback (population is disposed only at the top level; a nested producer inherits its prebound's inline masks verbatim + the parent's groups by construction — re-disposing a sub-plan would wrongly re-push a materialised conjunct). A cross-model-metric association (root ≠ host) keeps its DEV-1841 metric-root disposition, not the host view. Verify: 1.2 partitioned/windowed/first-last/association/nested green; DEV-1840/1841/1853 green.
- [x] 4.4 Empty-base gating in `_plan_empty_base_grain` + renderer (decision 6). Verify: 1.2 producer-only 82 / zero-rows green.

## 5. Renderer

- [x] 5.1 `slayer/sql/generator.py`: the empty-base placeholder branch of `_render_with_combined_attaches` builds the host FROM + `LIMIT 1` and applies `_semi_join_exists_conditions` when masks OR `host_gated`. Verify: 1.3 goldens re-blessed with EXISTS and no fanning join; `assert_scope_closed` green.

## 6. Reporting

- [x] 6.1 `slayer/core/warnings.py`: `measure: Optional[str] = None` + branched human message; `slayer/engine/query_engine.py`: top-level plan `semi_join_filters` emit `(location, None, text)` entries. Verify: 1.2 warning assertions green.

## 7. Docs, architecture, gates

- [x] 7.1 `docs/concepts/queries.md`: guard sentence replaced (Filters and Auto-Joins); `warnings` table cell notes `measure` is `null` for a population push. Nav unchanged.
- [x] 7.2 `architecture/semantics.arc42.md` axiom 14 gains `[enforced: test:tests/test_dev1909_population_pushdown.py]` (approved 2026-09-16). `arch_check` green.
- [x] 7.3 Full non-integration suite green (18354 passed), ruff clean, conventions gate green, basedpyright baseline unchanged, `arch_check` + LikeC4 green. Shifting test outside 1.4/1.6: the DEV-1840 `exists/*` goldens (host base flips an unproven-hop conjunct to EXISTS) — ruled + re-blessed by Egor 2026-09-16.
- [x] 7.4 Codex pass on the working tree (standing rule). Three findings resolved: per-conjunct (not per-group) materialisation with per-conjunct hop subtrees so a materialised sibling's hop never inner-joins the EXISTS (+`TestPerConjunctMaterialisation` unit tests); the host-rooted cross-model branch threads `view.n_date_range`; the sub-plan fallback confirmed sound (nested producers have subset grains). Re-review clean, no new issues.
