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

- [ ] 2.1 `slayer/ir/prebound.py`: `PreboundQuery.semi_join_filters`; `slayer/ir/planned.py`: `SemiJoinFilter.root_relation`, `EmptyBaseGrainPlan.host_gated`; move `SemiJoinHop` / `SemiJoinFilter` to a shared ir module if the import would cycle. Verify: `tools/arch_check.py` green; `basedpyright` baseline not grown.

## 3. Checker

- [ ] 3.1 `slayer/engine/elaborate_env.py`: `check_population_filter_in_pushdown_scope(filter_text, reason)` and `check_filter_dependencies_analyzable(filter_text, column)`; delete `check_population_filter_no_fanout`. Verify: 1.5 green.

## 4. Population disposition

- [ ] 4.1 `slayer/engine/compile/stages.py`: `PopulationFilters` + `dispose_population_filters` (decision 1), views by grain paths (decision 2); `_conjunct_disposition` raises the analyzability error on a `None` closure. Verify: 1.2 structural / derived / two-branch cases green.
- [ ] 4.2 Host base consumer in `compile_prebound`: masks, texts, typings, `n_date_range` rebuilt from the host view; groups copied to the plan with the root assertion (decision 3); the out-of-scope residue check (decision 4); the backstop assertion (decision 5); `_assert_population_filters_no_fanout` deleted. Verify: 1.2 same-branch, raw-row, residue and unanalyzable cases green.
- [ ] 4.3 Producer consumers: `_regroup_producer_prebound` takes the population view (replacing `inherited` / `n_date_range`) and sets `semi_join_filters` on the producer prebound; local regroups, `_synthesize_wrap_attach`, `_synthesize_association_producer` (its own disposition loop deleted) and the broadcast-local synthesis consume it; excluded conjuncts land on `dropped_filter_warnings`; `_regroup_inherited_filters` deleted; the two post-hoc `model_copy` sites go. Verify: 1.2 partitioned, windowed, first/last, association, nested cases green; DEV-1840 / DEV-1841 / DEV-1853 suites green.
- [ ] 4.4 Empty-base gating in `_plan_empty_base_grain` (decision 6). Verify: 1.2 producer-only 82 / zero-rows case green.

## 5. Renderer

- [ ] 5.1 `slayer/sql/generator.py`: the placeholder branch of `_render_with_combined_attaches` applies `_semi_join_exists_conditions` and uses the host FROM + `LIMIT 1` when masks or groups exist; confirm a pushed conjunct discovers no join on any base path. Verify: 1.3 goldens re-blessed with EXISTS and no `orders` join; `assert_scope_closed` green.

## 6. Reporting

- [ ] 6.1 `slayer/core/warnings.py`: `measure: Optional[str] = None` + human message; `slayer/engine/query_engine.py`: top-level plan entries in `_collect_semi_join_pushed_warnings`. Verify: 1.2 warning assertions green; `test_dev1745_warning_contract.py` / `test_xdist_warning_serialization.py` green.

## 7. Docs, architecture, gates

- [ ] 7.1 `docs/concepts/queries.md`: one sentence replacing the guard sentence (Filters and Auto-Joins); the `warnings` table cell notes `measure` is `null` for a population push. Verify: `zensical.toml` nav unchanged.
- [ ] 7.2 `architecture/semantics.arc42.md` axiom 14 tag — show the verbatim diff, apply on OK. Verify: `tools/arch_check.py` green.
- [ ] 7.3 Full non-integration suite, ruff, conventions gate, basedpyright baseline not grown, `arch_check`, LikeC4 validate; every shifting test outside 1.4 / 1.6 stops for a ruling. Verify: all green.
- [ ] 7.4 Codex pass on the working tree before the push (standing rule). Verify: findings resolved or recorded.
