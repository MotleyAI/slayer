## 1. Fixtures and failing tests (spec-tests stage)

- [ ] 1.1 `tests/_dev1900_fixtures.py`: DEV-1840 graph + `region_events` (1:N from `regions`), `regions.bad_pop = pop + region_events.value`, `bad_pop2 = bad_pop * 2`, `regions.derived_pop = pop * 2`, customers `wsumx` (default `regions.bad_pop`), `wsumy` (default `regions.bad_pop * 1`), a customers column filtered on `regions.bad_pop > 0`, an unparseable derived column; SQLite + DuckDB seeds with two same-value North events; hand-computed oracles (associate 100 / broadcast 167 for `amount:sum` by `bad_pop`; cross-model 280; re-agg `derived_pop` weight 46.667; population-guard oracles 420 / 120 for DEV-1909). Verify: fixture smoke test loads both engines.
- [ ] 1.2 `tests/test_dev1900_closure.py`: fragment/key closure unit tests — structural prefixes, derived chain (`bad_pop2`), trivial rename contributes nothing beyond its path, single-reference derived alias, bare / owner-qualified / dotted / expression defaults at a non-empty owner path, StarKey prefixes only, TimeTruncKey delegates, opaque qualifier contributes nothing, unparseable → `None`, cycle raises; `models_by_name` host-first incl. per-stage bundle and extension overlay; hand-built bundle resolves the source→host reverse hop. Verify: all fail before implementation, pass after.
- [ ] 1.3 `tests/test_dev1900_input_safety.py`: fail-closed per gap × mode (path-bearing derived kwarg broadcast/associate/error; chain; local kwarg; dotted and expression defaults; measure-local filter on a fanning derived column; unanalyzable definition → new typed error, ref-free); re-agg fanning derived parameter → parameter error; positives (to-one derived default executes; derived parameter with local-only dependencies executes). Verify: raises pinned by message family, positives by executed values.
- [ ] 1.4 `tests/test_dev1900_dimensions.py`: derived fanning dimension broadcast (warning names the hop) / associate (100, 280) / error, local and cross-model; explicit `partition_by=` on it is the hard error outside associate. Verify: executed on SQLite + DuckDB.
- [ ] 1.5 `tests/test_dev1900_population_guard.py`: structural and derived fanning filters with an inline local aggregate raise in all three modes; to-one filter stays inline; dimension-only, producer-only (cross-model / partitioned / windowed), HAVING filter, filter-only and order-only aggregates, multi-stage `source_queries` unaffected. Verify: raises + executed values unchanged.
- [ ] 1.6 `tests/test_dev1900_home_path.py`: definition defaults among home candidates (to-one default deeper than the source; default whose LCP with the source is a shallower valid home); the back-hop default still fails closed (DEV-1908 pin). Verify: plan structure + raise.
- [ ] 1.7 `tests/test_dev1900_golden_sql.py` + `tests/golden/dev1900_sql_baseline.json`: the fail-closed shapes as recorded raises and the positive shapes across the seven Tier-1 dialects, blessed pre-implementation. Verify: golden harness green.
- [ ] 1.8 `tests/_dev1871_raise_ledger.py`: two new checker rows; `tests/test_law_guard_ratchet.py` untouched. Verify: raise-parity test red until the checker rules land, green after.

## 2. Model map

- [x] 2.1 `ResolvedSourceBundle.models_by_name` property (source first, referenced next, dedup by name) and the idiom sweep across engine / ir / sql; walkers keep `setdefault` for a caller-supplied root; delete `models_with_host` and the hand-patched bundle in `_ref_sql_dependency_paths`. Verify: 1.2 map tests green; full non-integration suite green.

## 3. Closure module

- [ ] 3.1 Create `slayer/engine/reference_closure.py` (deterministic-refactor moves of `aggregate_input_paths.py` and `column_filter_paths.py`; binder import updated): `fragment_closure`, `key_closure` (with `_child_keys` moved from `filter_reachability`), tri-state `None`. Verify: 1.2 closure tests green; `tools/arch_check.py` green.
- [ ] 3.2 Move `ParamSpec`, `default_param_value_key`, `column_default_key`, `expr_default_ref_keys` out of `compile/stages.py`; add `resolve_aggregate_inputs` and `aggregate_input_closure` (safety mode: attached inputs opaque; discovery mode: descend). Verify: DEV-1892 suites green; DEV-1859 plan-structure + goldens byte-identical.
- [ ] 3.3 `filter_reachability.compute_key_join_paths` delegates to `key_closure`; `_ref_effective_paths` / `_ref_sql_dependency_paths` / `_owning_model` deleted. Verify: DEV-1840 / DEV-1783 filter-reachability suites green.

## 4. Predicates consume closures

- [ ] 4.1 `grain_determines(…, bundle)` closure-aware (exact member first; every path pinned); callers thread `bundle`. Verify: 1.3 re-agg cases green; DEV-1892 / DEV-1847 determination suites green.
- [ ] 4.2 `key_attributable_from_root` and its adoption by `grain_member_attributable`, `_first_unattributable_arg_leaf`, `_first_unattributable_attached_leaf`, `_conjunct_disposition`, `_local_broadcasts`. Verify: 1.4 green; DEV-1841 / DEV-1836 suites and goldens byte-identical.
- [ ] 4.3 Input safety via `aggregate_input_closure` in `_assert_cross_model_inputs_safe` / `_assert_local_producer_inputs_safe`; association synthesis drops the `locus="host"` safety copy and the literal-1 substitution; new `check_input_dependencies_analyzable` in the checker. Verify: 1.3 green; DEV-1892 home-rooting suite green.
- [ ] 4.4 `_home_path` candidates from `resolve_aggregate_inputs`. Verify: 1.6 green.

## 5. Population guard

- [ ] 5.1 `check_population_filter_no_fanout` in the checker + the main-plan trigger after regroup discovery (`_assert_population_filters_no_fanout`). Verify: 1.5 green; DEV-1840 / DEV-1853 pushdown suites green.

## 6. Docs, architecture, gates

- [ ] 6.1 One sentence each in `docs/concepts/queries.md` (inputs, filters) and `docs/concepts/models.md` (derived columns). Verify: `zensical.toml` nav unchanged (existing pages).
- [ ] 6.2 `architecture/engine.arc42.md` §3 principle 10 — show the verbatim diff, apply on OK. Verify: `tools/arch_check.py` green (status tag validated).
- [ ] 6.3 Full non-integration suite, ruff, conventions gate, basedpyright baseline not grown, LikeC4 validate; any shifting existing test → stop and ask. Verify: all green.
- [ ] 6.4 DEV-1903 comment (discovery-opacity unification) posted; DEV-1908 / DEV-1909 pointers current.
