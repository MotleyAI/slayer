## 1. Tests first (spec-tests stage)

- [ ] 1.1 Unit tests for `SqlFragmentKey`: construction, equality/hash, `children()`/`map_children` reroot, phase ROW, `KIND_POLICY` + totality `SAMPLES` entry, aggregate kwarg union admits it; verify they fail before implementation
- [ ] 1.2 Binder tests (bare, dotted, cancelled, root-fallback, literal, expression, zero-ref expression, explicit string fragment in the query frame, marker strings untouched, stage/no-owner source, saved measure, re-aggregation with no row leaves → host owner); verify each asserts the bound kwarg on the key
- [ ] 1.3 Executed SQLite + DuckDB tests for every scenario in `specs/queries/semantics` and `specs/aggregations/formula-templates` of this change (default ≡ twin one producer, `wsum_fan` α, shadowed bare default, quoted mixed-frame default, edge-name revisit → circular error, E2 `CASE WHEN` default in plain / cross-model / association / windowed / second-order producers, filter- and order-position defaults incl. dotted + derived, string fragment ≡ unquoted twin, unparseable / aggregate / window / subquery text → typed bind error, missing column → unknown-column error, `window` reserved at save and at bind); oracles hand-computed
- [ ] 1.4 Regression test: a local (root-sourced) aggregate with a kwarg on a fanning path fails closed (`pop:wsum_cust_spend(weight=region_events.value)` rooted at `regions`) — already true today, must stay true
- [ ] 1.5 Axiom 2.7 spelling-invariance law test: for home, input safety, dependency closure, join registration and the cross-model kwarg gate, `w=<col>` and `w=<SqlFragmentKey over col>` give identical verdicts
- [ ] 1.6 `_first_unattributable_arg_leaf` names the parameter for an unsafe reference inside an expression default (`CASE`/arithmetic)
- [ ] 1.7 Public-name pin: `amount:wpop` keeps `amount_wpop`; internal aliases of a default and its twin are identical; distinct templates never alias
- [ ] 1.8 Law test `tests/test_law_param_text_bound.py` (no `Aggregation.params` / `AggregationParam.sql` access in `slayer/engine`, `slayer/sql`, `slayer/ir` outside the binder module + allowlist; no `str` kwarg rendered as SQL); verify it fails on current main
- [ ] 1.9 Tighten DEV-1954's `TestDefinitionDefaultSpelledByModelName` to SQL equality (per the issue comment)
- [ ] 1.10 Flip DEV-1908 `TestFailClosed::test_cancel_then_fanning_hop_names_region_events` to assert the α home and twin equality (user-consented)
- [ ] 1.11 Rewrite at the bind layer, asserted outcomes preserved (user-consented): percentile `p="pg_sleep(10)"` tests (`tests/test_sql_generator.py`, `tests/dialects/test_generator_delegation.py`), `tests/test_agg_render_spec.py` default fallbacks (`rolling_avg` fixture placeholder renamed off `window`), `tests/test_dev1745_fragment_joins.py` / `tests/test_dev1750_shifted_fragment_joins.py` fragment-join unit tests, direct calls to `resolve_default_qualifier_path` / `resolve_default_reference_paths` (import path only); delete `tests/test_agg_registry.py`'s `merge_agg_params` test with the function
- [ ] 1.12 Gotcha for spec-tests: the edge-name revisit scenario needs fixture edges `hr` (`customers → regions`) and `back` (`regions → customers`); probe that the shape reaches the binder; if no such fixture is constructible, STOP and ask (plan is frozen)

## 2. Core key + errors

- [ ] 2.1 Add `SqlFragmentKey` to `slayer/core/keys.py` (ValueKey union, aggregate arg/kwarg union, `KIND_POLICY`, `model_rebuild`); verify 1.1 passes
- [ ] 2.2 Add `UnanalyzableAggregationParameterError(SlayerError)` to `slayer/core/errors.py`; canonical encoding arm for `SqlFragmentKey` + booleans in `slayer/core/refs.py` / `slayer/sql/naming.py`; verify 1.7
- [ ] 2.3 Reword system P13 and add `[enforced: test:tests/test_law_param_text_bound.py]` to engine P1 exactly as approved; verify `poetry run python tools/arch_check.py`

## 3. Binder

- [ ] 3.1 Create the binder module (engine node): owner resolution as a typed result, one parse in `bundle.dialect`, AST gate (aggregate / window / subquery / non-expression → typed error), column / literal / `SqlFragmentKey` binding, template canonicalization; move `resolve_default_qualifier_path` / `resolve_default_reference_paths` here from `slayer/sql/column_expansion.py`; delete `requalify_default_references`; verify 1.2
- [ ] 3.2 Wire into `_bind_agg`: fill defaults after explicit kwargs; bind placeholder-naming `str` kwargs in the query frame; raise the requires-parameter error at bind; `window` reserved check for stored models; public auto-name from user-written kwargs only; verify 1.2, 1.7
- [ ] 3.3 `check_aggregation_definition` rejects `window` at save; verify 1.3 `window` scenarios

## 4. Planner

- [ ] 4.1 Replace column-only kwarg filters with generic leaf walks (`home_path_for` incl. deleting the `safe_reachable` default carve-out, `_first_unattributable_arg_leaf`, `_param_is_determined`, kernels' picked params, cross-model kwarg gate, join registration); verify 1.5, 1.6, 1.3 α
- [ ] 4.2 Delete the default route: `ParamSpec.expr_sql`/`expr_refs`, default half of `resolve_aggregation_params`, `_default_param_spec(s)`, `_default_params_closure`, `_canonical_default_sql`, `requalify_expr_to_paths`, `_reroot_picked_expr`, `PickedParam.sql`, `home._default_home_candidate_paths`/`_default_param_keys`, the `str` branch of `_leaf_closure`, `agg_registry.merge_agg_params`; `_child_keys` arm for `SqlFragmentKey`; verify full unit suite

## 5. Renderer

- [ ] 5.1 One parameter render API (scalar → literal, ValueKey → `render_value_key`); `render_value_key` arm for `SqlFragmentKey` via `SqlTemplate`; migrate every `AggRenderSpec` builder (plain, expression-source, HAVING/filter, windowed, picked-param, association, re-aggregation); verify 1.3 across producer kinds
- [ ] 5.2 `AggRenderSpec` carries the formula only; delete `ResolvedAggKwarg` `"str"` route, `param_defaults`, `_resolve_agg_param` default branch, `_register_fragment_kwarg_joins`, `_default_frag_entry`, `_fragment_placeholders`, `_is_fragment`, fragment half of `_resolve_fragment_kwargs`; verify 1.8

## 6. Docs, goldens, gates

- [ ] 6.1 `docs/concepts/models.md` (lines ~307/309): one sentence each — `window` reserved; a default computes identically to (and shares) its explicit spelling; unanalysable default text fails when queried
- [ ] 6.2 Re-bless goldens whose internal CTE / hidden-slot names changed (divergence protocol: review each diff is a naming-only change)
- [ ] 6.3 Full unit suite, integration suite (CI invocation), ruff, basedpyright (no baseline growth), arch_check, `npx -y likec4@1.47.0 validate architecture`
