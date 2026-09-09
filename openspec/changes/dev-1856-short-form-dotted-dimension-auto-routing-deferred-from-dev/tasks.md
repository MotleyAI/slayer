# Tasks

## 1. Failing tests (TDD, spec-tests stage)

- [ ] 1.1 `tests/test_dimension_routing.py` — unit tests for the pure resolver: unique / ambiguous-one-safe / ambiguous-multi-safe / ambiguous-zero-safe / unreachable, and the fan-out-free subgraph excludes fan-out + undetermined edges, includes unique-key-covered edges. Verify: file present, tests fail (module absent).
- [ ] 1.2 `tests/test_dev1856_short_form_routing.py` — behaviour via `SlayerQueryEngine` + YAML storage covering every scenario in `specs/queries/dotted-dimension-routing/spec.md`: unique route (result key = full path), fan-out tie-break (incl. 3-route/1-safe resolves, 3-route/2-safe rejects), unreachable, broken chain (first- and later-hop) with/without suggestion, unique-fan-out route resolves, dimension + cross-model measure + star agg + filter + ORDER BY + time dimension, routed dimension type preserved + opaque routed dim rejected, routed saved measure name+type, main_time_dimension short form, reverse-INNER edge safety, absent-bundle-target stays unknown-ref, downstream stage stays illegal-scope, cross-datasource non-candidate, re-rooted saved-measure nested routing + round-trip. Verify: tests fail before implementation.
- [ ] 1.3 Schema-drift test: a persisted stage with `Consumer.email` routed through `Consumer` cascades when `Consumer.email` is dropped; ambiguous/unreachable short form attributes to nothing without raising. Verify: fails before implementation.
- [ ] 1.4 Rewrite `tests/test_dev1780_missing_join_path.py::TestUnboundPathsReject` + module docstring to the new behaviour (route / typed `UnresolvableDimensionJoinError`); leave `TestValidPathBindsAllJoins`, `TestCountSimplePaths`, `TestDownstreamStageScope`, circular-join untouched. Verify: rewritten cases fail before implementation.
- [ ] 1.5 Codex-review the test suite against the spec (spec-tests stage gate).

## 2. Routing core

- [ ] 2.1 New `slayer/engine/dimension_routing.py`: `_resolve_route`, `route_dotted_target` (raises), `short_form_route_or_none` (probe), `_to_one_adjacency` (fan-out-free graph via `provably_to_one`). Verify: 1.1 passes.

## 3. Binder routing

- [ ] 3.1 `binding._walk_join_chain` returns `(terminal, effective_hop_path)`; routes on `len==1` no-declared-join, broken-chain error on `len>=2`, absent-target stays `UnknownReferenceError`. Update callers `_resolve_dotted` and `_resolve_dotted_star`. Verify: routing dimension/star/filter/order/time-dim cases in 1.2 pass.
- [ ] 3.2 Add `BoundExpr.routed_dotted` + `dotted_from_key` + `_canonical_if_routed`; set in `bind_expr` / `bind_time_dimension` for whole-field routed refs only. Verify: routed vs non-routed naming behaves per 1.2 (existing keys unchanged).

## 4. Naming / type / opaque

- [ ] 4.1 `stage_planner._declared_measures_from_query`: bind-first for dimensions + time dims, `canonical = bound.routed_dotted or full` feeding `_flatten_dotted` / `_type_for_dimension` / `_reject_opaque_grouping_dim` / `_format_description_for_dimension` / `_guard_flatten` origin. Verify: routed-dim type + opaque + result-key tests in 1.2 pass; existing golden result keys unchanged.
- [ ] 4.2 `_resolve_saved_measure_ref` routing-aware, returns `(terminal, measure, canonical_ref)`; update all consumers (`_saved_measure_public_name`, `_saved_model_measure_type`). Verify: routed saved-measure test in 1.2 passes.

## 5. Schema-drift

- [ ] 5.1 `schema_drift._attribute_ref_to_base` and `_resolve_dotted_ref_to_model`/`_walk_alias_to_target_model` route a `len==1` non-direct prefix via `short_form_route_or_none` over datasource-filtered `models_by_name`; ambiguous/unreachable → None. Verify: 1.3 passes.

## 6. Docs + full suite

- [ ] 6.1 `docs/concepts/references.md` short-form auto-routing subsection (one concise sentence per point); check `docs/concepts/queries.md` and `.claude/skills/slayer-query.md`. Verify: docs build / links intact.
- [ ] 6.2 `poetry run pytest -m "not integration"` all green; `poetry run ruff check slayer/ tests/` clean. Verify: both pass.
