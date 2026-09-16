# Tasks

## 1. Failing tests (TDD, spec-tests stage)

- [x] 1.1 `tests/test_dimension_routing.py` — unit tests for the pure resolver: unique / ambiguous-one-safe / ambiguous-multi-safe / ambiguous-zero-safe / unreachable, and the oriented safe hop set (`_safe_hops`): excludes fan-out + undetermined orientations, includes declared-to-one, unique-key-covered, and safe *reverse* orientations, keeps parallel named edges distinct with edge-name tokens, drops unnamed-parallel hops. Verify: file present, tests fail (module absent).
- [x] 1.2 `tests/test_dev1856_short_form_routing.py` — behaviour via `SlayerQueryEngine` + YAML storage covering every scenario in `specs/queries/dotted-dimension-routing/spec.md`: unique route (result key = full path), fan-out tie-break (incl. 3-route/1-safe resolves, 3-route/2-safe rejects), unreachable, broken chain (first- and later-hop) with/without suggestion, unique-fan-out route resolves, reverse-hop route resolves, named-parallel oriented tie-break (edge-token result key), unnamed-parallel unroutable (no suggestion), adjacent-parallel stays `AmbiguousJoinPathError`, dimension + cross-model measure + star agg + filter + ORDER BY + time dimension, routed dimension type preserved + opaque routed dim rejected, routed saved measure name+type, main_time_dimension short form, reverse-edge oriented safety, absent-bundle-target stays unknown-ref, downstream stage stays illegal-scope, cross-datasource non-candidate, re-rooted saved-measure nested routing + round-trip. Verify: tests fail before implementation (boundary guards pinning DEV-1853 behaviour may already pass).
- [x] 1.3 Schema-drift test: a persisted stage with `Consumer.email` routed through `Consumer` cascades when `Consumer.email` is dropped AND when the intervening `Customer → Consumer` join is dropped; ambiguous/unreachable short form attributes to nothing without raising. Verify: fails before implementation.
- [x] 1.4 Rewrite `tests/test_dev1780_missing_join_path.py::TestUnboundPathsReject` + module docstring to the new behaviour (route / typed `UnresolvableDimensionJoinError`); leave `TestValidPathBindsAllJoins`, `TestCountSimplePaths`, `TestDownstreamStageScope`, circular-join untouched. Verify: rewritten cases fail before implementation.
- [x] 1.5 Codex-review the test suite against the spec (spec-tests stage gate).

## 2. Routing core

- [x] 2.1 New `slayer/engine/dimension_routing.py`: `_resolve_route`, `route_dotted_target` (raises), `short_form_route_or_none` (probe) — both return executable token paths; `_safe_hops` (directed oriented-to-one hop set via `join_walker.neighbors` + `provably_to_one(edge=…)`, edge-name tokens, unnamed-parallel dropped); full-graph trichotomy via post-1853 `JoinGraph.count_simple_paths`. Verify: 1.1 passes.

## 3. Binder routing

- [x] 3.1 `binding._walk_join_chain` returns `(terminal, effective_hop_path)`; routes on `len==1` when `resolve_hop` is None, broken-chain error on `len>=2`, `AmbiguousJoinPathError` propagates untouched, absent-target stays `UnknownReferenceError`. Update callers `_resolve_dotted` and `_resolve_dotted_star`. Verify: routing dimension/star/filter/order/time-dim cases in 1.2 pass.
- [x] 3.2 Add `BoundExpr.routed_dotted` + `dotted_from_key` + `_canonical_if_routed`; set in `bind_expr` / `bind_time_dimension` for whole-field routed refs only. Verify: routed vs non-routed naming behaves per 1.2 (existing keys unchanged).

## 4. Naming / type / opaque

- [x] 4.1 `stage_planner._declared_measures_from_query`: bind-first for dimensions + time dims, `canonical = bound.routed_dotted or full` feeding `_flatten_dotted` / `_type_for_dimension` / `_reject_opaque_grouping_dim` / `_format_description_for_dimension` / `_guard_flatten` origin. Verify: routed-dim type + opaque + result-key tests in 1.2 pass; existing golden result keys unchanged.
- [x] 4.2 `_resolve_saved_measure_ref` routing-aware, returns `(terminal, measure, canonical_ref)`; update all consumers (`_saved_measure_public_name`, `_saved_model_measure_type`). Verify: routed saved-measure test in 1.2 passes.

## 5. Schema-drift

- [x] 5.1 `schema_drift._attribute_ref_to_base` routes a `len==1` unresolvable prefix via `short_form_route_or_none` over datasource-filtered `models_by_name`; ambiguous/unreachable → None. A routed ref's stage cascades when the terminal column OR the terminal-reaching join is dropped — mirroring full-path refs exactly (short≡full). Earlier intervening-join drops don't cascade for full paths either; closing that gap for both forms is deferred to DEV-1885. Verify: 1.3 passes (column-drop and terminal-reaching-join-drop cases).

## 6. Docs + full suite

- [x] 6.1 `docs/concepts/references.md` short-form auto-routing subsection (one concise sentence per point); check `docs/concepts/queries.md` and `.claude/skills/slayer-query.md`. Verify: docs build / links intact.
- [x] 6.2 `poetry run pytest -m "not integration"` all green; `poetry run ruff check slayer/ tests/` clean. Verify: both pass.
