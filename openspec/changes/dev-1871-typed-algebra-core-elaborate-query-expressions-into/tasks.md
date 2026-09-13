# Tasks: Typed algebra core

Standing gates for EVERY numbered group below: golden baselines byte-identical with empty `ALLOWED_DELTAS` (exceptions itemized in the task), `poetry run pytest -m "not integration"` green, law harness green, `ruff check` clean. Normative `architecture/**` edits are presented individually for approval before writing.

## 1. Prerequisite sync (DEV-1897 regime)

- [x] 1.1 Confirm DEV-1897 is merged into this branch (import-linter gone, child-level model-truth in CI); read its OpenSpec change and system §3/§5 for the measurement/coverage semantics; verify `poetry run python tools/arch_check.py` green on the merged tree and no `[tool.importlinter]` remains. (Only group 17 depends on this — the spec-tests stage and groups 2–16 do not block on DEV-1897.)

## 2. Alias insulation (before any key-shape change)

- [x] 2.1 Extract key→alias/CTE-name mangling into one pinned legacy-spelling serializer (emits today's exact tokens independent of field/class names); verify all goldens byte-identical
- [x] 2.2 Alias-stability tests pinning representative mangled aliases (incl. `grain_target`, `partition_keys_frozenset_…` spellings) and a producer-interning dedup-stability test; verify they fail if the serializer is bypassed

## 3. Locus rename

- [x] 3.1 make-refactor-target-compliant pre-pass over the `AggregateKey.grain` blast radius (incl. facade `getattr(key, "grain", "target")` sites); verify basedpyright no new errors
- [x] 3.2 Rename `AggregateKey.grain` → `locus` via dr-refactor; sweep dynamic-access sites; verify goldens + full suite + alias-stability tests green; record any deterministic-refactor skill gaps and fix the skill

## 4. Grain retype

- [x] 4.1 Retype `AggregateKey.partition_keys: Optional[Grain]`, `TransformKey.partition_keys: Grain`, strict validation rejecting raw set-likes; verify construction of a key with a bare frozenset raises
- [x] 4.2 Sweep non-annotation sites: both `map_children`, `rewrite_rank_partition_keys`, all `model_copy(update={"partition_keys": …})`, binding construction, helper annotations, raw-set comparisons; verify traversal-law test (every rebuilt/rerooted/substituted key carries a Grain)
- [x] 4.3 Explicit `Grain` construction across production + tests (mechanical only, no assertion changes); verify full suite green
- [x] 4.4 Memo-stability test (hash/eq equal to pre-retype for equal contents) and planning-time benchmark over the golden corpus; verify regression <5%

## 5. Terms and elaborated types (ir)

- [x] 5.1 `ir/terms.py`: Dataset protocol + ModelDataset/StageDataset/Aggregate/Transform/Broadcast/Field per design D1–D3; verify construction-invariant unit tests (Transform time-axis, Broadcast direction, Aggregate grain totality, identity/hash semantics)
- [x] 5.2 `ir` elaborated-output types (`ElaboratedQuery` typing environment); verify model-truth (ir imports core only) and unit tests green

## 6. Raise-site ledger and parity pins

- [x] 6.1 Complete raise-site ledger for stage_planner/regroup_planner/planning (location · message · category · final owner · family · user-error vs invariant), committed under the change dir; verify every `raise` in those modules appears exactly once
- [x] 6.2 Table-driven message-parity tests from the ledger (exact type + message per user-facing raise); verify they pass against current code BEFORE any guard moves

## 7. Elaborator skeleton (inert)

- [x] 7.1 `engine/elaborate.py` with `elaborate_query(...)` building `ElaboratedQuery` (terms memoized by key identity, position verdicts, broadcast insertions), checker raising nothing yet; called internally by `plan_query`; verify goldens + suite green (behaviour identical)
- [x] 7.2 Equivalence proof for raw and `prebound=` entry paths (elaborator runs in both, results unchanged); verify targeted tests + full suite

## 8. Compiler package split

- [x] 8.1 dr-refactor moves: stage_planner→`engine/compile/stages.py`, regroup_planner→`engine/compile/regroup.py`, planning→`engine/compile/projection.py`; migrate every direct importer (tests, prebound, query_engine `_topo_sort`) — no shims; verify full suite + import graph clean
- [x] 8.2 `compile_query(elaborated, …)` seam wrapping plan_query; `bind_query_inputs`/`plan_query` kept as compatibility wrappers; verify both entry paths tested

## 9.–15. Family migrations (one commit each; guards relocate with their family; parity tests + ledger updated in lockstep; deferral-site list may move, count stays 5)

- [x] 9.1 Computed dimensions / regroup roots (ungrained-aggregate-in-dimension guard family) migrated to term consultation; verify parity + goldens + laws (reserved-`__regroup__` raise stays in `_plan_regroups` until G16 — its firing condition is regroup discovery itself)
- [x] 10.1 Transforms / time-axis (both time-axis guard sites) migrated; verify parity + goldens + laws
- [x] 11.1 Local partitioned aggregates + partition_by validation family migrated; verify parity + goldens + laws
- [x] 12.1 Cross-model producers (+ attributability guards) migrated; verify parity + goldens + laws
- [x] 13.1 Association producers (eligibility guards) migrated; verify parity + goldens + laws
- [x] 14.1 Reaggregation producers (union-grain residue, outer restrictions) migrated; verify parity + goldens + laws
- [x] 15.1 Positions: filter/order typing (`type_position_conjunct`) relocated into the checker; verify parity + goldens + laws; broadcast-coherence assert live with its test

## 16. Orchestration reroute

- [x] 16.1 query_engine orchestrates elaborate→compile directly; compilers' remaining user-facing raises are exactly the ledger's non-checker rows; internal invariants become asserts; verify position×family test matrix, multi-stage + prebound recursion tests, full suite

## 17. Term boundary declaration (DEV-1897 regime — no import-linter, ever)

- [x] 17.1 [approval] Declare `syntax`, `binding`, `elaborate`, `compile` as engine children in `slayer.c4` with only the permitted arrows among them (none from `compile` into the other three; exact allowed set taken from measured edges at declaration time) + `children:` on the engine node in `index.yaml`; regenerate diagrams; diagram audit for every touched arc42 doc (right view, right entities — engine seam view decision presented); verify arch_check child-level model-truth green, and that adding a synthetic `compile → binding` import makes it fail (approved variant: seven flat children incl. `bind_inputs`/`elaborate_env`/`plan`, nine measured arrows, `engine_focus` view)
- [x] 17.2 [approval] engine + ir arc42 updates (building blocks, boundary principle tagged with the model-truth enforcement id per DEV-1897's tag convention, "terms add what keys lack"); verify tag validation

## 18. Population: anchors fold

- [x] 18.1 Two-phase anchor classification per design D11; ordering invariants (EMPTY_DETERMINATION, SIBLING_STAGE before NO_DATASOURCE) pinned by existing DEV-1866 tests; verify both new regressions (named-join saved-measure datasource scoping; sibling-name collision) plus existing fail-closed tests green

## 19. Population: route-aware inference

- [x] 19.1 Route-aware `probe_item` per design D11 (literal-first, provably-to-one hops, consistent verdicts, selected-route hops); verify all six routing regression cases green
- [x] 19.2 Flip `test_short_form_cross_model_dim_not_routed_when_rootless` (consented) to routes-and-infers; update `docs/concepts/queries.md#population`; verify `test_law_population_invariance` and spec scenarios green

## 20. Cleanup and closure

- [x] 20.1 Delete dead derivation code (`_root_grain` re-derivations etc.); verify goldens + suite green and coverage of deleted paths gone
- [x] 20.2 [approval] semantics tag flips only where a new test honestly enforces an axiom (candidates 5, 6-dimension-position, 11), each proposed separately; verify arch_check tag validation
- [x] 20.3 Docs/skills sweep for touched behaviour; verify grep for stale names clean; zensical nav unchanged (no new pages)
- [x] 20.4 Post the raw-key-consumer inventory (DEV-1896's migration input) as a comment on DEV-1896; verify comment posted
- [x] 20.5 Final: full suite (`integration or not integration` where DBs available), ruff, enforcement bundle, benchmark re-run; verify all green
