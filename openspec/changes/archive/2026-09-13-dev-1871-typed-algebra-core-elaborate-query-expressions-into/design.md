# Design: Typed algebra core

## Context

See proposal.md — Why. Current state: `slayer/ir` holds plan shapes, bound types, `Grain` (DEV-1872); `engine/stage_planner.py` (~5030 lines) binds, guards, and plans; grain/shape guards are scattered raises; `AggregateKey.partition_keys` is a raw `Optional[frozenset[ValueKey]]`; key→alias serialization embeds Python field/class names (`grain_target`, `partition_keys_frozenset_…`) directly into emitted SQL aliases; the LikeC4 model carries `model-truth` exact-equality, extended to declared-child granularity by the prerequisite DEV-1897 (which retires import-linter before this change lands). Constraints: golden SQL byte-identical at every commit (empty `ALLOWED_DELTAS` except itemized blessings), law harness green, guard-ratchet count fixed at 5, error type+message parity, `.c4`/arc42 edits individually approved.

## Goals / Non-Goals

**Goals**: axioms load-bearing as term types + one checker; compilers shape-error-free; the enforcement regime (normative model, targeted contracts, CI). **Non-Goals**: new query capability (except the two spec deltas); DEV-1859/1868 shapes; replacing ValueKey identity (DEV-1896); error-vocabulary unification (DEV-1894); sql slot-id grain (DEV-1895). Child-level model-truth (DEV-1897) is a prerequisite, not part of this change.

## Decisions

**D1 — Terms annotate keys; keys stay the identity spine (Option A).** `ValueKey` remains the sole carrier for interning, substitution, naming, render addressing. Terms add only what keys lack: resolved `home: Dataset` and resolved total `grain: Grain`; `Aggregate.recipe` is a *reference* to the `AggregateKey`, never a copy. Alternative (terms replace keys) rejected here: conflates type introduction with an identity-spine swap; parked as DEV-1896 with the design constraint that term surfaces never leak key internals, keeping it open.

**D2 — `ElaboratedQuery` is a typing environment, not a parallel tree.** No typed nodes for arithmetic/literals/predicates (Codex finding 1, partial-reject). Every top-level expression (measure, dimension, filter conjunct, order target) gets: position verdict (field/measure), home dataset, resolved grain, and `Broadcast` insertions at grain-union points; every aggregate/transform occurrence gets its term, memoized by key identity. Compilers read semantic decisions from the environment; identity walks over keys remain legitimate. `ElaboratedQuery` and terms live in `ir` (representation); the elaborator lives in `engine/elaborate.py`.

**D3 — `Dataset` describes, never renders.** Protocol = schema + declarative input-relation description (model-backed / producer-plan-backed / stage-backed). `slayer/sql` keeps sole Node-building authority — self-rendering would fork the one render pipeline and require an upward ir→sql dependency. `Dataset` is a `typing.Protocol` consumption surface over concrete frozen Pydantic models; identities: `ModelDataset` by (datasource, model name), `StageDataset` by stage name, `Aggregate` by (home identity, recipe key, grain).

**D4 — Three raise surfaces, strict parity (Codex finding 2 resolution, acceptance wording amended by consent).** (1) parse/bind: name resolution and reference grammar — includes `AmbiguousJoinPathError` from `core/join_walker.py`, raised *within* the one elaboration pass (binding is its sub-phase); (2) THE checker: every algebra type error; (3) emission-time: the two `sql/generator.py` deferral sites. Relocated errors keep exception type + byte-identical message; improvements only as individually blessed `ALLOWED_DELTAS` keys with reasons. A **raise-site ledger** (location · message · category · final owner · migration family · user-error vs invariant) is built before any family migrates and drives table-driven parity tests; the ledger covers every user-facing raise in stage_planner/regroup_planner/planning, including window validation, raw-row-mode rejections, name collisions, stage-DAG errors, reserved-prefix collisions, and time-dimension resolution.

**D5 — Broadcast is a normative compiler input (Codex finding 7, partial-fold).** Grain alignment in compilation reads the elaborator's `Broadcast` nodes; a grain-differing combine without one is an elaboration bug caught by a coherence assert. Broadcast never rewrites the key tree and never changes SQL.

**D6 — Alias-serialization insulation precedes any key-shape change.** The key→alias/CTE-name mangling is extracted into one pinned legacy-spelling serializer emitting today's exact tokens (`grain_target`, `partition_keys_frozenset_…`) independent of Python field/class names, proven by goldens before the `locus` rename (deterministic-refactor, compliance pre-pass on the facade `getattr(key, "grain", "target")` sites) and the Grain retype land. `_structural_fingerprint` (interning-only, not SQL-visible) is exempt but pinned by a dedup-stability test.

**D7 — Grain retype mechanics.** `Optional[Grain]` on `AggregateKey` (None = inherit context; `Grain.EMPTY` = explicitly scalar), bare `Grain` on `TransformKey`; strict validation (no set-like coercion). Known non-annotation sites (Codex finding 6): both `map_children` implementations rebuild via `frozenset(...)` + unvalidated `model_copy`; `planning.rewrite_rank_partition_keys`; every `model_copy(update={"partition_keys": ...})` site; binding construction sites; helper annotations; raw-set comparisons now violating Grain-only equality. A traversal-law test asserts every rebuilt/rerooted/substituted key still carries a `Grain`. Hash/eq must equal today's for equal contents (memo-stability test); Pydantic kept unless the planning benchmark regresses >5%.

**D8 — Compiler split early, boundary declared late.** `engine/compile/` (stages.py, regroup.py, projection.py) created by deterministic-refactor moves right after the elaborator skeleton, so family migrations happen in final locations with no shims; the engine children + permitted arrows are declared only in the enforcement commit, once compile modules genuinely shed syntax/binding/elaborate imports — declaring earlier would fail child-level model-truth against the still-present edges (or need throwaway `#legacy` arrows). Public seams: `elaborate_query(...) → ElaboratedQuery` and `compile_query(elaborated, ...)`; `bind_query_inputs`/`plan_query` remain as compatibility wrappers until callers (including tests and prebound recursion) are migrated. Producer sub-plans are compiler-synthesized from terms — no compile→elaborate import.

**D9 — Staged reroute (Codex finding 13).** (1) terms + elaborator land inert behind existing orchestration; (2) `plan_query` calls the elaborator internally, raw and `prebound=` entries proven equivalent; (3) query_engine orchestration reroutes; (4) population work moves last.

**D10 — Enforcement regime (DEV-1897 prerequisite).** DEV-1897 lands first: import-linter is retired entirely (`[tool.importlinter]` deleted, `lint-imports` out of the bundle; never added back), and the one import law is arch_check `model-truth` in CI — declared LikeC4 arrows equal AST-measured runtime edges (TYPE_CHECKING excluded) at the finest *declared* granularity; the 7 grandfathered pairs become child-level `#legacy` arrows and the ratchet counts declared legacy arrows. This change expresses the term boundary purely by declaration: `syntax`, `binding`, `elaborate`, `compile` declared as engine children (a ban is expressible exactly between declared children — undeclared modules attribute to the node), with only the permitted arrows among them (`elaborate → binding`, `elaborate → syntax`, `binding → syntax`; none from `compile` into the other three — exact arrow set finalized against measured edges at declaration time). DEV-1897's OpenSpec change and system §3/§5 define the measurement semantics — read once merged, before wiring the boundary. Residual honestly documented (Codex finding 10, partial-reject): compilers may import `core.keys` for identity; interface-only consumption beyond the declared-children ban is review-guarded — no AST checker.

**D11 — Population algorithms.** *Anchors fold*: two-phase — syntactic anchor names select candidate datasource(s) and load models as today, then anchors and determination items are reclassified model-aware (saved-measure-excluding) and the precedence checks re-run on the final classification, preserving `EMPTY_DETERMINATION` / `SIBLING_STAGE` before `NO_DATASOURCE`; edge cases: same-named models across datasources, a saved measure whose first segment is a named join. *Route-aware inference* (Codex finding 8, folded): literal resolution first; short-form routing only where the literal single-hop anchor fails; every selected-route hop `provably_to_one` (binding's unique-but-fanning acceptances do NOT count for determination); consistent `_AMBIGUOUS`/`_UNREACHABLE`; hops from the selected route.

## Risks / Trade-offs

- [Alias drift from key-shape changes] → D6 insulation commit + alias-stability tests before retype/rename.
- [Message drift during guard relocation] → parity tests pinned from the ledger before any move; goldens record class+message.
- [Guard-ratchet lockstep] → deferral-site list updates in the same commit as any relocation; count stays 5.
- [Transitional double-derivation cost (terms computed while old derivations remain)] → accepted temporarily; end-state benchmark gate.
- [Population restructure regressing fail-closed ordering] → ordering invariants pinned by existing DEV-1866 tests, named in tasks.
- [dr-refactor gaps on dynamic access] → make-refactor-target-compliant pre-pass; skill refined as part of this change (deliberate fitness test).
- [Huge PR review] → per-commit invariants (goldens, laws, suite) + the ledger as the review index.

## Migration Plan

The ordered commit plan lives in tasks.md; every commit leaves goldens byte-identical, law harness and full suite green. Rollback: any commit is individually revertible pre-merge; the branch merges only when spec-review converges.

## Open Questions

None — remaining micro-choices (exact module names inside `compile/`, serializer placement) do not change specs, approach, or tasks.
