# Proposal: Typed algebra core

## Why

The algebra's axioms are tested (DEV-1869) and documented (DEV-1870) but not load-bearing: `ValueKey` trees are syntax, planners dispatch on syntactic shape, and grain/shape errors are scattered across a 5000-line planner — closure is discipline, not structure. With the semantic wave complete and `slayer/ir` extracted (DEV-1872), the shapes the typed core must express exist and are pinned, so the consolidation can land now.

## What Changes

- **Typed terms in `slayer/ir`**: `Dataset` protocol (schema + declarative input-relation description; no render methods), `ModelDataset`, `StageDataset`, `Aggregate(home, recipe, grain)` implementing `Dataset`, `Transform` (time-axis checked at construction), `Broadcast` (explicit coercion node), `Field`. Terms only add what keys lack (resolved home, resolved total grain); `recipe` references the existing `AggregateKey`.
- **One elaboration pass** (`engine/elaborate.py`) producing an `ElaboratedQuery` typing environment — position verdict, home, resolved grain, and broadcast insertions for every top-level expression; per-occurrence terms for aggregates/transforms — with THE type checker raising every surviving algebra type error under strict type+message parity (three raise surfaces: parse/bind name resolution, the checker, emission-time).
- **Planners become compilers** (`engine/compile/`): semantic decisions read terms, never re-derive shape from key syntax; scattered guards relocate to the checker per a complete raise-site ledger; compilers keep no user-facing shape errors.
- **Grain retype**: `AggregateKey.partition_keys: Optional[Grain]`, `TransformKey.partition_keys: Grain`, strict (no set-like coercion); `AggregateKey.grain` locus literal renamed `locus` via deterministic-refactor; key→alias serialization first insulated behind a pinned legacy-spelling function so golden SQL stays byte-identical.
- **Population inference absorbs its deferred fixes**: anchors derive from the same saved-measure-excluding classification as determination items; rootless inference becomes route-aware (short-form and full-path dims infer identically or fail closed identically).
- **Boundary declaration**: the term-interface boundary is declared in the living-architecture model — `syntax`, `binding`, `elaborate`, `compile` become `engine` children in `slayer.c4`/`index.yaml` with only the permitted arrows among them (none from `compile` into the other three), enforced by child-level `model-truth` in CI under the DEV-1897 regime (a prerequisite that lands first and retires import-linter entirely; no import-linter configuration is ever added back).
- Out of scope, parked with issues + worktrees: DEV-1859/DEV-1868 shapes (deferral sites unchanged), DEV-1894 (error-family unification), DEV-1895 (sql slot-id grain), DEV-1896 (terms replace keys). Prerequisite: DEV-1897 (child-level model-truth; its OpenSpec change defines the measurement semantics this change's boundary declaration relies on).

## Capabilities

### New Capabilities

(none — terms, elaborator, and checker are internal structure under strict behavior parity)

### Modified Capabilities

- `queries/population`: rootless inference becomes route-aware (short-form dims probe via the same safe-route enumeration binding uses, per candidate); datasource scoping and sibling detection derive anchors from the saved-measure-excluding determination classification.
- `queries/dotted-dimension-routing`: uniform application extends to population inference as a routing surface, with determination requiring provably-to-one routed hops and literal resolution taking precedence over short-form routing.

## Impact

- `slayer/ir` (new `terms.py`, `elaborated.py`), `slayer/engine` (new `elaborate.py`, `compile/` package absorbing stage_planner/regroup_planner/planning, `population.py`, `dimension_routing.py`), `slayer/core/keys.py`, `slayer/sql/naming.py` (pinned alias serializer), facade `getattr` dispatch.
- `architecture/` (engine + ir arc42, slayer.c4 engine children + permitted arrows, index.yaml `children:`, diagrams — each edit individually approved; CI enforcement and the system §3/§5 measurement semantics arrive with DEV-1897, read once merged before wiring the boundary).
- Tests: goldens byte-identical throughout (empty `ALLOWED_DELTAS` except itemized blessings); law harness green at every commit; one pinned test flips by consent (`test_short_form_cross_model_dim_not_routed_when_rootless`); `docs/concepts/queries.md#population` note updated.
