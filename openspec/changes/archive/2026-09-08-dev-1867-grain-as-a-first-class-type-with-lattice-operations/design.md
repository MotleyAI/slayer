# Design — Grain as a first-class type

## Context

See proposal.md — Why. Grain-valued frozensets flow through: `regroup_root_grain`
(regroup_planner.py:127), `_effective_root_grain` / `_root_grain` / `own_grain`
(stage_planner.py:1235, 2560, 2572), nested-attach admission
(`_validate_nested_producer_plan`, stage_planner.py:1070–1096, with the
strict-vs-non-strict windowed distinction at 1090), functional-determination pruning
(`_prune_functionally_determined_grain`, 1282), producer grouping (`gkey`, 2638–2655),
ordering entry points (`_regroup_partition_order` / `_combined_order` /
`_regroup_producer_prebound`), the wrap-producer grain (1795–1798), and the cross-model
safe grain (`grain_keys`, 2354, including its `enable_nested` uses). Separately,
`partition_keys` lives on `AggregateKey` / `TransformKey` identity (core/keys.py) and
participates in `__hash__`/`__eq__`/`map_children`. Codex plan review 2026-09-08
resolved into the decisions below.

## Goals / Non-Goals

**Goals:** one home for grain lattice reasoning; every grain-valued planner variable is
a `Grain`; mixed `Grain`/frozenset use fails loudly.

**Non-Goals (deferred, detailed comment on DEV-1871):** retyping `partition_keys` on the
key types; renaming `AggregateKey.grain` (the `"target"|"host"` locus literal — an
unrelated concept sharing the name); the `slayer/sql` slot-id grain representation
(`grain_slot_ids`, `RankedGrainMember`, join-back builder — DEV-1872/1871 territory);
folding grain-member *ordering* into `Grain` (order is planner naming policy, `Grain` is
unordered).

## Decisions

1. **Home `slayer/core/grain.py`, not engine.** `Grain` wraps core `ValueKey`s and is a
   domain value type; core placement keeps the layers contract untouched and lets `sql`
   adopt it later without a new `sql → engine` edge. DEV-1872 relocates it to
   `slayer/ir` mechanically.
2. **Set-like dunders + named lattice predicates**, over named-methods-only: near
   drop-in diff, with the semantically loaded admission sites using named predicates
   (`broadcasts_into` / `is_strict_subgrain_of`) so direction is spelled out. Convention
   recorded in the module docstring.
3. **Operand contract.** Comparisons and `union()` accept `Grain` only; `__or__` and
   `__sub__` additionally accept `AbstractSet[ValueKey]` (the `| {active_bucket}` and
   bucket-difference sites); every set-producing operation returns `Grain`; anything
   else gets `NotImplemented`, so `grain <= frozenset(...)` and its reflected form raise
   `TypeError`. `__eq__` is `Grain`-only — a `Grain` never equals a raw frozenset, so
   mixed-representation drift fails in tests instead of surviving silently.
4. **`Grain.EMPTY` is a `ClassVar` canonical constant** assigned after class creation
   (a bare class attribute would parse as a Pydantic field). Equality is guaranteed
   (`Grain.of([]) == Grain.EMPTY`), identity is not.
5. **Broadcast direction is a predicate, not an error site.** `broadcasts_into(finer)`
   ⇔ `is_subgrain_of(finer)` (coarse→fine only); its docstring names the
   second-order-aggregation remedy for the reverse direction (accumulate within own
   grain first — the DEV-1839 remedy). All existing error message text stays
   byte-identical at the call sites; the windowed `<=` vs non-windowed `<` admission
   rule stays planner policy.
6. **Boundary sites stay frozenset**: `_validate_partition_keys` (its return is written
   back into keys via `rewrite_rank_partition_keys`); classification sets
   (`row_agg_set`, `dim_agg_set`, `dim_keys` in filter splitting — sets *of aggregates*
   or membership classifiers, no lattice ops); ordered grain lists (`ordered_pks`,
   `partition_display`).

## Risks / Trade-offs

- [Swapped receiver on a direction predicate compiles cleanly and reverses semantics] →
  focused tests on both `_validate_nested_producer_plan` branches (equal-grain windowed
  admitted / equal-grain non-windowed rejected / strict subgrain admitted / supergrain
  rejected) plus exact-string assertions on the admission messages where no existing
  test pins them.
- [Pydantic validation cost on planner hot paths] → planning is per-query, not per-row;
  acceptable; revisit under DEV-1871 if keys move off Pydantic.
- [Silent partial conversion leaves mixed plumbing] → `Grain.__eq__`/comparison
  operators reject frozensets, so any missed site that compares representations fails
  the suite; goldens must be byte-identical (no re-blessing).

## Migration Plan

Single PR; pure refactor gated by byte-identical goldens and the full unit suite. No
rollout or rollback concerns.
