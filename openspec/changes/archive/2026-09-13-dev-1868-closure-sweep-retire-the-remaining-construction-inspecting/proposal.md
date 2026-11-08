# Proposal: Closure sweep — retire the remaining construction-inspecting guards

## Why

The closure axiom (`architecture/semantics.arc42.md` §2.9, `[target: DEV-1868]`) says an
operator may inspect only its operands' types — grain, home dataset — never how they were
constructed; every "X is not supported inside Y" refusal of a well-typed term violates it.
Four ratchet-pinned deferral guards plus the `time_shift` composite-rejection family are the
last such refusals; this sweep lifts each through existing primitives or pronounces it typed
residue, so the axiom can flip to enforced and the deferral ratchet shrinks to the one
DEV-1878 site.

## What Changes

- Cross-model first/last × `partition_by` compiles (checker raise removed; the existing
  target-rooted ranked producer already carries an explicit partition grain).
- A cross-model partitioned aggregate nested inside a transform compiles: the inner
  aggregate desugars to a producer placeholder the transform consumes, as local inners do.
- A cross-model aggregate operand inside an AGGREGATE-phase composite compiles: cross-model
  operands desugar to placeholders before phase classification, so the composite routes to
  the combined SELECT; the generator seam's raise becomes an internal invariant.
- `time_shift`/`change`/`change_pct` accept a nested transform input and a composite with
  cross-model aggregate leaves, via one new emission mode: shift a materialised series
  (the step/combined CTE output), joined back on the series grain; a shifted bucket absent
  from the series yields NULL. Aggregate-typed predicate inputs (boolean root) lift the same
  way. Row-level-leaf inputs stay rejected (DEV-1859) — that arm relocates to the checker.
- Typed residue, pronounced and documented once and for all: a transform inside a computed
  dimension must wrap an explicitly-grained aggregate (the ungrained default — the query's
  dimensions — would include the dimension being defined: self-referential). Split into two
  typed errors (`ValueError`, no issue ref): transform-needs-aggregate-input and
  every-contained-aggregate-declares-`partition_by`.
- Ratchet mechanics: 4 `DEFERRAL_SITES` entries removed, `guards.baseline` 5 → 1,
  `check_partitioned_measures` deleted, raise ledger updated, axiom 9 flips to enforced.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/partitioned-aggregates`: cross-model first/last with explicit `partition_by`,
  and cross-model partitioned aggregates inside transforms, become legal (coexistence
  requirement extends; deferral scenarios replaced by executed-value scenarios).
- `queries/cross-model-aggregates`: cross-model aggregates compose inside AGGREGATE-phase
  arithmetic/scalar composites (mixed with local operands and literals) and as `time_shift`
  composite leaves.
- `queries/transforms`: the `time_shift` rejection requirement shrinks to row-level-leaf
  inputs only; nested-transform inputs, cross-model composite leaves, and aggregate-typed
  predicate inputs become accepted shapes with defined series-shift semantics (NULL outside
  the materialised series).
- `queries/computed-dimensions`: the grain self-containment error surface splits the
  transform arm into two deliberate typed errors (transform-needs-aggregate;
  explicit-grain-per-contained-aggregate) — documented as permanent residue, not deferral.

## Impact

- `slayer/engine/elaborate_env.py` (guards removed/split/relocated),
  `slayer/engine/compile/stages.py` + `compile/projection.py` (desugar routing),
  `slayer/sql/generator.py` (series-shift emission mode, composite classifier, invariants).
- `tests/_law_harness.py` (DEFERRAL_SITES −4, law pools + cross-model partitioned
  operands), `tests/_dev1871_raise_ledger.py`, `tests/test_law_guard_ratchet.py` fixture
  surface, `architecture/index.yaml` (`guards.baseline` 5 → 1),
  `architecture/semantics.arc42.md` axiom 9 (user-approval-gated edit).
- Golden SQL per the DEV-1836 D10 protocol; `docs/concepts/formulas.md` / `queries.md`
  one-sentence updates; Linear: DEV-1873 residue list amended, DEV-1859 noted.
