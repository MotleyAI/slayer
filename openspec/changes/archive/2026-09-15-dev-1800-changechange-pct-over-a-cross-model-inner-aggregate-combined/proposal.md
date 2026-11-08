# Proposal: planner-owned materialisation stage (change / change_pct over a cross-model inner)

## Why

The issue's named shapes (`change(amount:wscaled_sum)`, `change_pct(customers.spend:sum)`)
now execute, but the class behind them is live: a composite mixing a transform over a
producer-bound input with a hidden local aggregate (`change(customers.spend:sum) +
amount:sum`, `iif(change(customers.spend:sum) > 0, customers.spend:sum, amount:sum)`, …)
still fails at render time with an internal missing-facility error. The SQL generator
re-derives which relation materialises each slot at 31 sites by inspecting key shapes or
alias availability, and the `_base` column set is assembled from four independent sources
that can disagree — engine P6 (phase is a property of the value) is only true for filters.
Every fix so far has been one more special case; the class stays open. The leveled
derived stage (design D10/D11) is an architectural prerequisite this class surfaced; the
issue's user-facing acceptance stays the change / change_pct executed-value matrix on
SQLite + DuckDB, the deeper nesting shapes serving as closure guards.

## What Changes

- The planner assigns every slot one materialisation stage (`BASE < PRODUCER < COMBINED
  < DERIVED(level)` — a value reading a transform sits one level above the deepest
  transform it reads: a transform is one more relation, a composite renders inline, and a
  transform's output is a dataset like any other) and a
  needs-column flag from one rule over its slot dependencies; a `PlannedQuery` whose slot
  references a later stage is rejected at plan time with a typed error; the generator
  refuses an unstaged plan.
- The generator becomes a stage partitioner: both render paths derive the `_base` column
  set, the combined-SELECT expressions, the transform steps per level plus the one fused
  derived-composite step (replacing alias-availability Kahn readiness), and filter / order
  placement from the slot stage. The
  key-walking classifiers, isolated-set builders, aux-slot collectors and the chain deadlock
  error are deleted.
- The `time_shift` regime (re-aggregation vs series) becomes a planner-owned fact on the
  transform slot, reproducing today's rule.
- Composites mixing a transform with any aggregate, transform or literal execute in
  measure, filter and order positions, hidden operands materialised.
- Generated SQL stays byte-identical for every already-legal shape; any divergence the
  rule surfaces (windowed-value filter placement, order-only composite materialisation) is
  listed for individual approval — no special cases are added to preserve an accident.
- `architecture/engine.arc42.md` P6 and `architecture/sql.arc42.md` P11 strengthened
  (target text in design D10, superseding the 2026-09-15 wording; re-presented as the exact
  diff for approval before applying), P6 tagged enforced by the new stage test.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: composites over transforms (mixed with hidden local aggregates,
  cross-model aggregates, literals, in measure / filter / order positions) become an
  accepted, executed-value-pinned shape — ADDED requirement.
- `queries/cross-model-aggregates`: "Every aggregate has exactly one disposition" widens
  to every value having exactly one planner-assigned materialisation stage, with
  later-stage references failing at plan time and SQL generation never re-deriving
  placement.

## Impact

- `slayer/ir/planned.py` (`Stage`, `ValueSlot.stage` / `needs_column` / `series`,
  `PlannedQuery` staging validator), `slayer/core/errors.py` (`MaterialisationStageError`),
  new `slayer/engine/compile/staging.py` wired from `compile_prebound`
  (`slayer/engine/compile/stages.py`), `slayer/engine/compile/projection.py`
  (`_iter_slot_deps` shared), `slayer/sql/generator.py` (routing sites replaced; ~600–800
  lines touched), `slayer/sql/render/order_terms.py` (scope derivation).
- Tests: new `tests/test_dev1800_materialisation_stage.py`, `tests/test_dev1800_execution.py`,
  `tests/_dev1800_fixtures.py`, golden baseline `tests/golden/dev1800_sql_baseline.json`;
  retargeted mechanism pins (approved): `test_dev1827_value_key_traversal.py`,
  `test_dev1777_emit_step_cte.py`, `test_dev1838_transform_predicate_scope.py`,
  `test_dev1733_order_only_transform_composite.py`, `test_planned.py`,
  `test_dev1746_projection_order.py`; `tests/_dev1871_raise_ledger.py` one row.
- Docs: one sentence in `docs/concepts/formulas.md`. Architecture: the two approved arc42
  edits; `architecture/index.yaml` unchanged (`queries` is already cross-cutting; the new
  module lives in the declared `engine.compile` child).
- Sequencing: DEV-1859 merged into this branch; `origin/main` merged forward once it lands.
