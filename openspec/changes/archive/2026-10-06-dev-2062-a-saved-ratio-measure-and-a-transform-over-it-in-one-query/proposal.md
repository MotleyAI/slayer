## Why

A query selecting a composite measure (e.g. a saved `aov = sum(amount) / count(*)`)
together with a transform, composite, filter or order entry that contains it fails with
`RenderContextMissingFacilityError` (DEV-2062, found by the slayer-evals benchmark), and a
transform over a bare literal crashes with `RuntimeError: transform input not
materialised` (DEV-1962). Both are one structural defect: the staging pass and the
alias-mode renderer disagree on how a later stage references an interned value, violating
engine P4/P6 and the Compositionality law.

## What Changes

- One reference rule: in alias mode, `render_value_key` reads any key whose slot is
  available in the current relation by that slot — the same rule the staging pass
  (`_direct_deps`) uses to decide which columns exist. A composite without an available
  slot renders structurally; an alias-slotted kind without one still fails closed.
- Retire the computed-dimension-only special case (`AliasFacilities.composite_alias_slot_ids`,
  `SQLGenerator._dimension_composite_slot_ids`).
- `_render_window_transform_sql` renders its input through that one call; its composite
  branch and its `transform input not materialised` branch retire, so a literal input
  renders as a literal and executes at the query grain.
- The law harness gains composite measures and a fixed composite-plus-consumer shape, so
  split-invariance covers this class.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/semantics`: Compositionality gains a scenario — a measure selected alongside a
  consumer that contains it changes nothing and never fails internally.
- `queries/transforms`: new requirement — window transforms over a constant input execute
  at the query grain.

## Impact

- `slayer/sql/render/value_expr.py` (alias-mode dispatch, `AliasFacilities`),
  `slayer/sql/generator.py` (window-transform input render, alias-context call sites).
- Tests: new executed-value and renderer unit tests; `tests/_law_harness.py` measure pool.
- No API, storage or docs change; `first`/`last` over an aggregate-free input stay an
  aggregation-bind refusal (typed conversion tracked by DEV-1969).
