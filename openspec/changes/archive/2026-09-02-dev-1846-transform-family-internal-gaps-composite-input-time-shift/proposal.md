# Proposal: Transform-family-internal gaps — composite-input time_shift / consecutive_periods, time_shift partition kinds

## Why

DEV-1838 emptied the *coexistence* guard list — every dimension-family × measure-family
pair composes — but left the fail-closed gaps *inside* individual transform families
(`DEV-1450 stage 7b.11` markers in `slayer/sql/generator.py`): `time_shift` and
`consecutive_periods` reject composite (arithmetic / scalar-call) inputs, so natural
shapes like `change_pct(revenue:sum / *:count)` (MoM ratio growth) and
`consecutive_periods(change(revenue:sum) > 0)` (growth-streak length) error out.
Which error fires even differs by render path. This change lifts the composite-input
gaps, proves the residual guards unreachable or re-justifies them, and unifies the
fail-closed errors.

## What Changes

- `time_shift` accepts composite inputs whose slottable leaves are all aggregates:
  the shifted CTE re-aggregates each aggregate leaf and recomposes the
  arithmetic/scalar-call structure on top, projected as one column (single
  `shifted_`/`sjoin_` CTE pair, unchanged join-back). `change`/`change_pct` over
  composites work by desugaring onto this.
- `consecutive_periods` accepts any Mode-B value-key input tree (arithmetic of any
  op, scalar calls, BETWEEN/IN, boolean connectives, nested transforms), rendered
  through the one alias-context path with a boolean-vs-value wrap chosen by a typed
  top-node contract.
- One hoisted validation gate: every render path (plain, combined-attaches, kernel
  body) raises the same user-facing `ValueError` for still-unsupported shapes,
  naming the shape and the multi-stage remedy.
- Still fail-closed (uniform errors): nested transforms inside `time_shift` input,
  pure-row / mixed composites for `time_shift`, cross-model aggregate leaves inside
  `time_shift` composites, boolean-shaped nodes in numeric contexts and
  string-family scalar calls as `consecutive_periods` predicates.
- Planner fix: `_iter_slot_deps` recurses into `BetweenKey`/`InKey` wherever they
  can nest, so their column leaves materialise.
- Guard hygiene: the unreachable `time_shift` partition-kind arm and dead explicit
  `partition_keys` loop are deleted (RuntimeError invariant remains); the dead
  `deferred`-op walk and its two unreachable raises are deleted; the window-dispatch
  fallthrough is re-worded as a total-dispatch backstop. No `stage 7b.11`
  `NotImplementedError` remains in `generator.py`.

## Capabilities

### New Capabilities
- `queries/transforms`: window/self-join transform composition rules — which input
  shapes `time_shift` and `consecutive_periods` accept, how composite inputs render
  (shifted-CTE re-aggregation per aggregate leaf; alias-context predicate
  rendering), the boolean-vs-value predicate contract, and the uniform fail-closed
  errors for the remaining unsupported shapes.

### Modified Capabilities

(none — existing spec'd capabilities are untouched)

## Impact

- `slayer/sql/generator.py`: `_emit_time_shift_ctes_for_planned`,
  `_emit_consecutive_periods_ctes_for_planned`,
  `_validate_window_transform_ops_for_7b10` (renamed), window-dispatch fallthrough,
  partition-kind arm.
- `slayer/engine/planning.py`: `_iter_slot_deps` BetweenKey/InKey recursion.
- `slayer/engine/binding.py`: stale partition_by comment only.
- Tests: new `tests/test_dev1846_composite_transforms.py` +
  `tests/golden/dev1846_sql_baseline.json`; flips in
  `tests/test_dev1750_guard_lift.py`, `tests/golden/dev1750_sql_baseline.json`,
  `tests/test_dev1838_sweep.py` allowlist.
- Docs: `docs/concepts/formulas.md`, `.claude/skills/slayer-*.md`.
