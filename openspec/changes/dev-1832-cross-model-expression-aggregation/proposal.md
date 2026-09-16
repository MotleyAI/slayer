## Why

Expression aggregation (DEV-1826) still refuses three well-typed shapes with "not
supported" errors — a closure violation under axiom 9: a joined-model column inside
the expression (`sum(amount - customers.discount)`), an operand carrying
`Column.filter`, and a transform nested in the source (`sum(cumsum(x) - 1)`). Each
refusal exists because the code answers "where does this aggregate's source live"
from one attribute (`source.path`) that an expression source does not have, and
because `Column.filter` is re-implemented per site instead of being the derived
definition it was always meant to be. Now that DEV-1847/1859/1868/1871/1900 have
landed the carrier, the row attach, the closure axiom's ratchet, the typed
elaborator, and dependency closures, the shapes compose from existing primitives.

## What Changes

- **Cross-model expression sources.** Every aggregation source has an *anchor* (the
  longest common prefix of its leaves' join paths) and a *home dataset*: the deepest
  join path from the query root that determines every input over provably to-one
  hops (Axiom 2), resolved once in the elaborator and consumed by the compiler. The
  aggregation runs over the home's rows; spelling never moves the home
  (`sum(customers.spend)` ≡ `sum(customers.spend + 0)`). No home → the existing
  input-safety error.
- **`Column.filter` is pure sugar** for `CASE WHEN (<filter>) THEN (<value>) END` in
  every position. **BREAKING** for three previously divergent behaviours, each a bug
  by that definition: a parametric aggregation's parameters are no longer masked by
  the source column's filter; a filtered column used as a parameter now *is* masked;
  a filtered column in dimension / row-filter / order / `partition_by=` position now
  reads the masked value (non-matching rows fall into a NULL group). Single-column
  aggregate SQL is unchanged.
- **Transform constituents.** A transform nested in an aggregation source is an
  attached constituent typed at the union of its inner aggregates' explicit grains
  (the transform-in-dimension rule; a windowed inner adds the active time bucket);
  ungrained inners type it at the query grain (identity + the degenerate warning); a
  row leaf under the transform that is not a projected grain key is rejected by the
  DEV-1859 non-shift rule, extended to source position; a time-ordered constituent
  without its axis fails with the existing time-axis error, reworded position-neutral.
- Every synthesized producer sub-plan is elaborated through the one elaboration pass
  (no compiler-local typing); the guard ratchet's stale allowlist entry is removed;
  `SqlExprKey` is retired if producer-less.

## Capabilities

### New Capabilities

- `models/column-filters`: `Column.filter` as a conditional derived definition — one
  meaning in every position, dependencies and cycle rules included.

### Modified Capabilities

- `aggregations/expression-aggregation`: the same-model requirement is replaced by
  row-level expressions with dotted leaves homed per Axiom 2; the unsupported-shapes
  requirement is replaced by expression-source typing (the three boundaries lifted,
  the surviving rules kept); the deterministic-key requirement gains the dotted-leaf
  spelling.
- `queries/semantics`: ADDED the home-dataset requirement for row-level sources;
  MODIFIED *Second-order aggregation over attached values* and *Row-grain aggregation
  sources* to admit explicitly grained transforms as constituents.
- `queries/partitioned-aggregates`: MODIFIED *Re-aggregation consumes attached
  operands as datasets* (transform constituents, their grain, the axis rule) and
  *Mixed sources carry the full expression-source surface* (the stays-rejected
  sentence goes; joined-model row leaves and transform constituents admitted).
- `queries/transforms`: MODIFIED *Non-shift transforms reject grain-refining
  row-level leaves* to cover a transform used as an aggregation-source constituent.
- `queries/cross-model-aggregates`: MODIFIED *Target-rooted computation with metric
  independence* — the root is the home dataset, not "the model its source names".

## Impact

- `slayer/core/keys.py` (source anchor / leaf paths, constituent classifiers),
  `slayer/core/models.py` (`Column` expansion predicate), `slayer/ir/terms.py`
  (`Aggregate.home_path`), `slayer/engine/elaborate.py` + a new home-resolution
  module (the S2 rule, moved out of `compile/stages.py`), `slayer/engine/elaborate_env.py`
  (checker walks sources; axis message), `slayer/engine/binding.py`,
  `slayer/engine/syntax.py`, `slayer/engine/join_safety.py`,
  `slayer/engine/reference_closure.py`, `slayer/engine/column_dependency.py`,
  `slayer/engine/compile/{stages,staging}.py`, `slayer/sql/column_expansion.py`,
  `slayer/sql/scope.py`, `slayer/sql/generator.py`, `slayer/sql/naming.py`,
  `slayer/sql/render/value_expr.py`, `slayer/sql/sql_expr.py`.
- Tests: new `_dev1832` fixtures and suites, `tests/golden/dev1832_sql_baseline.json`,
  `tests/_dev1871_raise_ledger.py` (axis row), `tests/test_law_guard_ratchet.py`
  (stale entry), re-pointed pins in `tests/test_dev1847_gate.py` and
  `tests/test_dev1859_plan_structure.py`, allowed-delta re-blessing where filtered
  parametric goldens move.
- Docs: `docs/concepts/formulas.md`, `docs/concepts/models.md`,
  `docs/concepts/queries.md` (one sentence each). arc42: proposed `enforced:` tags on
  semantics axioms 2 and 6 (presented for approval at implement time).
