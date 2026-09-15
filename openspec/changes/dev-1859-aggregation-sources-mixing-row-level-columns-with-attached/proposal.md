# Proposal: row-grain aggregation sources — mixing row-level columns with attached values

## Why

The last unfinished leg of axiom 6 (`architecture/semantics.arc42.md`, tagged
`[target: DEV-1859]`): an aggregation source mixing a raw row-level column with an
attached (partitioned-aggregate) value — `sum(quantity * avg(unit_price,
partition_by=product))` — fails closed at parse, though the grain-union doctrine
already defines it: the union with a row leaf is row grain, so the outer aggregate
consumes base rows with the attached value broadcast per row (explicit, intended
per-row weighting — the deliberate counterpart of the row-count-weighting bug
DEV-1847 guards against). Alongside, an execution-confirmed pre-existing miscompile:
a non-shift transform over a row-level leaf that refines the query grain
(`cumsum(weight)`, `rank(qty)`) silently inflates the base grain to one row per
(bucket, leaf-value) — no checker, no test, no spec covers it.

## What Changes

- **Leg A — the mixed source compiles at row grain.** The operand's grain-union
  determines the aggregation's input relation (DEV-1847's typing rule, second
  branch): a union containing a row leaf is row grain; the input relation is the
  row-filtered base population with each attached constituent row-attached
  (null-safe LEFT join on its complete grain — the computed-dimension mechanism);
  the outer aggregation evaluates inline at its consumer grain. Discovery gains an
  explicit mixed-source classifier so mixed roots never enter the pure
  producer-over-producer path (whose cell-over-cell semantics would be silently
  wrong here); the pure-re-aggregation checkers (`window=`, column-param) scope to
  pure roots. The full expression-source surface applies by grain type — outer
  `partition_by=`/`window=`, parametric/custom aggregations, `count`/
  `count_distinct`; `first`/`last` over the expression stays rejected by the
  existing expression rule; no new rejections (a mixed-specific restriction would
  be construction-inspecting). The parse gate narrows: only nested transforms in
  sources stay rejected.
- **Leg B — typed rejection for grain-refining row leaves under non-shift
  transforms.** A non-shift transform (every transform op except `time_shift`/
  `change`/`change_pct`; `first`/`last` are aggregation-dispatched) in
  measure/filter/order position rejects, in THE checker, any row-level leaf that
  refines the consumer grain; leaves that are projected grain keys stay legal
  (`rank(weight)` with `weight` a query dimension — verified sound today). Closes
  the miscompile; remedy: aggregate the leaf.
- **Leg C — attached parameter on a row-level source** (deferred here from
  DEV-1892, lands only after DEV-1892 merges):
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
  under `associate`. The parameter's producer row-attaches into the aggregation's
  input relation (association kernel: into level 1), DEV-1892's level-1 pick path
  carries it to level 2; the lift removes DEV-1892's "attached parameter requires
  an attached source" rejection wholesale, not per-kernel. NULL grain key follows
  the pinned null-safe-cell rule (a region-less customer gets the NULL-region
  cell's total).
- `architecture/semantics.arc42.md` axiom-6 tag flips `[target: DEV-1859]` →
  `[enforced: test:…]` (normative harness, separate per-change approval).
  Explicit non-changes, verified against HEAD: `guards.baseline` stays 1 (the one
  counted guard is DEV-1878's), no raise-ledger edit for the parse-gate raise
  (syntax.py is unscanned), nothing to delete in `slayer/mcp/server.py`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/semantics`: adds the row-grain branch of axiom 6's second-order rule —
  a source mixing row leaves with attached values aggregates over the row-filtered
  base population with attached constituents broadcast per row, no warning.
- `queries/partitioned-aggregates`: mixed sources join the legal surface with the
  full expression-source modifier set (outer `partition_by=`/`window=`,
  parametric/custom, `count`/`count_distinct`, all positions, NULL semantics,
  filter inheritance); the attached-parameter form (leg C) becomes legal on
  row-level sources.
- `aggregations/expression-aggregation`: the "Unsupported expression shapes"
  boundary narrows — only transforms nested in the aggregated expression remain
  rejected; the mixed row/attached scenario flips to accepted.
- `queries/transforms`: new requirement — non-shift transforms reject
  grain-refining row-level leaves with a typed checker error; projected grain
  keys and the shift family's bare-leaf regime are exempt.

## Impact

- `slayer/engine`: `syntax.py` parse gate narrows; `core/keys.py` mixed-source
  classifier beside `operand_aggregates`; `compile/stages.py` row-grain branch in
  `_plan_regroups` (row-phase attaches + inline outer, before re-aggregation
  discovery), pure-root scoping of `check_reaggregation_no_window`/
  `check_reaggregation_no_column_param`; `elaborate_env.py` leg-B checker rule
  (generalizing the `check_time_shift_input` walk, consumer-grain-aware).
- `slayer/sql/generator.py`: leg-C level-1 parameter pick reuse (after DEV-1892).
- Tests: `_dev1847_fixtures.py` extended additively (`quantity`, `unit_price`);
  new dev1859 executed/plan-structure/rejection suites; two existing rejection
  pins re-pointed with maintainer consent; new ledger rows for leg B; golden SQL
  per the divergence protocol.
- Docs: `docs/concepts/formulas.md` boundary sentence narrows + one-line
  capability example; `docs/concepts/queries.md` associate sentence at leg-C lift.
- Sequencing: legs A/B immediately; leg C gated on DEV-1892 merging (integrate
  forward by merging; re-verify its landed names then).
