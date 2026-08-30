## Why

Every grain-changing feature (cross-model `_cm_`, windowed `_wm_`, ranked `_rk_`, partitioned/regroup) has its own plan class, planner, and renderer arm, so each pairwise feature combination needs bespoke assembly code — ~50 `NotImplementedError` guards and counting. DEV-1824 owns the deferred `partition_by` shapes; rather than lifting them as more one-off arms, this change establishes ONE composition mechanism — producer sub-plan + grain-keyed attach + placeholder substitution, rendered recursively — and lifts the whole local `partition_by` guard list on it (stage 1 of a three-stage migration; stages 2–3 are follow-up issues).

## What Changes

- **Architecture (internal):** recursive render contract with CTE-hoist (producers may contain `_wm_`/`_rk_`/transform-step CTEs; parent hoists them, names kept globally unique by threading the parent's `AliasAllocator` through nested renders); regroup discovery generalized from bare partitioned `AggregateKey`s to a recursive grain classification (atomic grain-bearing roots; composition over attached columns; `requires_nested_attach` fails closed); explicit filter placement decided pre-substitution; attach plans declare the producer's unique key and the planner validates join pairs cover it exactly (structural cardinality invariant).
- **Lifted query shapes (spec-level, all local-source):** `partition_by` combined with `window=`; `partition_by` on `first`/`last`; partitioned aggregates nested inside transforms (as measures); filters referencing partitioned aggregates; row + combined regroup coexistence (incl. the direct-aggregate ORDER BY form); producers that themselves need isolated CTEs; transforms, `first`/`last`, and `window=` inside computed-dimension expressions (measure⇔dimension symmetry: any grain-self-contained measure-legal expression is dimension-legal).
- **Unchanged / still errors:** bare aggregates without `partition_by` in dimensions; grain-circular dimension expressions; cross-model aggregate sources in dimension expressions and cross-model partitioned aggregates (stage 3); DEV-1504 G4/G5 for `window=` measures (stage 2); predicates with no common availability scope; cross-phase attach dedup (stage 2 — duplicate producers render meanwhile).
- **No BREAKING changes:** all currently-passing golden SQL baselines stay byte-identical (divergences individually approved and recorded); result keys / meta / warnings unchanged for existing shapes.

## Capabilities

### New Capabilities
- `queries/partitioned-aggregates`: `partition_by=` on aggregations as measures — composition with `window=`, `first`/`last`, transforms, filters, ORDER BY, coexistence with other isolated features, and the cardinality invariant of grain-keyed attaches.
- `queries/computed-dimensions`: dimension expressions — the measure⇔dimension symmetry axiom, grain self-containment, transforms/`first`/`last`/`window=` over partitioned aggregates inside dimensions, and the remaining error surface.

### Modified Capabilities
<!-- none: the spec corpus is empty; both capabilities are introduced here -->

## Impact

- **Code:** `slayer/engine/regroup_planner.py` (recursive grain classification, discovery), `slayer/engine/stage_planner.py` (guard lifts, producer synthesis incl. synthesized time dimension and explicit ranking key, filter placement, coexistence), `slayer/engine/planned.py` (attach plan: producer key, placement fields), `slayer/sql/generator.py` (recursive render + CTE-hoist, shared allocator threading, combined-WHERE placement consumption), `slayer/sql/naming.py` (allocator threading).
- **Tests:** new executed-value (SQLite + DuckDB), golden-SQL, guard-preservation (both directions), hoist-collision, multi-stage/StageSchema-boundary, and placeholder-leakage suites; existing goldens as the byte-identity net.
- **Docs:** new `docs/architecture/composable-attach.md` (architecture + 3-stage roadmap, linked in `zensical.toml`), updates to `docs/concepts/queries.md`, `docs/concepts/formulas.md`, aggregation examples, and the three `.claude/skills/slayer-*.md`; `DECISIONS.md` entry.
- **No** public-API surface additions: all lifted shapes use existing syntax.
