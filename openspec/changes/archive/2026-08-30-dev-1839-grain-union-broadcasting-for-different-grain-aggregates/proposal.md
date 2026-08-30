# Grain-union broadcasting for different-grain aggregates

## Why

When an arithmetic expression combines aggregates declared at different partition grains, the correct semantics is to union the grains and broadcast each aggregate to the union — never to error, never to misgrain. This already holds for no-transform computed dimensions, transform-as-measure, and filter routing (one producer per grain). Two shapes do not conform today: a different-grain arithmetic nested inside a transform used as a computed dimension — e.g. `rank(amount:sum(partition_by=region) - amount:sum(partition_by=city))` — which compiles inside one producer at one grain and therefore fails closed (DEV-1824's interim guard); and bare composite arithmetic over two-plus partitioned aggregates as a measure, which fails with a leaked-placeholder error. This change codifies the principle as normative and lifts both gaps, applying the regroup primitive recursively for the transform case. It also fails closed a discovered row-duplication defect (a time-ordered transform in a dimension whose grain lacks its time axis).

## What Changes

- **Codify the principle** (docs + spec): within a dimension's expression tree, every node's output grain is the union of its children's grains (aggregate → its partition set; transform → union of its input's grains; arithmetic/scalar/CASE → union of operands); each combination point broadcasts. Consuming at a finer grain broadcasts further; a coarser consumer stays illegal. `partition_by=[]` is the empty grain.
- **Transform-root grain becomes the union** of all grained inner aggregates (any nesting depth), replacing "first inner aggregate's grain"; the mixed-grain `NotImplementedError` in `_guard_computed_dimension` is removed.
- **Recursive producer synthesis**: the union-grain producer computes exactly-union aggregates inline; strict-subset bare aggregates become nested combined attaches; strict-subset *transform roots* (e.g. an inner `cumsum` at its own grain) become nested attaches whose producer is the already-shipped transform-at-producer-grain shape, applied recursively. Discovery inside a producer excludes roots at exactly the producer's own grain — the recursion terminator (strictly decreasing grains ⇒ bounded depth).
- **Planner controls**: a dedicated producer-planning control enables regroup discovery inside producers while host-rooted isolation stays disabled; admitted nested producer plans are structurally validated (local, combined-phase, strict-subset grains, no row attaches) — anything else fails closed.
- **Renderer**: split "CTE body, `WITH` forbidden" from "producer body whose internal `WITH` is hoisted"; nested combined attaches and step chains render under the existing CTE-hoist. The grain-coverage assert holds at every nesting level.
- **New deferrals fail closed citing DEV-1835**: a mixed-grain transform root any of whose inner aggregates carries `window=` or is `first`/`last` (the union would need the synthesized time-bucket grain — a stage-2 design question). Single-grain windowed/ranked transform-in-dimension is unaffected.
- **Temporal-axis containment guard** (fixes a live row-duplication defect): a time-ordered transform (`cumsum`, `lag`, `time_shift`, …) inside a dimension whose evaluation grain lacks its time-ordering key currently pulls the query's time bucket into the producer base and joins back on the coarser grain, duplicating result rows. It now fails closed with a message directing the author to include the time key in `partition_by`.
- **Composite measure arithmetic over partitioned aggregates**: bare arithmetic combining two or more partitioned aggregates as a measure (`a:sum(partition_by=region) - b:sum(partition_by=city)`, incl. the same-grain form) currently fails with a leaked `__regroup__` placeholder error; it lifts to broadcast evaluation at the query grain (the transform-wrapped form of the same arithmetic already executes correctly).
- **Locking coverage** for the already-conforming surfaces: plain + partitioned measure mix, transform-as-measure over mixed grains, mixed-grain filter routing.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

(Both capabilities entered the corpus when `dev-1824-partition-by-on-aggregations-deferred-shapes-window` and `dev-1837` were archived on 2026-08-30; the deltas below modify/extend that corpus.)

- `queries/computed-dimensions`: the "Transforms inside dimension expressions" requirement — mixed inner grains lift from fail-closed to union-grain evaluation with recursive broadcast; new fail-closed deferrals for mixed windowed/`first`/`last` grains (DEV-1835); new fail-closed temporal-axis containment rule (time-ordered transform whose evaluation grain lacks its time key — today a silent row-duplication defect).
- `queries/partitioned-aggregates`: ADDED requirement locking grain-union broadcasting in measure, filter, and transform-as-measure contexts. Transform-as-measure and filter routing are already conforming; bare composite arithmetic over two-plus partitioned aggregates (mixed- or same-grain) is broken today (leaked-placeholder error) and is implemented under this requirement.

## Impact

- `slayer/engine/regroup_planner.py`: union-grain root classification (transform roots at union of inner grains; recursive root discovery usable inside producers).
- `slayer/engine/stage_planner.py`: guard removal; producer-planning control; nested-plan structural validation; producer synthesis split (inline vs nested); temporal-axis containment guard.
- `slayer/sql/generator.py`: hoistable-producer render context (combined attaches legal when the internal `WITH` hoists); no change for genuinely non-hoistable CTE bodies. Phase classification of regroup-substituted composites so bare arithmetic over attached partitioned aggregates renders at the combined stage instead of hitting the row-phase "needs an aggregation" error.
- Tests: new executed-value + golden + guard suites; DEV-1837 compatibility matrix gains the mixed-grain dimension family; the DEV-1824 mixed-grain guard test flips to a positive lift assertion.
- Docs: `docs/architecture/composable-attach.md` (new principle section), `docs/concepts/formulas.md` / `queries.md` where mixed-grain semantics are user-visible.
- Linear: DEV-1835 gains the explicit lift line-item; DEV-1839's stale "blocked by stage 3" note is corrected.
