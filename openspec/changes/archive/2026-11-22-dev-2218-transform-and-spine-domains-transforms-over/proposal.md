## Why

Under the 2.0 row rule, adding a measure can add rows to a root-less query, and every transform today runs as a window over the final result rows. So a sibling measure shifts `lag`, `ntile`, `percent_rank`, `rank` and `first`/`last` values in rows that already existed, breaking measure monotonicity and compositionality (semantics Laws 3 and 7). `rank` over a partitioned aggregate ranks query-grain rows instead of the partition cells, and spine queries keep a grid of rows that hold no facts.

## What Changes

- **BREAKING** Every transform, in every position (dimension, measure, filter, order) and every mode, evaluates over its operand's **domain** — the operand's own cells at its grain — and its result attaches on its result grain. A query cell outside the domain reads NULL for that transform. With a named population, a cell the operand lacks (an empty calendar month, a region without orders) no longer takes a transform value: a hand-made calendar population is no longer a dense series.
- **BREAKING** Over the spine, the domain is the operand's non-time tuples in visible buckets × every in-frame bucket, so the dense-series values stay; without the spine, `lag` / `lead` step over the operand's present cells.
- **BREAKING** The rank family ranks its operand's cells: `rank(sum(amount, partition_by=region))` by `[region, city]` ranks the regions and broadcasts to their cities. A running total over a partitioned inner accumulates within the operand's non-time dimensions.
- The measure-position carve-out for transform-bearing computed dimensions is removed: a measure equal to a transform dimension reads the dimension's value.
- **BREAKING** A spine query omitting `population` takes its non-time tuples from its row supply: spine buckets × the union of each measure's non-time tuples in visible buckets. Spine bounds restrict every home and the dimension anchor like a field filter, so a group whose facts all lie outside the frame disappears, and an in-frame orphan lands in the NULL tuple. A named population keeps today's grid.
- **BREAKING** A dataset with no to-one route to a time axis reaches the spine by association across its nearest fanning axis: `customers.customer_count` by spine month counts the customers with an in-frame order that month. Several fanning axes at the same distance fail with the typed ambiguous-route error naming each route as a fact time column.
- The spine's lower bound stays required.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/time-spine`: spine rows follow the row supply of the non-spine dimensions; bounds restrict homes and the anchor like field filters; fanning-axis association and its tie error; transforms over the spine read the operand's domain.
- `queries/transforms`: transforms evaluate over their operand's domain in every position and mode.
- `queries/semantics`: compositionality and the row laws hold for transforms (exemption removed).
- `queries/partitioned-aggregates`: transforms over partitioned inners evaluate over the partition cells.
- `queries/computed-dimensions`: the dimension and measure forms of a transform read the same union-grain domain.
- `queries/positions`: a dimension value reads the dimension's grouped value in measure position for transform-bearing dimensions too.

## Impact

- Engine: `slayer/engine/compile` (transform domain producer, spine rows, fanning-axis association), `slayer/engine/population` (spine row supply), `slayer/engine/query_engine.py` (spine population), `slayer/engine/elaborate_env.py` (carve-out removal), `slayer/engine/cardinality/relationship_safety.py` (fanning-axis route and tie error).
- SQL: `slayer/sql/generator.py` (transform chains over the domain relation, spine series × row keys).
- IR: `slayer/ir/planned.py` (domain producer plan).
- Architecture: `architecture/semantics.arc42.md` Axioms 11 and 15, Laws 3, 4, 7 (approved wording).
- Docs: `docs/concepts/expressions.md`, `docs/concepts/time.md`, `docs/concepts/queries.md`, `docs/upgrading/2.0.md`.
- Response contract: spine queries report `row_sources` as their supplying homes.
- Goldens re-blessed: `lift/dual_role`, `dedup/dual_role`, `spine/per_group` and transform goldens whose shape changes.
