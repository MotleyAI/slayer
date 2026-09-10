# Proposal: aggregate-over-attached — producer-over-producer re-aggregation

## Why

The one composition the regroup primitive cannot express is re-aggregating an attached
value at another grain — `avg(sum(amount, partition_by=[city, region]))` by region
("average city-total per region"). It fails closed today because feeding the broadcast
column to the outer aggregate would weight each city total by its row count — silently
wrong numbers. This change makes axiom 6 of `architecture/semantics.arc42.md` ("an
aggregate is a dataset: it can be filtered, queried, and aggregated again") true for
second-order aggregation, on the substrate the prior waves landed: interned producer
nodes and the CTE-hoist (DEV-1838), bidirectional join traversal (DEV-1853), and the
`to_many_handling` mode axis with the two-level association kernel (DEV-1841).

## What Changes

- An aggregation whose source resolves entirely to attached values (partitioned
  aggregates, directly or as a composite of them) becomes a **re-aggregation**: its
  input relation is the population at the operand's union grain with each inner
  producer null-safely attached, not the query rows.
- Outer-grain resolution is uniform with DEV-1841: keys attributable to the inner
  dataset (in its grain, or determined from an entity-key grain field via provable
  to-one chains) partition exactly; unattributable keys resolve per `to_many_handling`
  (broadcast + warning / associate / error).
- A degenerate re-aggregation (operand grain equals outer grain) executes as identity
  and warns.
- `partition_by=` naming an attach-carrying computed dimension compiles (row-attach
  inside the producer); nested producers lift to arbitrary depth.
- The three DEV-1847 fail-closed sites are removed; `guards.baseline` drops 8 → 5.
  The parse gate narrows: aggregates become legal inside an aggregation source;
  transforms stay rejected.
- New typed rejections (never new `NotImplementedError` deferrals): row-mixing operands
  (DEV-1859's shape), transforms in aggregation sources, outer measure-local `filter=`,
  outer `window=` / ranked-as-aggregation over an aggregate-dataset.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/semantics`: adds the second-order-aggregation requirement under axiom 6 —
  re-aggregation input relation, union-grain carrier population, attributability with
  determination chains, mode-axis resolution, degenerate identity warning.
- `queries/partitioned-aggregates`: re-aggregation mechanics (operand typing, edge
  semantics per aggregation family, shape B, nesting depth, interning, filters);
  the combined-consumer partition-key requirement narrows (a re-aggregated inner
  operand is a producer-internal grain, exempt like the dimension role); the
  nested-producer requirements lift the single-level and strict-subset limits.
- `aggregations/expression-aggregation`: the "Unsupported expression shapes" boundary
  narrows — nested aggregation is rejected only when the source mixes row-level
  references with attached values or nests a transform.

## Impact

- `slayer/engine`: `syntax.py` parse gate, `stage_planner.py` regroup discovery /
  nested-producer validation / re-aggregation planning, warnings plumbing.
- `slayer/sql`: outer producer kernel sharing DEV-1841's two-level shape, node
  assembly, CTE-hoist at depth.
- `architecture/`: `semantics.arc42.md` axiom-6 tag flips to enforced;
  `index.yaml` `guards.baseline` 8 → 5; `tests/_law_harness.py` loses the three
  DEV-1847 deferral sites.
- Docs and skills: `docs/concepts/formulas.md` / `queries.md`, aggregation examples,
  `.claude/skills/slayer-query.md`.
- Follow-ups unblocked: DEV-1859 (row-mixing operands), DEV-1868 (closure hunt).
