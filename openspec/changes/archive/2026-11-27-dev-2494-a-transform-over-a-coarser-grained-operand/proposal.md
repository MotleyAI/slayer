## Why

A transform whose operand is coarser than the query returns the right value as a metric but a different one inside an aggregation: `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` by region and city reads North 3 where the metric `rank(...)` reads 2. Inside a re-aggregation the transform is evaluated over the re-aggregation's operand rows, which are finer than the transform's own cells whenever an associated outer dimension, a finer sibling constituent or a parameter widens them (semantics Axioms 11.4, 11.6; Law 4).

## What Changes

- A transform inside an aggregation source or an aggregation parameter evaluates over its operand's domain, at any depth, and reads the value it has in metric position. Rank-family values that changed with the query's finer dimensions or a finer sibling constituent now match the metric-position value.
- Evaluating a transform over a plan's own rows becomes an explicit declaration by the plan that knows its rows are that transform's domain; every other transform takes its own domain producer through the one routing rule metric position already uses.
- A plan-time check refuses, as an internal error, a transform evaluated over rows whose grain differs from its operand grain.
- The transforms domain requirement lists every position explicitly, aggregation source and parameter included.
- One sentence in `docs/concepts/expressions.md` that the rule holds inside an aggregation.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/transforms`: "Transforms evaluate over their operand's domain" names aggregation source and aggregation parameter among its positions, with scenarios for both.

## Impact

- `slayer/engine/compile/stages.py` (producer context, `_route_producer`, the re-aggregation carrier, the shifted and transform producers, plan-time check), `slayer/engine/compile/discovery.py` (`TransformRouting`, `routes()`).
- `architecture/semantics.arc42.md`: an enforcement tag on Axiom 11 and Law 4 (user-approved).
- `docs/concepts/expressions.md`.
- Tests: new `tests/test_transform_domain_in_aggregation.py`. No golden SQL change expected.
- Out of scope: a re-aggregation parameter combining several aggregates (DEV-2505).
