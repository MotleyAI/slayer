## Why

In a grouped query, a measure or order target that reads a query dimension's value outside an aggregate — `P + 1` with dimension `P = amount:sum(partition_by=region)`, `quantity * count(*)` by `quantity`, `iif(region == 'North', sum(amount), 0)` by `region` — fails with an internal error (`RenderContextMissingFacilityError`, or a `'__regroup__…' needs an aggregation` `ValueError`), while the same expression as a filter works. A measure reading a row-level column that is not a dimension (`amount + sum(amount)`) reaches SQL rendering instead of the checker's typed error. Both break Axiom 9 closure and Axiom 13 position parity.

The cause is structural: "this sub-expression is a dimension's value, available at query grain" is decided in three places with three answers — the checker types filters and order targets but never declared measures; discovery resolves a dimension sub-expression to its grouped value for order targets and measure-typed filters but not measures; and the base grouped SELECT renders the same composite through three contexts (HAVING with a scope, projection with none, row-phase non-dimension with an unconditional refusal).

## What Changes

- One concept, the **dimension value**: a sub-expression of a measure, order target or filter structurally equal to a query dimension's bound key is that dimension's grouped value, in every position and for every dimension kind (plain, joined, stage, computed, partitioned aggregate, re-aggregation, transform).
- The checker types every declared measure — and, in a grouped query, every order target — at query grain: a row-level reference outside aggregates and outside a dimension value fails with `PositionTypingError` at the consuming position, after the existing measure checks.
- An aggregate-free measure over dimension values (`quantity + 1` by `quantity`) becomes legal, evaluating per result cell.
- A measure equal to a computed dimension's whole aggregate (`C` with dimension `x = C`, `C` finer-grained) reads `x` instead of failing with `PartitionKeyError`, matching order and filter behaviour.
- The base grouped SELECT renders projected composites, hidden order composites and measure-typed HAVING masks through one render context that resolves dimension values to their GROUP BY expressions; the ad hoc HAVING "not in GROUP BY" `ValueError` and the row-branch "needs an aggregation" `ValueError` are removed.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/positions`: declared measures and grouped order targets type at query grain; dimension values read the grouped value in every position.
- `queries/computed-dimensions`: arithmetic over a computed dimension's own aggregate, and the whole aggregate, evaluate as the dimension's value in measure and order position.
- `queries/partitioned-aggregates`: a combined-position sub-expression equal to an entire bound dimension is not a combined consumer of its partition keys.

## Impact

- `slayer/engine/elaborate_env.py` — measure / grouped-order typing at query grain; `PositionClasses.combined_admits` for measures.
- `slayer/engine/compile/discovery.py` — no combined attach for a dimension value inside a measure.
- `slayer/sql/generator.py`, `slayer/sql/render/value_expr.py` — one grouped-SELECT render context; two untyped `ValueError`s removed.
- Golden SQL: plans for measures containing a dimension value drop a redundant combined attach; each re-blessed baseline is recorded in design.md.
- Docs: `docs/concepts/formulas.md`. Architecture: an enforced-test tag on Axiom 13 in `architecture/semantics.arc42.md`.
