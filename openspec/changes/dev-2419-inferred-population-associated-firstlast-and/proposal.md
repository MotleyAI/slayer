## Why

With no `population`, a query's rows are the row supply: the union of each metric's cells (semantics Axioms 12.3–12.4).
An associated windowed aggregate does not supply its own cells, so its rows come from the dimension anchor: a query by
`products.category` and order month measuring `sum(customers.credit, window='60d')` is supplied by `order_items` and
lacks the rows of the customers whose orders have no items or who have no orders, which `sum(customers.credit)`
returns. Separately, a windowed producer reads its buckets from the dataset hosting the query, so a cell only the home's
own rows or a sibling metric reach reads the empty value instead of its windowed value: beside `count(customers.*)` by
`orders.status`, `count(customers.*, window='60d')` reads 0 in the NULL-status cells that read 1 under
`population: customers` (Law 3; `docs/concepts/queries.md` › an entity with no related row counts in the NULL cell
however the query is rooted). Fixing the first exposes the second in a query with no siblings, so both land together.

## What Changes

- An associated windowed aggregate supplies the same cells as the same aggregate without `window=` over the same home
  and dimensions, spine queries keeping their in-frame witness rule (`queries/time-spine`).
- A windowed aggregate's value in a row depends only on that row's cell: it is evaluated over the stage's rows (after
  field filters, before measure-typed filters, order, limit and offset), never over the cells the hosting dataset
  reaches, and never changes with sibling metrics.
- The row supply decides association from the `to_many_handling` mode alone, read through its one reader; no
  aggregation kind is exempt by name (`first` / `last` follow in DEV-2436).
- The rule for aggregates outside the metrics is stated positively (owner ruling): the query's metrics define the row
  supply; an aggregate in a filter, an order entry or a computed dimension reads those rows and adds none. No code or
  executed value changes for it.
- `architecture/semantics.arc42.md` Axiom 12.4 states that rule (approved wording); `docs/concepts/queries.md` gains
  one bullet.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/population`: the row-supply requirement states the positive metrics-only rule, the windowed supply and the
  host-independence of windowed values, with scenarios over the issue's seed.

## Impact

- `slayer/engine/population/row_supply.py` — `_operand` drops the `window=` exclusion; the mode is read through
  `ModePolicy` in `classify_row_sources` and `_spine_witnesses`.
- `slayer/sql/generator.py` — the windowed producer's base cells (`_build_windowed_grain_base` /
  `_window_endpoints_base`, the `_base LEFT JOIN _src` range join) become the stage's rows; SQL goldens pinning the
  host-derived base may be re-blessed.
- `architecture/semantics.arc42.md` Axiom 12.4; `docs/concepts/queries.md`; `tests/_law_harness.py` gains windowed
  root-less operands.
- No API, storage or import-edge change.
