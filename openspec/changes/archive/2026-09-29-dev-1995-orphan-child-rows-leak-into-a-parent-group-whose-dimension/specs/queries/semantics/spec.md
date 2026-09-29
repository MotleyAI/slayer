## ADDED Requirements

### Requirement: Aggregates are virtual models
An aggregate SHALL behave as a virtual model keyed by its grain: its rows are computed
from its source by the same joins as any query — a to-one hop that finds no related row
null-extends, never drops the row — and by the query's row filters, whatever consumes
it. In an expression its value SHALL be read as a field of that model, joined
one-to-one on the grain, NULL being a grain value like any other: a NULL grain cell
receives the virtual model's NULL row, and a virtual-model row with no matching result
cell contributes nothing. Consequently a source row whose path to a grain dimension is
broken (an orphan or a dangling reference) counts in the NULL cell of that dimension,
exactly as it does when the same aggregate is materialised on its own; and the value
never depends on which dataset roots the query.

#### Scenario: Orphan child rows count in the NULL grain cell
- **WHEN** a query rooted at `customers` selects `sum(orders.amount)` and
  `count(orders.id)` by `regions.name`, one customer has no region, and one order has no
  customer
- **THEN** the NULL-region cell holds the region-less customer's orders plus the
  customerless order (47 / 2 on the reference dataset), identically under `broadcast`,
  `associate` and `error` and with the population inferred, by executed values on
  SQLite and DuckDB

#### Scenario: Attached value equals the materialised aggregate
- **WHEN** the same aggregate is saved as a query-backed model grouped by the same grain
- **THEN** every result cell of the attached aggregate, the NULL cell included, equals
  that model's row for the cell's grain value, for the query rooted at the population
  and at the aggregate's source alike, by executed values

#### Scenario: A dangling or partial reference counts like an orphan
- **WHEN** a child row's foreign key names no existing parent, or a composite foreign
  key is partially NULL, and the aggregate is grouped by a parent-level dimension
- **THEN** the row counts in the NULL cell of that dimension exactly as in the
  materialised aggregate, by executed values

#### Scenario: Multi-hop grain through a broken hop
- **WHEN** a query rooted at `regions` selects `sum(customers.orders.amount)` by `name`,
  and one region's name is NULL
- **THEN** the NULL-name cell equals the materialised aggregate's NULL row — that
  region's orders together with every order whose path to a region is broken — by
  executed values

#### Scenario: A row filter narrows the virtual model's rows
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum` by
  `status` with the row filter `channel = 'app'`, and one customer has no orders
- **THEN** the orderless customer fails the filter on its null-extended row and counts
  in no cell, and every cell equals the hand-computed aggregate over the customers
  associated with app orders, by executed values
