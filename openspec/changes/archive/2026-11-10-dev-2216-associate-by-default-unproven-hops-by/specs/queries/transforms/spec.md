## MODIFIED Requirements

### Requirement: Composites over transforms

A composite (arithmetic or scalar-call expression) whose operands include a transform
SHALL be legal in measure, filter and ORDER BY positions whatever its other operands are:
a local aggregate, a cross-model aggregate, another transform, or a literal, in any
nesting. Every operand the composite needs SHALL be materialised for it, including a local
aggregate that appears nowhere else in the query, and the composite SHALL evaluate to the
same value whether or not any operand is also selected on its own. The transform's own
regime (re-aggregation or series) SHALL be unchanged by the composite around it. No such
composite SHALL fail with an internal render error. The scenarios below run under
`to_many_handling: "broadcast"`, where a cross-model inner over an order month repeats
its total.

#### Scenario: Transform over a cross-model inner plus a hidden local aggregate

- **WHEN** a query rooted at `orders` with a month time dimension selects
  `change(sum(customers.spend)) + sum(amount)` and nothing else
- **THEN** each row carries the month-over-month change of the cross-model aggregate
  plus that month's local sum, by hand-computed executed values on SQLite and DuckDB,
  with NULL in the first month

#### Scenario: Hidden local operand of another aggregation kind

- **WHEN** the composite's hidden local operand is `count(*)` or `max(amount)` (for example
  `change(sum(customers.spend)) + count(*)`)
- **THEN** the query executes with the operand materialised, by executed values

#### Scenario: Transform over a crossing-fragment inner plus a hidden local aggregate

- **WHEN** a query selects `change(wscaled_sum(amount)) + sum(amount)`, where `wscaled_sum`
  is an aggregation whose default parameter crosses a join
- **THEN** each row carries the weighted-scaled delta plus the local sum, by executed
  values on SQLite and DuckDB

#### Scenario: Other transform families compose the same way

- **WHEN** the transform is `time_shift(sum(customers.spend), -1)` or
  `cumsum(sum(customers.spend))` combined with a hidden local aggregate
- **THEN** the composite executes with the transform's own semantics unchanged, by
  executed values

#### Scenario: Conditional over a transform and mixed aggregates

- **WHEN** a query selects
  `iif(change(sum(customers.spend)) > 0, sum(customers.spend), sum(amount))`
- **THEN** each row carries the cross-model total where the change is positive and the
  local sum otherwise, by executed values

#### Scenario: Selecting an operand on its own changes nothing

- **WHEN** the same query additionally selects `sum(amount)` as its own measure
- **THEN** the composite's values, the row count and every other column are identical to
  the query without it

#### Scenario: Composite in filter and order positions

- **WHEN** `change(sum(customers.spend)) + sum(amount)` appears only in a filter or only as
  an ORDER BY key, with a dimension present
- **THEN** rows are masked or sorted by the composite's value exactly as when it is
  selected, by executed values

#### Scenario: Inner varying along the time axis

- **WHEN** the cross-model inner is attributable to the query's time axis (for example
  `change(sum(orders.amount))` by customer signup month from `customers`, or
  `change(sum(customers.spend))` by `customers.signup_at` from `orders`)
- **THEN** the delta and `change_pct` percentages are correct per period by hand-computed
  executed values on SQLite and DuckDB, and adding the measure changes no row and no
  other column

#### Scenario: The issue's named shapes execute directly

- **WHEN** a query selects `change(wscaled_sum(amount))`, `change_pct(wscaled_sum(amount))`,
  `change(sum(customers.spend))` or `change_pct(sum(customers.spend))` alone
- **THEN** each executes with hand-computed values on SQLite and DuckDB, the broadcast
  cross-model inner yielding a zero delta and zero percentage after the first period
