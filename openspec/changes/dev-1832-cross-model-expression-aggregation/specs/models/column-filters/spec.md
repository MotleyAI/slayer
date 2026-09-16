## Purpose

Defines `Column.filter` as sugar for a conditional derived definition, so a filtered
column behaves as one column expression in every position, with one dependency rule.

## ADDED Requirements

### Requirement: Column.filter is a conditional derived definition
A column carrying `filter` SHALL behave, in every position, exactly as a derived
column whose definition is `CASE WHEN (<filter>) THEN (<value>) END`, where `<value>`
is the column's `sql` when set, else its physical column: as an aggregation source
(single-column or as a leaf of an expression), as an aggregation parameter, as a
dimension, in a row filter, as an order key, as a `partition_by=` key, as a
computed-dimension leaf, and when referenced from another derived definition. The
filter's references SHALL be dependencies of the column exactly as its value's
references are — for join classification, dependency closure, cycle detection and
save-time path validation — while the wrapped value's reference to the column's own
physical column SHALL NOT form a dependency cycle. Variable substitution SHALL apply
to `filter` and `sql` alike. No other behaviour SHALL attach to `filter`: an
aggregation's parameters are masked only by their own columns' filters, never by the
source column's, and no position reads the unmasked value.

#### Scenario: Single-column aggregate SQL unchanged
- **WHEN** `q_amount` is `amount` with filter `product = 'Q'` and a query selects `q_amount:sum`
- **THEN** the generated SQL is `SUM(CASE WHEN product = 'Q' THEN amount END)` as
  before and executed values are unchanged

#### Scenario: Parameters are not masked by the source's filter
- **WHEN** a query selects `weighted_avg(q_amount, weight=quantity)`
- **THEN** the value is masked and the weight is not: the result equals
  `SUM(CASE WHEN product = 'Q' THEN amount END * quantity) / SUM(quantity)` by
  hand-computed executed values on SQLite and DuckDB — never the former
  filter-everything form

#### Scenario: A filtered column used as a parameter is masked
- **WHEN** a query selects `weighted_avg(amount, weight=q_amount)`
- **THEN** the weight is NULL on non-Q rows and the result equals the manual
  encoding with `CASE WHEN product = 'Q' THEN amount END` as the weight, by executed
  values — never the former silently unfiltered weight

#### Scenario: Filtered column as a dimension groups non-matching rows under NULL
- **WHEN** a query groups by `q_amount`
- **THEN** rows whose `product` is not `'Q'` form the NULL group and the other rows
  group by their amount, by executed values

#### Scenario: Filtered column in row-filter, order and partition-key positions
- **WHEN** a query filters on `q_amount > 10`, orders by `q_amount`, or selects
  `amount:sum(partition_by=q_amount)`
- **THEN** each position reads the masked value: the filter keeps only Q rows above
  ten, the order sorts NULLs per the dialect's default with matching rows by amount,
  and the partition forms one cell per masked value including the NULL cell, by
  executed values

#### Scenario: Filtered leaf inside an aggregated expression
- **WHEN** a query selects `sum(q_amount - 1)` and `count(q_amount - 1)`
- **THEN** the first sums `amount - 1` over Q rows and the second counts the Q rows,
  by executed values

#### Scenario: Filtered leaf on a joined model contributes its filter's crossings
- **WHEN** a query rooted at `orders` selects `sum(amount - customers.north_spend)`
  where `north_spend` is `spend` filtered by `regions.name = 'North'`
- **THEN** the leaf's dependencies include `customers.regions`, the join is registered
  once inside the aggregation's home relation, and the value is correct by executed
  values; the same leaf whose filter crosses a fanning hop fails closed with the
  existing input-safety error

#### Scenario: Derived column over a filtered column expands the wrapped value
- **WHEN** a column `q_double` is defined with `sql: "q_amount * 2"`
- **THEN** it expands to `(CASE WHEN product = 'Q' THEN amount END) * 2` in every
  position, by executed values

#### Scenario: Filtered columns pass cycle validation
- **WHEN** a model is saved with a filtered physical column (`amount` with a filter)
  and a filtered derived column (`sql: "amount"`, filter on `product`)
- **THEN** the save succeeds — the wrapped value's self-reference is not a cycle —
  while a filter that names another derived column creates a dependency edge, and a
  filter that names its own column is rejected as a cycle

#### Scenario: Variables substitute into both fields
- **WHEN** `sql` and `filter` each contain a `{variable}` and the query supplies it
- **THEN** both are substituted before the definition is composed, by generated SQL

#### Scenario: Broken filter path fails at save time
- **WHEN** a column's filter names a dotted path that does not walk from its model
- **THEN** the save fails with the existing path-validation error naming the path,
  exactly as for a broken path in `sql`

#### Scenario: Ranked aggregation over a filtered column picks the masked value
- **WHEN** a query selects `q_amount:last`
- **THEN** the picked value is `CASE WHEN product = 'Q' THEN amount END` at the
  latest ranking timestamp, unchanged from before
