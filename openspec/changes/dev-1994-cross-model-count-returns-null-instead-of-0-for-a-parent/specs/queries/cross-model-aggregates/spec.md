## ADDED Requirements

### Requirement: Empty cells take the aggregation's empty value
A result cell whose attached aggregate has no home rows SHALL take the aggregation's
**empty value**: 0 for the built-in count family (`count`, `count_distinct`,
`count_distinct_approx`, including `count(x.*)`), NULL for every other aggregation. A
model-level aggregation declaring its own `formula` — including one named after a
built-in count — and every custom aggregation SHALL have the empty value NULL; a
formula-less declaration of a built-in name keeps the built-in's empty value; the
declaration consulted is the one on the aggregation's owning model. The empty value
SHALL hold in every position (measure, composite, measure filter, order key, dimension,
aggregation source, transform operand), in every `to_many_handling` mode, and for
windowed aggregates. A transform cell with no value (e.g. a `time_shift` whose prior
bucket is absent) SHALL stay NULL, and no population cell SHALL be fabricated.

#### Scenario: Single-hop count of a childless parent is 0
- **WHEN** a query rooted at `customers` groups by `name` and selects `count(orders.id)`,
  `count(orders.*)`, `count_distinct(orders.id)` and `count_distinct_approx(orders.id)`,
  and one customer has no orders
- **THEN** by executed values on SQLite and DuckDB that customer's cell holds 0 for every
  one of the four measures

#### Scenario: Non-count aggregations stay NULL
- **WHEN** the same query selects `sum(orders.amount)` and `avg(orders.amount)`
- **THEN** the childless customer's cell holds NULL for both

#### Scenario: Inferred population
- **WHEN** a query without `source_model` has `dimensions=[customers.name]` and selects
  `count(orders.id)`
- **THEN** the population is `customers` and the childless customer's cell holds 0

#### Scenario: Multi-hop count
- **WHEN** a query rooted at `regions` groups by `name` and selects
  `count(customers.orders.id)` and `count(customers.id)`, and one region has no customers
- **THEN** that region's cell holds 0 for both, and a region whose customers have no
  orders holds 0 for the first

#### Scenario: Associate mode
- **WHEN** the single-hop count query runs with `to_many_handling: "associate"`
- **THEN** the childless customer's cell holds 0

#### Scenario: Windowed cross-model count
- **WHEN** a query rooted at `customers` with a time dimension selects a windowed
  `count(orders.id, window='30d')`, and a customer has no orders in any interval
- **THEN** that customer's cells hold 0, never NULL

#### Scenario: Empty value in every position
- **WHEN** a query rooted at `customers` groups by `name` and selects the composite
  `count(orders.id) * 2`, filters on `count(orders.id) = 0`, orders by
  `count(orders.id)`, and adds the dimension `count(orders.id, partition_by=[id]) = 0`
- **THEN** the childless customer's composite is 0, the filter keeps exactly the
  childless customers, the ordering places them as 0, and they fall in the `true`
  dimension bucket

#### Scenario: Coarser-grained count broadcasts the empty value
- **WHEN** a query rooted at `customers` groups by `region` and `name` and selects
  `count(orders.id, partition_by=[region])`, and one region's customers have no orders
- **THEN** every cell of that region holds 0

#### Scenario: Re-aggregation counts the zeros
- **WHEN** a query rooted at `customers` selects
  `avg(count(orders.id, partition_by=[name]))`
- **THEN** the average includes each childless customer as 0 (total orders divided by
  the number of customers), by executed values

#### Scenario: Transform over a count consumes 0, missing transform cell stays NULL
- **WHEN** a query rooted at `customers` with a time dimension selects
  `cumsum(count(orders.id))`, `change(count(orders.id))` and
  `time_shift(count(orders.id), -1)`
- **THEN** a cell whose count operand has no home rows contributes 0 to the transform,
  and a cell whose shifted prior bucket is absent from the result holds NULL

#### Scenario: Custom formula override stays NULL, formula-less declaration keeps 0
- **WHEN** the owning model declares an aggregation named `count` with its own `formula`
  and a query selects `count(orders.id)` from `customers`
- **THEN** the childless customer's cell holds NULL; with a formula-less `count`
  declaration it holds 0
