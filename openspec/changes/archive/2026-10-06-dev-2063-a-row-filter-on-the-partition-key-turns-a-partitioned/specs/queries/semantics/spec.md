## MODIFIED Requirements

### Requirement: Grain guarantee
For a query with at least one measure, and for a measure-less query with distinct
dimension values enabled (the default), the result SHALL have exactly one row per
combination of dimension values present among the row-filtered population's rows.
Raw-row mode (`distinct_dimension_values=false`) is the documented exception and
returns one row per population row. A row-level filter that reaches the population root
only across a non-determining path SHALL NOT multiply the population's rows, whatever the
conjunct's boolean shape: raw-row mode returns each population row passing the restriction
exactly once. A value the query evaluates only for a filter, an order key or an attached
aggregate's join SHALL NOT change the result's grain: it never adds a row, splits a
dimension combination, or repeats one.

#### Scenario: One row per dimension combination
- **WHEN** an aggregating query groups by dimensions whose value combinations repeat
  across many population rows
- **THEN** the result contains each present combination exactly once

#### Scenario: Raw rows are never multiplied by a population filter's join
- **WHEN** a query rooted at `customers` selects `dimensions: ["tier"]` with
  `distinct_dimension_values: false` and `filters: ["orders.status = 'ok'"]`, and one
  customer has two `ok` orders
- **THEN** by executed values the result has one row per customer with at least one `ok`
  order (five rows on the reference dataset), never one row per matching order

#### Scenario: A filter on a computed dimension's partition key keeps the dimension's grain
- **WHEN** a query rooted at `orders` selects the computed dimension
  `case when sum(amount, partition_by=customer_id) > 50 then 'high' else 'other' end`
  and the measure `sum(amount)`, with a row filter that reads `customer_id`
  (`customer_id is not null`, or `customer_id + 0 > 0`), over customers whose order totals
  are 90, 70 and 50
- **THEN** by executed values the result has exactly one row per band (`high` 160,
  `other` 50), the filter removing rows before every aggregation, never one row per customer

#### Scenario: A filter on a joined partition key keeps the dimension's grain
- **WHEN** the computed dimension partitions by a joined column (`customers.tier` or
  `customers.regions.name`), a row filter reads that column, and one band covers rows of
  two distinct values of it
- **THEN** by executed values the band appears in exactly one row, never once per value of
  the partition key

#### Scenario: An order key beside a ranked measure keeps the query's grain
- **WHEN** a grouped query declares a `first` or `last` measure (e.g.
  `amount:last(ordered_at)`) and orders by a column that is not a dimension, local
  (`city`) or joined (`customers.regions.name`)
- **THEN** by executed values the result has exactly one row per dimension combination
  (one row when the query has no dimensions), never one row per value of the order key
