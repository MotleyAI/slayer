# queries/partitioned-aggregates — delta

## ADDED Requirements

### Requirement: Cross-model ranked partitioned aggregates
A `first`/`last` aggregation over another model's column with an explicit `partition_by=`
SHALL compile and execute like its local twin: ranked inside a producer rooted at the
aggregate's own model at the declared partition grain, attached back without changing
cardinality. The shape SHALL be legal in every position — measure, filter, order target,
and computed-dimension expression — with identical values in each (position parity).

#### Scenario: Cross-model last with partition_by executes
- **WHEN** a query rooted at `orders` selects `customers.spend:last(partition_by=region)`
  alongside a plain measure
- **THEN** each row carries its region's last customer-spend value, correct by
  hand-computed executed values on SQLite and DuckDB, with row count and the sibling
  measure's values unchanged — never the former not-yet-supported error

#### Scenario: Cross-model ranked partitioned aggregate in filter and order positions
- **WHEN** the same aggregate is referenced only in a filter, and separately only as an
  ORDER BY target
- **THEN** the filter masks by the same per-region value the measure form returns and the
  order sorts by it, both by executed values

### Requirement: Cross-model partitioned aggregates nest inside transforms
A transform whose input contains a cross-model `partition_by=` aggregate SHALL compile:
the inner aggregate is computed in its own producer exactly as when consumed directly, and
the transform consumes the attached value like any local partitioned input. This includes
ranked inners (`first`/`last`) and holds in measure, filter, and order positions.

#### Scenario: Transform over a cross-model partitioned sum executes
- **WHEN** a query selects `cumsum(customers.spend:sum(partition_by=region))` over a month
  time dimension with `region` among the query dimensions
- **THEN** the cumulative series accumulates the per-region cross-model totals, correct by
  hand-computed executed values on SQLite and DuckDB — never the former
  not-yet-supported error

#### Scenario: Transform over a cross-model ranked partitioned aggregate executes
- **WHEN** a query selects `change(customers.spend:last(partition_by=region))` over a month
  time dimension with `region` among the query dimensions
- **THEN** each row carries the period-over-period difference of its region's last value,
  correct by executed values

#### Scenario: Nested producer plan shape is pinned
- **WHEN** two consumers (for example a measure and a filter) share one cross-model
  partitioned inner aggregate under transforms
- **THEN** the plan contains exactly one producer for that aggregate, attached in the
  combined phase on its complete partition grain, and the emitted SQL contains one producer
  relation for it — no duplicate producers and no incomplete attach key
