## MODIFIED Requirements

### Requirement: A refined run equals the hand-written merged final stage
Run-by-name SHALL accept a refinement carrying any of `dimensions`, `time_dimensions`, `metrics`,
`filter`, `order`, `limit`, `offset`, `main_time_dimension`, `whole_periods_only`,
`distinct_dimension_values` and `to_many_handling`, with the same input forms and validation as the
same fields of a query. The refinement SHALL merge into the saved query's final stage only — earlier
stages are never changed — and the result SHALL be exactly the values, result keys, warnings and
errors of that merged stage written by hand in place of the saved final stage. A refinement carrying
`population`, `population_inferred`, `name`, `version`, `variables` or any unknown field SHALL be rejected. An empty
refinement SHALL behave exactly like no refinement.

Scenario data for this capability (executed on SQLite and DuckDB): table `orders(id, customer_id, region,
status, amount, ordered_at)` with rows (1, c1, US, paid, 100, 2025-01-05), (2, c1, US, refunded, 40,
2025-01-20), (3, c2, EU, paid, 70, 2025-02-03), (4, c3, EU, paid, 35, 2025-02-17), (5, c2, US, paid,
50, 2025-03-09); model `orders` over it with default time dimension `ordered_at`. Saved query
`monthly_revenue` = `{"population": "orders", "time_dimensions": [{"field": "ordered_at",
"granularity": "month"}], "metrics": [{"metric": "sum(amount)", "alias": "revenue"}], "filter":
["status = 'paid'"]}`, which runs by name to 2025-01 → 100, 2025-02 → 105, 2025-03 → 50 under keys
`orders.ordered_at`, `orders.revenue`.

#### Scenario: Extra dimension
- **WHEN** `monthly_revenue` runs with refinement `{"dimensions": ["region"]}`
- **THEN** the rows are (US, 2025-01, 100), (EU, 2025-02, 105), (US, 2025-03, 50)

#### Scenario: Extra measure
- **WHEN** `monthly_revenue` runs with refinement `{"metrics": ["count(*)"]}`
- **THEN** the result adds `orders._count` = 1, 2, 1 for 2025-01, 2025-02, 2025-03

#### Scenario: Extra filter ANDs with the saved filters
- **WHEN** `monthly_revenue` runs with refinement `{"filter": ["amount >= 50"]}`
- **THEN** the rows are 2025-01 → 100, 2025-02 → 70, 2025-03 → 50

#### Scenario: Window added to the saved time dimension
- **WHEN** `monthly_revenue` runs with refinement `{"time_dimensions": [{"field": "ordered_at", "granularity": "month", "date_range": ["2025-02-01", "2025-03-31"]}]}`
- **THEN** the rows are 2025-02 → 105, 2025-03 → 50

#### Scenario: Order and limit
- **WHEN** `monthly_revenue` runs with refinement `{"order": [{"field": "revenue", "direction": "desc"}], "limit": 1}`
- **THEN** the single row is 2025-02 → 105

#### Scenario: Second granularity renames the saved key
- **WHEN** `monthly_revenue` runs with refinement `{"time_dimensions": ["date_trunc('year', ordered_at)"]}`
- **THEN** the result keys are `orders.ordered_at.month`, `orders.ordered_at.year` and `orders.revenue`, as in the hand-written query with both granularities

#### Scenario: Functional granularity in dimensions
- **WHEN** a refinement's `dimensions` contains `date_trunc('year', ordered_at)`
- **THEN** it merges as that time dimension, exactly as a query's `dimensions` entry does

#### Scenario: Empty refinement
- **WHEN** `monthly_revenue` runs with refinement `{}`
- **THEN** the result equals the plain run by name, with the same SQL

#### Scenario: Forbidden refinement fields
- **WHEN** a refinement carries `population`, `population_inferred`, `name`, `version`, `variables`, or an unknown key
- **THEN** it is rejected as invalid input before anything executes
