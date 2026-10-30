## MODIFIED Requirements

### Requirement: A granularity call is a row-level expression in every position

A call `gran(expr)` — `gran` a time granularity (case-insensitive) and `expr` a row-level expression typed DATE or TIMESTAMP (a bare or dotted column reference being the one-leaf case) — SHALL denote `expr` truncated to that granularity, with the same per-dialect bucketing as a time dimension, wherever a row-level expression is legal: filters, aggregation sources and parameters, computed-dimension sub-expressions and arithmetic. A whole `dimensions` or `time_dimensions` entry of that shape SHALL keep its time-dimension rewrite, and an order key of that shape SHALL keep its projected-time-dimension rule. A comparison between `gran(expr)` and a time point SHALL return exactly the rows of the equivalent bound on `expr`. A granularity callee with any other argument shape (`month()`, `month(a, b)`, `month(*)`, keyword arguments) SHALL fail with the typed error naming the required `gran(expr)` shape and the valid granularities; an argument that does not type DATE or TIMESTAMP SHALL fail with the date-operand type error naming the operand.

#### Scenario: Granularity call in a filter

- **WHEN** a filter is `month(created_at) >= '2024-03-15'`
- **THEN** it matches exactly the rows with `created_at >= 2024-04-01`, by executed values on SQLite and DuckDB

#### Scenario: Granularity calls compared with each other

- **WHEN** a filter is `month(shipped_at) = month(ordered_at)`
- **THEN** it matches the rows shipped in the month they were ordered

#### Scenario: Granularity call inside a measure and a computed dimension

- **WHEN** a query has the measure `count_distinct(month(created_at))` and the computed dimension `{"expression": "year(created_at)", "name": "y"}`
- **THEN** the measure counts distinct months and the dimension groups by year bucket, by executed values on SQLite and DuckDB

#### Scenario: Granularity call over an expression

- **WHEN** a filter is `month(coalesce(shipped_at, created_at)) >= '2024-03-15'` and a computed dimension is `{"expression": "year(CAST(created_txt AS DATE))", "name": "y"}`
- **THEN** the filter matches exactly the rows whose coalesced timestamp is at or after 2024-04-01 and the dimension groups by the year of the cast value, by executed values on SQLite and DuckDB

#### Scenario: Wrong-shape granularity call in a filter

- **WHEN** a filter contains `month()`, `month(a, b)` or `month(*)`
- **THEN** the query fails with the typed error naming the `gran(expr)` shape and the valid granularities

#### Scenario: Non-temporal granularity argument

- **WHEN** a filter contains `month(upper(x)) >= '2024-01-01'`
- **THEN** the query fails with the date-operand type error naming `upper(x)`

#### Scenario: Granularity call against an instant

- **WHEN** filters are `month(created_at) = '2025-03-15 10:00:00'` and `month(created_at) = '2025-03-01 00:00:00'`
- **THEN** the first matches no rows and the second matches the March rows
