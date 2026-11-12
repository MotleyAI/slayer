## MODIFIED Requirements

### Requirement: A custom granularity is accepted wherever a built-in one is

A custom granularity name SHALL be accepted, and SHALL behave exactly like a built-in granularity with the same buckets, in every position that accepts a granularity: a time dimension's `granularity` (on a model column, a joined column, a stage column or `time_spine.timestamp`), the unit of a `date_trunc('<name>', expr)` call — in `dimensions`, `time_dimensions` string entries and `order`, and inside an expression or filter —, `Column.granularity`, and a time-ordered transform's unit and calendar step. Names SHALL resolve case-insensitively against the datasource the query runs against, at binding; a name that is neither built-in nor defined there SHALL fail with a typed error listing the built-in and the datasource's granularities. A custom granularity SHALL have no call form: a call `<name>(expr)` that resolves to no aggregation SHALL fail with a typed error whose remedy is `date_trunc('<name>', expr)`, and a model's custom aggregation of that name SHALL resolve as the aggregation. Result keys SHALL follow the built-in rules (the granularity is appended only to disambiguate same-column time dimensions).

#### Scenario: Functional form and order key

- **WHEN** a query has `dimensions=["date_trunc('fiscal_year', order_date)"]`, `measures=["sum(amount)"]` and `order=[{"column": "date_trunc('fiscal_year', order_date)", "direction": "desc"}]`
- **THEN** it returns the fiscal-year rows above in descending order, with result key `orders.order_date`, identically to the explicit time-dimension form

#### Scenario: Custom granularity in a filter

- **WHEN** a query filters `date_trunc('fiscal_year', order_date) = '2024-04-01'` and selects `sum(amount)`
- **THEN** the value is 50

#### Scenario: On the spine

- **WHEN** a spine query groups by `time_spine.timestamp` at `fiscal_year` with `date_range: ["2023-04-01", "2026-03-31"]` and selects `sum(orders.amount)`
- **THEN** the rows are 2023-04-01: 10, 2024-04-01: 50, 2025-04-01: 40

#### Scenario: Unknown name

- **WHEN** a query uses granularity `fiscal_yr`, the dimension entry `date_trunc('mnth', order_date)`, or the dimension entry `mnth(order_date)`
- **THEN** planning fails with a typed error listing the built-in granularities and `fiscal_year`, `billing_month`, `quarter_hour`, `sprint`

#### Scenario: Name scoped to its datasource

- **WHEN** a query against a second datasource that defines no granularities uses `fiscal_year`
- **THEN** it fails with the unknown-granularity error

#### Scenario: Unit matched case-insensitively

- **WHEN** a query has `dimensions=["date_trunc('Fiscal_Year', order_date)"]` and selects `sum(amount)`
- **THEN** it returns the same rows and result key as the lowercase unit

#### Scenario: Call form refused with the date_trunc remedy

- **WHEN** a query has the dimension entry `fiscal_year(order_date)`, or the filter `fiscal_year(order_date) = '2024-04-01'`, and the model defines no aggregation named `fiscal_year`
- **THEN** planning fails with the typed error naming `date_trunc('fiscal_year', order_date)`, and no SQL is generated

#### Scenario: A like-named custom aggregation resolves as the aggregation

- **WHEN** the model `orders` defines a custom aggregation named `sprint` and a query selects `sprint(amount)` with `dimensions=["date_trunc('sprint', order_date)"]`
- **THEN** the measure is the model's aggregation, the dimension is the `sprint` bucket, and no conflict error is raised
