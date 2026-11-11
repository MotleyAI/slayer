## MODIFIED Requirements

### Requirement: The spine has no countable rows

An aggregation whose home is `time_spine` (any aggregation over `time_spine.timestamp`, or `count(*)` over a spine-rooted population) SHALL fail with a typed error. `time_spine.timestamp` SHALL be usable only as a time dimension with a granularity or inside a frame-bound filter; as a plain dimension, a computed-dimension operand, an order key other than its projected bucket, or in a raw-rows (`distinct_dimension_values: false`) query it SHALL fail with a typed error. When the offending plain dimension is a date-part extraction of `time_spine.timestamp` whose part is a built-in granularity (e.g. `month(time_spine.timestamp)`), the error's remedy SHALL name `date_trunc('<part>', time_spine.timestamp)`.

#### Scenario: Aggregation over the spine

- **WHEN** a spine-month query, or a spine-month stage of a query list, selects `count(*)` with `source_model: time_spine`, or `min(time_spine.timestamp)`
- **THEN** it fails with the typed no-countable-rows error

#### Scenario: Plain spine dimension

- **WHEN** a query lists `time_spine.timestamp` under `dimensions`, or sets `distinct_dimension_values: false` with a spine time dimension
- **THEN** it fails with a typed error naming the time-dimension remedy

#### Scenario: Extraction of the spine column as a dimension

- **WHEN** a query lists `month(time_spine.timestamp)` under `dimensions`
- **THEN** it fails with the typed error, whose remedy names `date_trunc('month', time_spine.timestamp)`, and no SQL is generated

### Requirement: An expression is never an axis

A dataset's axis SHALL be a column, never an expression: a time dimension whose expression reads `time_spine.timestamp` other than as the whole bare column SHALL fail before any SQL is generated with a typed error naming the time dimension and the remedy of bucketing `time_spine.timestamp` itself. A time dimension whose expression is a date-part extraction of `time_spine.timestamp` (e.g. `month(time_spine.timestamp)`) is such an expression, and its remedy SHALL name `date_trunc('<unit>', time_spine.timestamp)`, `<unit>` the time dimension's granularity. An expression time dimension that does not read the spine SHALL be an ordinary dimension of the spine query's P, exactly as a fact column's time dimension is.

#### Scenario: Spine dimension over an expression rejected

- **WHEN** a query buckets `time_spine.timestamp + INTERVAL '1 day'` at `month`, or `coalesce(time_spine.timestamp, orders.order_date)` at `month`
- **THEN** planning fails with the typed error naming the time dimension and the remedy, and no SQL is generated

#### Scenario: A fact expression time dimension is a dimension of P

- **WHEN** the two-fact Jan–Jun spine-month query also buckets `coalesce(orders.shipped_at, orders.order_date)` at `month`
- **THEN** it executes with population `spine × P`, P carrying that bucket, with the same values per (spine month, bucket) as the query with a column holding the coalesced value

#### Scenario: Extraction of the spine column as a time dimension

- **WHEN** a query has `time_dimensions=[{"dimension": "month(time_spine.timestamp)", "granularity": "month"}]`
- **THEN** planning fails with the typed expression-axis error, whose remedy names `date_trunc('month', time_spine.timestamp)`, and no SQL is generated
