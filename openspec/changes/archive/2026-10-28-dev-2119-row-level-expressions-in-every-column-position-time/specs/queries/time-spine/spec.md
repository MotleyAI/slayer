## ADDED Requirements

### Requirement: An expression is never an axis

A dataset's axis SHALL be a column, never an expression: a time dimension whose expression reads `time_spine.timestamp` other than as the whole bare column SHALL fail before any SQL is generated with a typed error naming the time dimension and the remedy of bucketing `time_spine.timestamp` itself. An expression time dimension that does not read the spine SHALL be an ordinary dimension of the spine query's P, exactly as a fact column's time dimension is.

#### Scenario: Spine dimension over an expression rejected

- **WHEN** a query buckets `time_spine.timestamp + INTERVAL '1 day'` at `month`, or `coalesce(time_spine.timestamp, orders.order_date)` at `month`
- **THEN** planning fails with the typed error naming the time dimension and the remedy, and no SQL is generated

#### Scenario: A fact expression time dimension is a dimension of P

- **WHEN** the two-fact Jan–Jun spine-month query also buckets `coalesce(orders.shipped_at, orders.order_date)` at `month`
- **THEN** it executes with population `spine × P`, P carrying that bucket, with the same values per (spine month, bucket) as the query with a column holding the coalesced value
