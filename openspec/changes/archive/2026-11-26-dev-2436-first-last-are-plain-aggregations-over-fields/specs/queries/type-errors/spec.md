## MODIFIED Requirements

### Requirement: The time-axis violation is a type error, not an unimplemented feature
A time-ordered transform (`cumsum`, `lag`, `lead`, `time_shift`, `change`, `change_pct`, `consecutive_periods`) whose operand grain does not contain its time axis — the query's active time bucket — SHALL fail with a `TimeAxisError`, never a `NotImplementedError`, in every position: dimension, measure, filter, order key, aggregation-source constituent and aggregation parameter, nested inside arithmetic, a scalar call or another transform included. The operand grain SHALL be the union of the operand's inner grains: an aggregate's explicit `partition_by=` (a raw time column naming the active bucket's column counts as that bucket), else the query grain; a windowed aggregate adding the active time bucket; a nested `first` / `last` over an aggregate contributing its own grain, its ranking key dropped (`queries/semantics` › A ranked aggregation's grain drops its ranking key); a nested transform contributing its own operand grain. A `first` / `last` over an aggregate whose defaulted ranking key, the active time bucket, is not in its operand grain SHALL fail with the same `TimeAxisError` in every position (`aggregations/functional-form` › Default ranking key). The error's suggestion SHALL name including the time key in the inner aggregate's `partition_by=`. The REST query endpoint SHALL answer it with HTTP 400 carrying the error text, and the Flight SQL server SHALL report it as an invalid-argument error, not as unimplemented.

#### Scenario: Time axis missing from an aggregated transform's grain
- **WHEN** a measure aggregates a time-ordered transform (such as `cumsum`) over an inner aggregate whose `partition_by=` omits the query's time dimension
- **THEN** the query fails with a `TimeAxisError` that is not a `NotImplementedError`, whose suggestion is to include the time key in the aggregate's `partition_by=`

#### Scenario: Calendar-step transforms over a time-less operand fail in every position
- **WHEN** a query on `orders` by `region` over a month time dimension on `ordered_at` uses `time_shift(sum(amount, partition_by=region), -1)`, `change(sum(amount, partition_by=[]))` or `change_pct(sum(amount, partition_by=region))` as a measure, in a filter, as an order key, inside arithmetic beside `sum(amount)`, inside `rank(…, direction='desc')`, as the source of `max(…)`, as the `weight=` parameter of `weighted_avg(sum(amount, partition_by=[region, ordered_at]), weight=…)`, or inside a dimension expression `CASE WHEN … > 0 THEN 1 ELSE 0 END`
- **THEN** every query fails with a `TimeAxisError` whose suggestion is to include the time key in the aggregate's `partition_by=`, exactly as `cumsum` over the same operand does

#### Scenario: A calendar-step operand that keeps the axis executes
- **WHEN** the same query selects `time_shift(sum(amount, partition_by=[region, ordered_at]), -1)`
- **THEN** it executes, each row reading the same value as `time_shift(sum(amount), -1)` by executed values on SQLite and DuckDB

#### Scenario: A nested first/last drops the axis in every position
- **WHEN** a query by `region` over a month time dimension uses `time_shift(last(sum(amount, partition_by=[region, ordered_at])), -1)` or `cumsum(last(sum(amount, partition_by=[region, ordered_at])))` as a measure, as the source of `max(…)`, or inside a dimension expression
- **THEN** every query fails with a `TimeAxisError` that is not a `NotImplementedError` — the `last` drops its ranking key, the month bucket, from its grain

#### Scenario: A windowed inner carries the axis in every position
- **WHEN** a query by `region` over a month time dimension uses `cumsum(sum(amount, window='90d', partition_by=region))` as a measure, as the source of `max(…)`, and inside a dimension expression `CASE WHEN … > 0 THEN 1 ELSE 0 END`
- **THEN** every query executes; `max(…)` reads the measure form's value in each `(region, month)` cell and the dimension reads 1 exactly in the cells where the measure form is positive, by executed values on SQLite and DuckDB

#### Scenario: The axis is the query's active time bucket
- **WHEN** a time-ordered transform's inner aggregate is partitioned by a raw time column whose bucket is the active one (at a built-in or a custom granularity), by a downstream stage's own time dimension in that stage, or by the time-spine bucket in a spine query
- **THEN** the query executes; and with two time dimensions where `main_time_dimension` selects one, an inner partitioned only by the other time column fails with a `TimeAxisError`

#### Scenario: REST answers the time-axis violation with 400
- **WHEN** the same query is posted to the REST query endpoint
- **THEN** the response is HTTP 400 and its detail starts with `TimeAxisError:`

#### Scenario: Flight SQL reports the violation as invalid argument
- **WHEN** a type error is raised while planning a Flight SQL query
- **THEN** the client receives an invalid-argument error carrying the error text, not an unimplemented error

#### Scenario: A ranked aggregation over an operand without the axis fails in every position
- **WHEN** a query on `orders` by `region` over a month time dimension uses
  `last(sum(amount, partition_by=region))` as a measure, in a filter, as an order key or as
  the source of `max(…)`, and a query with no time dimension selects `last(sum(amount))`
- **THEN** every query fails with a `TimeAxisError`, never a `NotImplementedError` or a
  value
