# queries/type-errors Specification

## Purpose
Defines the error contract for query type errors: every well-formedness violation the query type checker rejects surfaces as one typed, stably formatted error family that callers can catch as a whole or per rule family.

## Requirements

### Requirement: Query type errors are typed and stably formatted
Every query type error the checker raises SHALL be an instance of a concrete rule-family class under the `QueryTypeError` base (itself a `SlayerError`, and so a `ValueError`); the base itself SHALL never be raised, and constructing it directly SHALL fail. The families are `TimeAxisError` (with `TimeDimensionFieldError` beneath it), `WindowDurationError`, `PartitionKeyError`, `UnsafeRelationshipInputError`, `UnanalyzableDependencyError`, `TransformInputError`, `ComputedDimensionError`, `AssociationError`, `ParameterGrainError`, `ReaggregationError`, `NameCollisionError` (with `CanonicalAliasShadowsFieldError` and `DuplicateMetricNameError` beneath it), `DimensionTypeError`, `DatasetFilterError`, `PositionTypingError` and `DistinctDimensionValuesError`. The error's text SHALL be: a first line `<ClassName>: <summary>` naming the defect; then, when the error has a subject (a measure, filter, dimension, order item, transform, time dimension or model filter), a line `  at <subject>`; then, when a remedy exists, a line `  suggestion: <remedy>`. The summary, subject and remedy SHALL also be exposed as the error's `summary`, `location` and `suggestion` attributes (`None` when absent).

#### Scenario: A partition key that is not a query dimension
- **WHEN** a query's rank-family transform declares a `partition_by=` column that is neither a query dimension nor a time dimension
- **THEN** the query fails with a `PartitionKeyError` whose first line is `PartitionKeyError: ` followed by its summary, whose `at` line names the transform (`transform '<op>'`), and whose `suggestion:` line names the remedy and the available dimensions

#### Scenario: The whole family is catchable as one type and as ValueError
- **WHEN** any query type error is raised
- **THEN** it is an instance of `QueryTypeError`, `SlayerError` and `ValueError`, and callers that catch `ValueError` keep catching it

#### Scenario: A subject-less error has no at line
- **WHEN** a windowed measure's query has no resolvable time dimension
- **THEN** the query fails with a `TimeAxisError` whose text has no `at` line and whose `location` is `None`

#### Scenario: The base is never raised directly
- **WHEN** code constructs `QueryTypeError` itself rather than a family class
- **THEN** construction fails

### Requirement: The time-axis violation is a type error, not an unimplemented feature
A time-ordered transform (`cumsum`, `lag`, `lead`, `time_shift`, `change`, `change_pct`, `consecutive_periods`, `first`, `last`) whose operand grain does not contain its time axis — the query's active time bucket — SHALL fail with a `TimeAxisError`, never a `NotImplementedError`, in every position: dimension, measure, filter, order key, aggregation-source constituent and aggregation parameter, nested inside arithmetic, a scalar call or another transform included. The operand grain SHALL be the union of the operand's inner grains: an aggregate's explicit `partition_by=` (a raw time column naming the active bucket's column counts as that bucket), else the query grain; a windowed aggregate adding the active time bucket; a nested `first` / `last` transform contributing its own operand grain minus its time axis; a nested preserving transform contributing its own operand grain. The error's suggestion SHALL name including the time key in the inner aggregate's `partition_by=`, except when the axis is missing only because a nested `first` / `last` dropped it, where it SHALL instead name applying the time-ordered transform to that `first` / `last`'s operand, or an axis-preserving operand. The REST query endpoint SHALL answer it with HTTP 400 carrying the error text, and the Flight SQL server SHALL report it as an invalid-argument error, not as unimplemented.

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
- **THEN** every query fails with a `TimeAxisError` whose suggestion names applying the time-ordered transform to the `last`'s operand and does not offer the `partition_by=` remedy

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

### Requirement: Malformed window durations raise one typed error
Every malformed `window=` duration — a non-string value, an empty string, a malformed or gapped string, or a non-positive part — SHALL fail with a `WindowDurationError`, whether it reaches the query or the duration parser directly.

#### Scenario: A malformed duration in a query
- **WHEN** a query declares the measure `sum(revenue, window='90x')`
- **THEN** the query fails with a `WindowDurationError` naming the value and the accepted syntax

#### Scenario: The duration parser rejects every malformed input the same way
- **WHEN** the duration parser receives a non-string value, `''`, `'d90'`, or `'0d'`
- **THEN** each fails with a `WindowDurationError`

### Requirement: Raw-rows mode rejections are query type errors at every firing point
The rejections of `distinct_dimension_values=False` — whether raised when the query is constructed or when it is type-checked — SHALL be `DistinctDimensionValuesError` instances in the stable format and members of the `QueryTypeError` family.

#### Scenario: Raw-rows mode with measures fails at construction
- **WHEN** a query sets `distinct_dimension_values=False` and supplies a measure
- **THEN** construction fails with an error whose text starts with `DistinctDimensionValuesError: ` and whose underlying error is a `QueryTypeError`

### Requirement: MCP error text does not repeat the class name
MCP tool errors that prefix an error with its class name SHALL not repeat the prefix when the error's text already starts with `<ClassName>: `.

#### Scenario: A stably formatted error through an MCP tool
- **WHEN** an MCP tool renders a stably formatted error
- **THEN** the class name appears once, at the start of the error text
