## Purpose

Defines the error contract for query type errors: every well-formedness violation the query type checker rejects surfaces as one typed, stably formatted error family that callers can catch as a whole or per rule family.

## ADDED Requirements

### Requirement: Query type errors are typed and stably formatted
Every query type error the checker raises SHALL be an instance of a concrete rule-family class under the `QueryTypeError` base (itself a `SlayerError`, and so a `ValueError`); the base itself SHALL never be raised, and constructing it directly SHALL fail. The families are `TimeAxisError` (with `TimeDimensionColumnError` beneath it), `WindowDurationError`, `PartitionKeyError`, `UnsafeJoinInputError`, `UnanalyzableDependencyError`, `TransformInputError`, `ComputedDimensionError`, `AssociationError`, `ParameterGrainError`, `ReaggregationError`, `NameCollisionError` (with `MeasureNameCollidesWithColumnError`, `CanonicalAliasShadowsColumnError`, `DuplicateMeasureNameError` beneath it), `DimensionTypeError`, `ModelFilterError`, `PositionTypingError` and `DistinctDimensionValuesError`. The error's text SHALL be: a first line `<ClassName>: <summary>` naming the defect; then, when the error has a subject (a measure, filter, dimension, order item, transform, time dimension or model filter), a line `  at <subject>`; then, when a remedy exists, a line `  suggestion: <remedy>`. The summary, subject and remedy SHALL also be exposed as the error's `summary`, `location` and `suggestion` attributes (`None` when absent).

#### Scenario: A partition key that is not a query dimension
- **WHEN** a query's rank-family transform declares a `partition_by=` column that is neither a query dimension nor a time dimension
- **THEN** the query fails with a `PartitionKeyError` whose first line is `PartitionKeyError: ` followed by its summary, whose `at` line names the transform's measure, and whose `suggestion:` line names the remedy and the available dimensions

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
A time-ordered transform that evaluates at a grain not containing its time axis SHALL fail with a `TimeAxisError`, never a `NotImplementedError`. The REST query endpoint SHALL answer it with HTTP 400 carrying the error text, and the Flight SQL server SHALL report it as an invalid-argument error, not as unimplemented.

#### Scenario: Time axis missing from an aggregated transform's grain
- **WHEN** a measure aggregates a time-ordered transform (such as `cumsum`) over an inner aggregate whose `partition_by=` omits the query's time dimension
- **THEN** the query fails with a `TimeAxisError` that is not a `NotImplementedError`, whose suggestion is to include the time key in the aggregate's `partition_by=`

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
