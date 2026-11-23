## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: A measure-name collision suggests a free name
**Reason**: A user-written metric name may now equal a field name under every conformance (fields are always qualified, names bare), so `MetricNameCollidesWithFieldError` is never raised and has no suggestion to carry.
**Migration**: None needed: the query that raised it now executes, keyed by its name. Code catching `MetricNameCollidesWithFieldError` can drop the handler; `NameCollisionError` still covers the remaining collisions.
