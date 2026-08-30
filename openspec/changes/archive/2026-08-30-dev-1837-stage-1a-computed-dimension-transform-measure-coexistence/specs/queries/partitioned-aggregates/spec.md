## Purpose

Defines how `partition_by=` aggregations (measures computed at an explicitly declared grain, attached back to the query rows) compose with the rest of the query surface: `window=`, `first`/`last`, transforms, filters, ORDER BY, and other measure kinds.

## ADDED Requirements

### Requirement: Temporal transforms compose with partitioned measures
A partitioned measure SHALL be usable in the same query as `time_shift`, `change`, and `change_pct`, producing valid SQL on every supported dialect: the shifted re-aggregation groups only by real query dimensions, and reserved internal placeholder names never reach the emitted statement.

#### Scenario: Partitioned measure with a time-shift measure executes
- WHEN a query selects `amount:sum(partition_by=region)` and `time_shift(amount:sum, periods=-1)` over a month time dimension
- THEN the query executes with correct values for both measures and the emitted SQL contains no reserved placeholder prefix

#### Scenario: Shifted re-aggregation grain excludes the attached value
- WHEN the shifted comparison period is computed for such a query
- THEN it groups by the query's dimensions and shifted time bucket only, and joins back on exactly that grain
