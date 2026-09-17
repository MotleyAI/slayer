# queries/time-dimensions Specification

## Purpose
The `TimeDimension` input contract: which keys may name the time column, and how the
object is serialized and advertised, independent of any one query surface.

## Requirements

### Requirement: column is an accepted alias of dimension

A time dimension SHALL accept its column under either the key `dimension` or the key
`column`, with identical coercion (a bare string or a column-reference object).
Serialization SHALL always emit `dimension`, and the JSON schema SHALL advertise
`dimension` as the property.

#### Scenario: column key validates and dumps as dimension

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"column": "created_at", "granularity": "month"}]`
- **THEN** the time dimension's column is `created_at`, and dumping the query emits
  `{"dimension": ..., "granularity": "month"}` with no `column` key

#### Scenario: dimension key still validates

- **WHEN** a query is constructed with
  `time_dimensions` = `[{"dimension": "created_at", "granularity": "month"}]`
- **THEN** it validates exactly as before, and constructing the object with the
  `dimension` keyword still works

#### Scenario: Schema advertises dimension

- **WHEN** the query's JSON schema is generated
- **THEN** the time-dimension object's property is `dimension` and no `column` property is
  listed
