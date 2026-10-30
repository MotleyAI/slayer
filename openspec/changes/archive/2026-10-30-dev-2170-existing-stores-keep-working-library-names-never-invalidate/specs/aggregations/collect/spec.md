## ADDED Requirements

### Requirement: A model's own collect formula is its aggregation

When a model defines the aggregation `collect` with a `formula`, every `collect` call over that model's columns SHALL be that aggregation: its result type, empty value, database support and SQL SHALL follow the formula, never the built-in list aggregation. Other models SHALL keep the built-in.

#### Scenario: Custom collect on a database without lists

- **WHEN** a SQLite model stored by the previous release defines `collect` with formula `SUM({value})` and a query asks for `collect(a)`
- **THEN** the measure returns the sum of `a`, as it did on the previous release, and no list-support error is raised

#### Scenario: Custom collect on a database with lists

- **WHEN** a DuckDB model defines `collect` with formula `SUM({value})` and a query asks for `collect(a)`
- **THEN** the measure returns the sum of `a`, not a list
