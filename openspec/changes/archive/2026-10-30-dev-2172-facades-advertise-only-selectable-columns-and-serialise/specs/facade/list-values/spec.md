## MODIFIED Requirements

### Requirement: Facade catalogs keep models with list columns

A model with list columns SHALL stay in every facade catalog, with each list column typed as the facade's text type and selectable through a plain projection. The catalog SHALL NOT advertise a custom-aggregation metric over a list column.

#### Scenario: Catalog listing
- **WHEN** a BI client lists the tables and columns of a datasource whose `contractors` model has a list column `skills`
- **THEN** `contractors` is listed, and `skills` is typed as text

#### Scenario: No custom aggregation over a list
- **WHEN** `contractors` has a parameterless custom aggregation `my_agg` and a list column `skills`
- **THEN** `INFORMATION_SCHEMA.METRICS` has no `skills_my_agg` entry, and keeps one for each scalar column

### Requirement: List values travel as JSON text

Every list value a facade returns — a list column or a `collect` result — SHALL be its JSON array text as `results/value-encoding` defines it (strings quoted and escaped, booleans as `true` / `false`, dates and timestamps as ISO strings); a NULL list SHALL be SQL NULL. The same value SHALL be produced over the Postgres wire, Arrow Flight and the JDBC catalog path.

#### Scenario: A collected list over the Postgres wire
- **WHEN** a Postgres-wire client runs a query returning `collect(city)` per team
- **THEN** team B's value is the text `["Oslo","Rome"]` (whitespace aside), never a Python list representation

#### Scenario: Escaping
- **WHEN** a returned list holds the string `a "b" \c`
- **THEN** its JSON text escapes the quotes and the backslash

#### Scenario: Flight parity
- **WHEN** the same query runs over Arrow Flight
- **THEN** the value is the same JSON text in a UTF-8 column

#### Scenario: A list column over the Postgres wire
- **WHEN** a Postgres-wire client runs `SELECT id, skills FROM contractors`
- **THEN** each row carries that contractor's `skills` as JSON array text, and a NULL list as SQL NULL
