## ADDED Requirements

### Requirement: Division by zero is NULL

A division whose divisor evaluates to 0 SHALL be NULL on every backend and in every position (measure, filter, order key, computed dimension) — never an error, an infinity or NaN.

#### Scenario: A ratio over an empty population

- **WHEN** a query measures `sum(amount) / count(*)` and its filters keep no population row
- **THEN** one row is returned whose ratio is NULL on SQLite and DuckDB, and the SQL emitted for every dialect guards the divisor with `NULLIF(…, 0)`
