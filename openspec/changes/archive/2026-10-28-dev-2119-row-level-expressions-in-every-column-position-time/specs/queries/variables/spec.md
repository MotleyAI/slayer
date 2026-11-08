## MODIFIED Requirements

### Requirement: Time-dimension date ranges substitute variables; granularities and columns do not

A time dimension's `date_range` bounds SHALL substitute `{var}` and SHALL then be validated as time points exactly like literal bounds. A placeholder inside a time dimension's expression SHALL substitute a literal exactly as in a computed dimension's expression, and an unnamed such time dimension SHALL take its name from the pre-substitution template. A placeholder in a time dimension's granularity, or one standing for its whole dimension, SHALL be rejected with an error stating that variables supply literal values and cannot name a granularity or a column.

#### Scenario: Variable date range

- **WHEN** a time dimension has `date_range: ["{start}", "{end}"]` with `variables={"start": "2024-01-01", "end": "2024-02-28"}`
- **THEN** rows are restricted to that range, by executed values on SQLite and DuckDB

#### Scenario: Substituted bound is malformed

- **WHEN** a time dimension has `date_range: ["{start}", null]` with `variables={"start": "2025/01/01"}`
- **THEN** execution fails with the typed date-range error listing the accepted time-point forms

#### Scenario: Granularity or column placeholder

- **WHEN** a time dimension has granularity `{g}`, or is written `"{g}(ordered_at)"`, or names column `{col}`
- **THEN** the query is rejected with the error stating variables cannot name a granularity or a column

#### Scenario: Literal placeholder in a time-dimension expression

- **WHEN** a time dimension is `month(coalesce(shipped_at, created_at + INTERVAL '{days}' DAY))` with `variables={"days": 30}`
- **THEN** rows with a NULL `shipped_at` are bucketed by `created_at` 30 days later, by executed values on SQLite and DuckDB, and its result key is derived from the template, not the value
