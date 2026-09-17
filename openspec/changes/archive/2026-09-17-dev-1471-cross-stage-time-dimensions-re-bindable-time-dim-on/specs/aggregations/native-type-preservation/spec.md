# aggregations/native-type-preservation Delta

## ADDED Requirements

### Requirement: Declared temporal casts are suppressed on dialects without native temporal storage

On a dialect whose temporal values are stored as text under numeric affinity (SQLite), a declared or inferred DATE / TIMESTAMP result type SHALL NOT be rendered as `CAST(... AS DATE)` / `CAST(... AS TIMESTAMP)` around an aggregate, a composite, a windowed or ranked value, or a derived column expression, because that cast collapses a text date to its leading year. Executed temporal values on such a dialect SHALL be the full date. Every other dialect SHALL render its declared-type casts unchanged.

#### Scenario: Temporal max returns the full date on SQLite

- **WHEN** a query against a SQLite datasource selects `created_at:max` grouped by `customer_id`
- **THEN** the generated SQL carries no `CAST(... AS TIMESTAMP)` around the aggregate and each value is the customer's full latest date; the same query on DuckDB returns the same dates

#### Scenario: Partitioned temporal aggregate returns the full date on SQLite

- **WHEN** a query against a SQLite datasource selects `created_at:max(partition_by=customer_id)`
- **THEN** the attached value is the full latest date per customer

#### Scenario: Derived temporal column returns the full date on SQLite

- **WHEN** a model declares a TIMESTAMP column whose `sql` is a non-identifier expression (for example `coalesce(shipped_at, created_at)`) and a SQLite query projects it as a dimension and aggregates it with `max`
- **THEN** both values are full dates, by executed values on SQLite and DuckDB

#### Scenario: Other dialects keep their casts

- **WHEN** the same temporal aggregate is rendered for Postgres, DuckDB, T-SQL or BigQuery
- **THEN** the generated SQL is byte-identical to the SQL rendered before this change
