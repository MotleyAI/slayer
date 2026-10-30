## MODIFIED Requirements

### Requirement: collect returns the distinct sorted non-NULL values

`collect(x)` SHALL return, per cell, the list of distinct non-NULL values of `x`, sorted ascending. A cell with no rows, or whose inputs are all NULL, SHALL take the empty list — never NULL or a list holding NULL. This SHALL hold whatever the column's physical type, including one whose declared type differs from it (a BigQuery `NUMERIC` or `BIGNUMERIC` column read as DOUBLE).

#### Scenario: Values per team
- **WHEN** a query on `contractors` has the dimension `team` and the measure `collect(city)`
- **THEN** team A returns `['Oslo']` and team B returns `['Oslo', 'Rome']`

#### Scenario: Elements per team
- **WHEN** a query has the dimension `team` and the measure `collect(skills.value)`
- **THEN** team A returns `['go', 'sql']` and team B returns `['go']`

#### Scenario: All inputs NULL
- **WHEN** a query filters `team = 'A' AND city IS NULL` and measures `collect(city)`
- **THEN** it returns `[]`

#### Scenario: Exact numerics on BigQuery
- **WHEN** a query on BigQuery has the dimension `team` and measures `collect(n)` and `collect(big)` over `NUMERIC` and `BIGNUMERIC` columns holding `1.5`, `2.25`, `1.5` for team a and only NULL for team b
- **THEN** team a returns `[1.5, 2.25]` for both, and team b returns `[]` for both

### Requirement: collect runs on every database with native lists

`collect` SHALL return the same lists on Postgres, DuckDB, BigQuery, Snowflake, ClickHouse, Trino and Presto, and SHALL fail at plan time on SQLite, MySQL, MariaDB, SQL Server, Redshift, Databricks, Spark and Oracle.

#### Scenario: Mixed NULL inputs on every backend
- **WHEN** the per-team `collect(city)` query runs on each native-list backend
- **THEN** every backend returns team A `['Oslo']` without a NULL element

#### Scenario: Refused without native lists
- **WHEN** a query on an Oracle datasource has the measure `collect(team)`
- **THEN** it fails at plan time naming `collect` and Oracle
