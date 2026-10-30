# aggregations/collect Specification

## Purpose
Defines `collect`, the built-in aggregation that returns the distinct, sorted list of a value's non-NULL inputs per cell, and how its list results are typed and returned.

The examples use `contractors(id, team, city, skills ARRAY<TEXT>)` with these rows:
- Ann: team A, city Oslo, skills `['go', 'sql', 'go']`
- Bob: team A, city NULL, skills `['sql', NULL]`
- Cid: team B, city Rome, skills `[]`
- Dee: team B, city Oslo, skills NULL
- Eve: team B, city Oslo, skills `['go']`

## Requirements

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

### Requirement: collect takes scalar inputs only

`collect` SHALL be a built-in aggregation eligible by default on `TEXT`, `INT`, `DOUBLE`, `BOOLEAN`, `DATE` and `TIMESTAMP` columns, on element columns (`L.value`) and on scalar expressions. It SHALL be rejected on list-valued and opaque inputs with a typed error. Its result SHALL be a list of the input's type.

#### Scenario: collect of a list
- **WHEN** a measure is `collect(skills)`
- **THEN** it fails with a typed error naming `collect(skills.value)`

### Requirement: collect results are lists

A `collect` result SHALL be usable wherever a list is accepted: as an operand of the list functions in formulas and measure filters, and as a list column of a later query stage. It SHALL be rejected as a dimension. REST and MCP responses SHALL return it as a JSON list.

#### Scenario: Filtering on a collected list
- **WHEN** a query has the dimension `team`, the measure `collect(skills.value)` and the filter `contains(collect(skills.value), 'sql')`
- **THEN** only team A is returned

#### Scenario: JSON result
- **WHEN** the query grouping `collect(city)` by `team` is run through the MCP `query` tool with JSON output
- **THEN** team B's value is the JSON list `["Oslo", "Rome"]`

#### Scenario: collect as a dimension
- **WHEN** a dimension is `collect(city, partition_by=team)`
- **THEN** the query fails with a typed error naming the list type

### Requirement: collect runs on every database with native lists

`collect` SHALL return the same lists on Postgres, DuckDB, BigQuery, Snowflake, ClickHouse, Trino and Presto, and SHALL fail at plan time on SQLite, MySQL, MariaDB, SQL Server, Redshift, Databricks, Spark and Oracle.

#### Scenario: Mixed NULL inputs on every backend
- **WHEN** the per-team `collect(city)` query runs on each native-list backend
- **THEN** every backend returns team A `['Oslo']` without a NULL element

#### Scenario: Refused without native lists
- **WHEN** a query on an Oracle datasource has the measure `collect(team)`
- **THEN** it fails at plan time naming `collect` and Oracle
