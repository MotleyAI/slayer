# models/list-columns Specification

## Purpose
Defines list (array) columns as model metadata: the `ARRAY` type and its element type, ingestion of native list types, what a list column may and may not do, the non-NULL-element rule, re-typing on re-ingest, and how list columns are presented to agents.

## Requirements

### Requirement: The list type carries an element type

A column whose values are lists of scalars SHALL have `type: ARRAY` and an `element_type` naming the scalar type of its elements (`TEXT`, `INT`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`, or `UNKNOWN` for opaque elements such as nested lists or records). `element_type` SHALL be required when the type is `ARRAY`, SHALL be rejected on any other type, and SHALL never be `ARRAY`. A column with `UNKNOWN` elements supports only `length` and NULL tests.

#### Scenario: Missing element type
- **WHEN** a model is saved with a column `type: ARRAY` and no `element_type`
- **THEN** the save fails with a typed error naming the column and `element_type`

#### Scenario: Element type on a scalar column
- **WHEN** a model is saved with a column `type: TEXT, element_type: INT`
- **THEN** the save fails with a typed error naming the column

#### Scenario: Opaque elements
- **WHEN** a column is `type: ARRAY, element_type: UNKNOWN` and a query uses `length(col)` and, separately, `col.value` as a dimension
- **THEN** the `length` query runs, and the dimension query fails with a typed error naming the opaque element type

### Requirement: Ingestion maps native list types

Ingestion SHALL map each dialect's native list type to `ARRAY` with the declared element type: Postgres `T[]`, BigQuery `REPEATED T` / `ARRAY<T>`, DuckDB `T[]` / `LIST` (on its INFORMATION_SCHEMA fallback path too), ClickHouse `Array(T)` with `Nullable` / `LowCardinality` unwrapped, Trino `array(T)`, and Snowflake structured `ARRAY(T)`. Nested lists and lists of records SHALL get `element_type: UNKNOWN`. `db_type` SHALL keep the raw database type including its element type.

#### Scenario: BigQuery REPEATED field
- **WHEN** a BigQuery table with `skills REPEATED STRING` is ingested
- **THEN** the column is `type: ARRAY`, `element_type: TEXT`, with a `db_type` naming the array and its `STRING` element

#### Scenario: Postgres integer array
- **WHEN** a Postgres table with `scores integer[]` is ingested
- **THEN** the column is `type: ARRAY`, `element_type: INT`

#### Scenario: DuckDB list via the INFORMATION_SCHEMA fallback
- **WHEN** a DuckDB table with `scores INTEGER[]` is ingested through the INFORMATION_SCHEMA path
- **THEN** the column is `type: ARRAY`, `element_type: INT`, never `INT`

#### Scenario: ClickHouse wrapped element
- **WHEN** a ClickHouse table with `tags Array(LowCardinality(Nullable(String)))` is ingested
- **THEN** the column is `type: ARRAY`, `element_type: TEXT`

#### Scenario: Nested list
- **WHEN** a DuckDB table with `grid INTEGER[][]` is ingested
- **THEN** the column is `type: ARRAY`, `element_type: UNKNOWN`

### Requirement: Snowflake untyped arrays get an inferred element type

For a Snowflake `ARRAY` without a declared element type, ingestion SHALL infer `element_type` from the stored elements with one query per such column: a single scalar family maps to its type, mixed scalar families to `TEXT`, and objects, nested arrays or no non-NULL elements to `UNKNOWN`. A re-ingest SHALL re-infer only while the stored `element_type` is `UNKNOWN`. A structured `ARRAY(T)` SHALL use `T` without a query.

#### Scenario: Integer elements
- **WHEN** an untyped Snowflake `ARRAY` column holds only integers and is ingested
- **THEN** its `element_type` is `INT`

#### Scenario: Mixed scalar elements
- **WHEN** an untyped Snowflake `ARRAY` column holds strings and integers
- **THEN** its `element_type` is `TEXT`

#### Scenario: Object elements
- **WHEN** an untyped Snowflake `ARRAY` column holds objects
- **THEN** its `element_type` is `UNKNOWN`

#### Scenario: Structured array
- **WHEN** a Snowflake `ARRAY(VARCHAR)` column is ingested
- **THEN** its `element_type` is `TEXT` and no inference query runs

### Requirement: A list's elements are its non-NULL elements

Every list construct — list functions, element access, sampling and `collect` — SHALL treat a list's elements as its non-NULL elements. A NULL list is distinct from an empty list.

#### Scenario: NULL element is not an element
- **WHEN** a row's `skills` is `['sql', NULL]`
- **THEN** `length(skills)` is 1, `contains(skills, 'go')` is FALSE, and the row contributes one `skills.value` row, `'sql'`

### Requirement: List columns are not dimensions, comparisons or general aggregation inputs

A list column SHALL NOT be a dimension, and SHALL NOT be an operand of `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, `BETWEEN` or `IS DISTINCT FROM`. `IS NULL` / `IS NOT NULL` SHALL be accepted. The only aggregation over a list-valued source SHALL be `count`, which counts non-NULL lists; every other built-in aggregation, `collect` and every custom aggregation over a list-valued source SHALL be rejected — whether the source is a column or an expression. `allowed_aggregations` on a list column naming anything but `count` SHALL fail at save time. Each rejection SHALL be a typed error naming its remedy.

#### Scenario: List as a dimension
- **WHEN** a query on `contractors` has the dimension `skills`
- **THEN** it fails with a typed error naming `skills.value`

#### Scenario: Comparing a list
- **WHEN** a filter is `skills = 'go'` or `skills IN ('go', 'sql')`
- **THEN** it fails with a typed error naming `contains` / `contains_any`

#### Scenario: Distinct count of a list
- **WHEN** a measure is `count_distinct(skills)`
- **THEN** it fails with a typed error naming `count_distinct(skills.value)`

#### Scenario: Counting lists
- **WHEN** a measure is `count(skills)` over rows holding `['go']`, `[]` and NULL
- **THEN** it returns 2

#### Scenario: Custom aggregation over a list
- **WHEN** a model's custom aggregation `my_agg` is applied as `my_agg(skills)`
- **THEN** the query fails with a typed error naming the list type

#### Scenario: Aggregating a collected list
- **WHEN** a measure is `min(collect(team))`
- **THEN** it fails with a typed error naming the list type

#### Scenario: Save-time allowed aggregations
- **WHEN** a model is saved with a list column whose `allowed_aggregations` is `[sum]`
- **THEN** the save fails with a typed error naming the column

#### Scenario: NULL test
- **WHEN** a filter is `skills IS NULL`
- **THEN** it keeps exactly the rows whose list is NULL

### Requirement: Backends without native lists reject list constructs

On SQLite, MySQL and SQL Server, every list construct — a list function over a list, an array literal, element access and `collect` — SHALL fail at plan time with a typed error naming the construct and the database.

#### Scenario: List function on SQLite
- **WHEN** a model on a SQLite datasource declares a list column and a query filters `contains(skills, 'go')`
- **THEN** the query fails at plan time naming `contains` and SQLite

#### Scenario: collect on MySQL
- **WHEN** a query on a MySQL datasource has the measure `collect(team)`
- **THEN** it fails at plan time naming `collect` and MySQL

### Requirement: Re-ingest re-types TEXT list columns

When a stored column is `TEXT` and its live column is a native list, re-ingest SHALL change it to `type: ARRAY` with the live `element_type` and `db_type`, keeping every other stored attribute (description, label, `allowed_aggregations`, meta). A stored `ARRAY` column's `element_type` SHALL NOT be overwritten, except a Snowflake `UNKNOWN` being re-inferred.

#### Scenario: Re-typing keeps metadata
- **WHEN** a stored model has `skills: TEXT` with a description, the live column is `VARCHAR[]`, and the datasource is re-ingested
- **THEN** `skills` is `type: ARRAY`, `element_type: TEXT`, and keeps its description

#### Scenario: A user's element type survives
- **WHEN** a stored `ARRAY` column has `element_type: TEXT` set by the user over a live `INTEGER[]` column, and the datasource is re-ingested
- **THEN** its `element_type` stays `TEXT`

### Requirement: Agents see list columns and their operations

`inspect` of a model or column, `search` column hits and `models_summary` SHALL render a list column's type as `ARRAY<element type>`, its element samples when profiled, and the operations it supports: `contains`, `contains_all` / `contains_any` and element access through `<column>.value`. The JSON rendering SHALL carry `element_type`.

#### Scenario: Rendered list column
- **WHEN** `inspect_model` renders `contractors` with a profiled `skills` column
- **THEN** the rendering shows `ARRAY<TEXT>`, the sampled elements, and names `contains`, `contains_all` and `skills.value`

### Requirement: Snowflake JSON reads on arrays keep working

`json_value` SHALL accept a list column whose `db_type` is a JSON-family type, so that a Snowflake `ARRAY` remains readable as JSON.

#### Scenario: Reading an array element as JSON
- **WHEN** a Snowflake `ARRAY` column `tags` is queried with the dimension `json_value(tags, '$[0]')`
- **THEN** the query renders the Snowflake JSON read of the first element
