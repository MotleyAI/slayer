## MODIFIED Requirements

### Requirement: The list type carries an element type

A column whose values are lists of scalars SHALL have `type: ARRAY` and an `element_type` naming the scalar type of its elements (`TEXT`, `INT`, `DOUBLE`, `BOOLEAN`, `DATE`, `TIMESTAMP`, or `UNKNOWN` for opaque elements such as nested lists or records). `element_type` SHALL be required when the type is `ARRAY` (except on an `sql`-mode model, see below), SHALL be rejected on any other type, and SHALL never be `ARRAY`. A column with `UNKNOWN` elements supports only NULL tests and `count`; every construct reading its elements or its element set — `length`, grouping, comparison, `IN`, `count_distinct` and `<column>.value` — SHALL fail with a typed error naming the opaque element type.

#### Scenario: Missing element type
- **WHEN** a table-backed model is saved with a column `type: ARRAY` and no `element_type`
- **THEN** the save fails with a typed error naming the column and `element_type`

#### Scenario: Element type on a scalar column
- **WHEN** a model is saved with a column `type: TEXT, element_type: INT`
- **THEN** the save fails with a typed error naming the column

#### Scenario: Opaque elements
- **WHEN** a column is `type: ARRAY, element_type: UNKNOWN` and queries use, in turn, the filter `col IS NULL` with the measure `count(col)`, the dimension `length(col)`, `col` as a dimension of a grouping query, and `col.value` as a dimension
- **THEN** the first query runs, and each of the others fails with a typed error naming the opaque element type

## ADDED Requirements

### Requirement: An sql-mode list column's element type is declared

An `sql`-mode model's `ARRAY` column without a declared `element_type` SHALL have `element_type: UNKNOWN`, whether the model is saved by a client or loaded from a store. A declared `element_type` SHALL be kept by every re-ingest, and an `UNKNOWN` one SHALL change only as the Snowflake untyped-array re-inference allows.

#### Scenario: Undeclared element type
- **WHEN** an `sql`-mode model over DuckDB selects the `VARCHAR[]` column `skills` and declares it `type: ARRAY` without an `element_type`
- **THEN** `skills` is `type: ARRAY`, `element_type: UNKNOWN`, and `skills.value` as a dimension fails naming the opaque element type

#### Scenario: Undeclared element type in a stored model
- **WHEN** a store holds that `sql`-mode model without an `element_type` on `skills`, and it is loaded and saved again
- **THEN** it loads, `skills` is `element_type: UNKNOWN`, and the saved document carries `element_type: UNKNOWN`

#### Scenario: Declared element type
- **WHEN** the same model declares `skills` with `element_type: TEXT` and is re-ingested
- **THEN** `skills` keeps `element_type: TEXT` and `contains(skills, 'go')` runs

#### Scenario: An undeclared element type stays opaque on re-ingest
- **WHEN** the model with the undeclared `skills` over DuckDB `VARCHAR[]` is re-ingested
- **THEN** `skills` stays `element_type: UNKNOWN`

#### Scenario: Snowflake re-inference applies to sql-mode
- **WHEN** an `sql`-mode model over Snowflake declares `tags: ARRAY` without an `element_type`, its query selects an untyped `ARRAY` column whose element type is inferred as `INT`, and the datasource is re-ingested
- **THEN** `tags` is `element_type: INT`
