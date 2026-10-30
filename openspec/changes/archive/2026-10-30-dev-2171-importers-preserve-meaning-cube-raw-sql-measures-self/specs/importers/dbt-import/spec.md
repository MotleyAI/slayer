## Purpose

dbt-specific translation rules: how declared column `data_type`s and semantic-model measure sources become typed SLayer columns.

## ADDED Requirements

### Requirement: Declared data_type maps to SLayer types

A `data_type` declared for a column of the dbt model a semantic model reads (in its `schema.yml` or the manifest) SHALL be the declared type of the SLayer column reading that physical column, whether that column is a dimension, an entity key or a measure source, and whether or not hidden regular models are imported. It SHALL be read in the datasource's dialect when known, and SHALL map as: integer types → `INT`; floating, decimal and numeric types → `DOUBLE`; character types → `TEXT`; boolean → `BOOLEAN`; date → `DATE`; timestamp and datetime types (with or without zone) → `TIMESTAMP`; array types → a list whose element type maps by the same rules (`UNKNOWN` when the element type is not one of these or is itself a list). An unrecognised `data_type` SHALL leave the column to the next step of the typing order, with an informational report entry naming it.

#### Scenario: List data types
- **WHEN** columns declare `data_type: array<string>`, `data_type: text[]` and `data_type: ARRAY<INT64>`, and the datasource is not reachable
- **THEN** the columns are lists with element types `TEXT`, `TEXT` and `INT`

#### Scenario: Scalar data types
- **WHEN** columns declare `data_type: bigint`, `numeric(10,2)`, `varchar(255)`, `boolean`, `date` and `timestamp with time zone`, and the datasource is not reachable
- **THEN** the columns are `INT`, `DOUBLE`, `TEXT`, `BOOLEAN`, `DATE` and `TIMESTAMP`

#### Scenario: Unrecognised data type
- **WHEN** a measure-source column declares `data_type: geography`, its measure is `sum`, and the datasource is not reachable
- **THEN** the column is `DOUBLE` and the report holds an informational entry naming `geography`

### Requirement: The datasource is used whenever it resolves

`slayer import-dbt` SHALL resolve its `--datasource` and use it for column typing whenever it is configured and reachable, independent of `--include-hidden-models`.

#### Scenario: Typing without hidden models
- **WHEN** `import-dbt` runs without `--include-hidden-models` against a reachable DuckDB datasource whose `orders.tags` is a text list, and a measure counts `tags`
- **THEN** the measure's source column is a list of text
