# importers/dbt-import Specification

## Purpose
dbt-specific translation rules: how declared column `data_type`s and semantic-model measure sources become typed SLayer columns.

## Requirements

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

### Requirement: Relations follow dbt's quoting policy

A relation name dbt renders — a semantic model's `model` that has no SQL file, a `ref()` to such a model, a `source()` — and the table a hidden regular model is read from SHALL be read as dbt renders it: each part (database, schema, identifier) quoted when its quoting flag is on, else read with the datasource's identifier folding. The flags SHALL be the dbt adapter's defaults for the datasource dialect — off for Snowflake and Oracle, on for every other dialect. For a model's relation each flag is overridden by the project's `dbt_project.yml` `quoting:` when it sets that flag; for a `source()`, as in dbt, by the source's `quoting:` and then its table's, never by the project's. A flag value that is not a boolean SHALL be reported and ignored. A per-model `config(quoting=…)` SHALL be reported and not applied.

#### Scenario: Postgres default keeps the case
- **WHEN** a Postgres datasource holds `"OrderItems"` with an integer `qty` and the row `(4)` and `orderitems` with a text `qty` and the row `('t')`, and a semantic model's `model` is `ref('OrderItems')` with no SQL file
- **THEN** `qty` is typed `INT` and grouping by it returns `4`

#### Scenario: The project turns identifier quoting off
- **WHEN** the same project's `dbt_project.yml` sets `quoting: {identifier: false}`
- **THEN** `qty` is typed `TEXT` and grouping by it returns `t`

#### Scenario: Snowflake default folds
- **WHEN** on a Snowflake datasource a semantic model's `model` is `ref('OrderItems')` with no SQL file
- **THEN** the SQL SLayer generates reads the table `orderitems`, unquoted

#### Scenario: Snowflake with identifier quoting on
- **WHEN** the same project sets `quoting: {identifier: true}`
- **THEN** the generated SQL reads the table `"OrderItems"`

#### Scenario: Flags apply part by part
- **WHEN** on Postgres the source `raw` declares `quoting: {schema: false}`, `schema: Raw` and a table `orders` with `identifier: Orders`, and a model's SQL reads `{{ source('raw', 'orders') }}`
- **THEN** the model reads the relation `raw."Orders"`

#### Scenario: Project quoting does not reach a source
- **WHEN** on Postgres the project sets `quoting: {identifier: false}` and a model's SQL reads `{{ source('raw', 'orders') }}` declared with `identifier: Orders`
- **THEN** the model reads the relation `raw."Orders"`

#### Scenario: A per-model override is reported
- **WHEN** a regular model's SQL holds `{{ config(quoting={'identifier': false}) }}`
- **THEN** the report holds an entry naming the model and saying the override is not applied, and its relations follow the project policy

#### Scenario: A non-boolean flag is reported
- **WHEN** `dbt_project.yml` sets `quoting: {identifier: "{{ var('q') }}"}`
- **THEN** the report holds an entry naming the flag, and the adapter default applies

### Requirement: source() names the declared relation

`{{ source(g, t) }}` SHALL name the relation the project's `sources:` declares for source `g`, table `t`: the source's `database` when declared, its `schema` (by default the source name `g`) and the table's `identifier` (by default the table name `t`). A `source()` with no declaration SHALL be reported and read as `g.t`. A source declared twice SHALL be reported and its first declaration used.

#### Scenario: Declared schema and identifier
- **WHEN** a source `raw` declares `schema: raw_v2` and a table `orders` with `identifier: orders_tbl`, and a model's SQL reads `{{ source('raw', 'orders') }}`
- **THEN** the model reads the relation `raw_v2.orders_tbl`

#### Scenario: Defaults
- **WHEN** a source `raw` declares only a table `orders`
- **THEN** `{{ source('raw', 'orders') }}` reads the relation `raw.orders`

#### Scenario: Undeclared source
- **WHEN** a model's SQL reads `{{ source('legacy', 'events') }}` and no source `legacy` is declared
- **THEN** the report holds an entry naming `legacy.events`, and the model reads `legacy.events`
