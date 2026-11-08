## ADDED Requirements

### Requirement: Relations follow dbt's quoting policy

A relation name dbt renders — a semantic model's `model` that has no SQL file, a `ref()` to such a model, a `source()` — and the table a hidden regular model is read from SHALL be read as dbt renders it: each part (database, schema, identifier) quoted when its quoting flag is on, else read with the datasource's identifier folding. The flags SHALL be the dbt adapter's defaults for the datasource dialect — off for Snowflake and Oracle, on for every other dialect — each overridden by the project's `dbt_project.yml` `quoting:` when it sets that flag. A flag value that is not a boolean SHALL be reported and the default used. A per-model `config(quoting=…)` or a per-source `quoting:` SHALL be reported and not applied.

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
- **WHEN** on Postgres the project sets `quoting: {schema: false}` and a model's SQL reads `{{ source('raw', 'orders') }}` declared with `schema: Raw` and `identifier: Orders`
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
