## Purpose

Defines how an aggregation over a boolean-valued input — a boolean column, a boolean
expression, a predicate, a boolean stage column or an attached boolean value — is computed,
typed and formatted, identically on every dialect.

## ADDED Requirements

### Requirement: Boolean-valued inputs are recognised by their value, never their spelling
An aggregation input SHALL be boolean-valued when its value is certainly boolean: a column
declared BOOLEAN (host, joined or stage column, base or derived), a boolean literal, a
comparison, a boolean connective (`and` / `or` / `not`), an `IN` / `NOT IN` or `BETWEEN`
predicate, `like`, an `iif` whose every branch is boolean-valued, a `coalesce` / `ifnull` /
`greatest` / `least` whose every argument is boolean-valued, a `nullif` whose first argument is
boolean-valued, and a `min` / `max` / `first` / `last` over a boolean-valued source. An input
with any non-boolean returnable branch SHALL NOT be boolean-valued. The same recognition SHALL
drive binding gates, result typing and SQL emission.

#### Scenario: Mixed-type coalesce is not boolean
- **WHEN** a measure is written `sum(coalesce(flag, amount))` with `flag` BOOLEAN and `amount` INT
- **THEN** it is aggregated as a plain numeric expression, with no boolean lowering and no
  boolean typing

#### Scenario: nullif follows its first argument
- **WHEN** a measure is written `sum(nullif(flag, false))`
- **THEN** the input is boolean-valued and aggregated as in the lowering requirement below

### Requirement: Numeric aggregations lower a boolean input to an integer
`sum`, `avg`, `min` and `max` over a boolean-valued input SHALL aggregate the input's integer
value — true as 1, false as 0, NULL as NULL — so NULL inputs are ignored as for any aggregate.
`min` and `max` SHALL return a BOOLEAN, converted back within the aggregate expression itself,
so the aggregate is boolean wherever it appears (projection, post-aggregation filter, ordering,
arithmetic). The count family (`count`, `count_distinct`, `count_distinct_approx`), `first`,
`last` and custom aggregations SHALL receive the boolean input unchanged. A boolean-valued
input SHALL remain boolean in every non-aggregate position (dimension, row filter).

#### Scenario: Sum counts the trues
- **WHEN** a query over rows whose `flag` values are `true, true, false, NULL, false` selects
  `flag:sum`
- **THEN** it returns 2 on SQLite, DuckDB and Postgres — never `true`, and never a database error

#### Scenario: Avg is the share of true rows
- **WHEN** the same query selects `flag:avg`
- **THEN** it returns 0.5 (two trues among four non-NULL rows) on SQLite, DuckDB and Postgres

#### Scenario: Min and max return booleans
- **WHEN** the same query selects `flag:min` and `flag:max`
- **THEN** it returns `false` and `true` on DuckDB and Postgres (`0` and `1` on SQLite, which has
  no boolean type), never a database error

#### Scenario: Post-aggregation filters compare booleans and counts
- **WHEN** a query filters on `flag:max = true` or on `flag:sum > 1`
- **THEN** each filter applies after aggregation and executes on Postgres, keeping exactly the
  groups whose value satisfies it

#### Scenario: Counting keeps the boolean
- **WHEN** a query selects `flag:count` and `flag:count_distinct` over the same rows
- **THEN** it returns 4 and 2 — false values counted, NULL not

#### Scenario: Emitted SQL on every dialect
- **WHEN** SQL is generated for `flag:sum`, `flag:avg`, `flag:min` and `flag:max` on each
  supported dialect
- **THEN** each aggregate takes the integer form of `flag` as its input, `min` / `max` convert
  the aggregate back to the dialect's boolean type, and no aggregate receives the raw boolean

### Requirement: Boolean aggregation result types and formats
The result of an aggregation over a boolean-valued input SHALL be typed and formatted as:
`sum` → INT with INTEGER format; `avg` → DOUBLE with PERCENT format unless the source column
declares a format, which wins; `min` / `max` → BOOLEAN; the count family → INT; `first` /
`last` and custom aggregations → the source type. Every surface that reports an aggregate's
type — query response metadata, stage schemas consumed by a later stage, and the SQL facade's
metric catalogue — SHALL report the same type for the same column and aggregation.

#### Scenario: Sum is an integer measure
- **WHEN** a query selects `flag:sum`
- **THEN** the measure's response type is INT and its format INTEGER, and no cast to BOOLEAN
  wraps the aggregate

#### Scenario: Avg displays as a percentage
- **WHEN** a query selects `flag:avg` and `flag` declares no format
- **THEN** the measure's format is PERCENT

#### Scenario: Explicit column format wins
- **WHEN** `flag` declares a FLOAT format and a query selects `flag:avg`
- **THEN** the measure's format is that FLOAT format

#### Scenario: Facade agrees with the engine
- **WHEN** the SQL facade lists the metrics of a model with a BOOLEAN, an INT, a DOUBLE, a TEXT
  and a TIMESTAMP column
- **THEN** every metric's data type equals the type the query engine assigns the same column and
  aggregation, including custom aggregations (the source type)

### Requirement: Boolean defaults include avg
A BOOLEAN column with no explicit allowed-aggregations list SHALL accept `count`,
`count_distinct`, `count_distinct_approx`, `sum`, `avg`, `min`, `max`, `first` and `last`. A
boolean-valued expression source SHALL accept every aggregation in that set (subject to the
existing `first` / `last`-over-an-expression rejection) and SHALL reject other numeric-only
aggregations at binding with a typed error.

#### Scenario: Avg allowed by default
- **WHEN** a model declares a BOOLEAN column without `allowed_aggregations` and a query selects
  `flag:avg`
- **THEN** the model validates and the query succeeds

#### Scenario: Boolean expression accepts sum
- **WHEN** a measure is written `sum(coalesce(flag, false))`
- **THEN** it binds and returns the number of true rows

#### Scenario: Statistical aggregation over a boolean rejected
- **WHEN** a measure is written `median(flag)` or `median(amount > 15)`
- **THEN** it fails at binding with a typed error, never in the database

### Requirement: Predicates are aggregatable booleans
A comparison, boolean connective, `IN` / `NOT IN` or `BETWEEN` predicate SHALL be a legal
aggregation source — over row-level values and over attached (aggregated) values alike — and
SHALL be aggregated exactly as a boolean column with the same values would be.

#### Scenario: Row-level comparison
- **WHEN** a query over rows with `amount` values `10, 20, 30, NULL` selects `sum(amount > 15)`,
  `avg(amount > 15)` and `count(amount > 15)`
- **THEN** it returns 2, 0.6667 (two of three non-NULL) and 3 on SQLite, DuckDB and Postgres

#### Scenario: Row-level IN and BETWEEN
- **WHEN** a measure is written `sum(status in ('ok', 'hold'))` or
  `sum(amount between 10 and 20)`
- **THEN** it binds without an internal validation error and returns the number of rows
  satisfying the predicate

#### Scenario: Predicate over an attached value
- **WHEN** a measure is written `sum(count(amount) in (1, 2))`,
  `sum(count(amount) between 1 and 2)` or `sum(sum(amount) > 45 and sum(amount) < 100)` over
  a per-entity grain
- **THEN** it binds without an internal validation error and returns the number of grain cells
  satisfying the predicate on SQLite and DuckDB

### Requirement: Boolean inputs compose through every aggregation shape
The lowering and typing above SHALL hold for every shape an aggregation takes: a cross-model
source, a stage column consumed by a later stage, a re-aggregation, a `partition_by` or
`window=` aggregate, a transform over the aggregate, and the association producer that picks one
value per entity.

#### Scenario: Cross-model boolean sum
- **WHEN** a query rooted at `customers` selects `sum(orders.flag)`
- **THEN** each customer's value is its count of flagged orders

#### Scenario: Stage re-aggregation
- **WHEN** a first stage selects `flag:sum` and `flag:max` per customer and a second stage
  selects `sum` and `max` over those columns
- **THEN** the second stage returns the total count and the overall boolean maximum

#### Scenario: Transform over a boolean sum
- **WHEN** a query over a month time dimension selects `cumsum(flag:sum)`
- **THEN** each month carries the running count of trues

#### Scenario: Windowed and partitioned boolean sums
- **WHEN** a query selects `sum(flag, window='90d')` over a month time dimension and
  `sum(flag, partition_by=region)`
- **THEN** each returns the count of trues over its window or partition

#### Scenario: Association pick over a boolean
- **WHEN** a boolean aggregate is computed by the association producer under
  `to_many_handling: "associate"`
- **THEN** each entity's boolean is picked once and aggregated as above, executing on Postgres
