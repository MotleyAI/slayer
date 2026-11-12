# facade/projection Specification

## Purpose
Defines what a plain projection through the BI wire facades (Postgres wire, Arrow Flight) returns, and guarantees
that every column a facade catalog advertises can be selected as advertised.

## Requirements

### Requirement: A plain projection returns one row per record

A facade `SELECT` whose select list has no aggregate and no metric, and which has no `GROUP BY`, no `HAVING`, no
`DISTINCT` and no measure reference in `WHERE`, SHALL return one row per record of its table (joined
rows included), as SQL does. A `SELECT DISTINCT`, a `GROUP BY`, a `HAVING`, an aggregate or metric in the select
list, or a measure reference in `WHERE` SHALL return grouped rows, one per distinct combination of
the projected dimensions. A time-grain projection such as `date_trunc('month', ordered_at)`, and an extraction
projection such as `month(ordered_at)`, SHALL follow the same rule: one value per record in a plain projection, one
per distinct value when grouped.

#### Scenario: Plain projection keeps duplicates
- **WHEN** a client runs `SELECT status FROM orders` over 10 orders with 3 distinct statuses
- **THEN** 10 rows are returned

#### Scenario: DISTINCT groups
- **WHEN** a client runs `SELECT DISTINCT status FROM orders` or `SELECT status FROM orders GROUP BY status`
- **THEN** 3 rows are returned

#### Scenario: Measure in WHERE groups
- **WHEN** a client runs `SELECT status FROM orders WHERE revenue_sum > 100`
- **THEN** the rows are grouped by `status` and filtered on the aggregated `revenue_sum`

#### Scenario: Time grain without an aggregate
- **WHEN** a client runs `SELECT date_trunc('month', ordered_at) FROM orders` over 10 orders in 2 months
- **THEN** 10 rows are returned, each the month of its order; with `DISTINCT` 2 rows are returned

#### Scenario: Ordering and paging a plain projection
- **WHEN** a client runs `SELECT name, skills FROM contractors ORDER BY name LIMIT 2 OFFSET 1`
- **THEN** the second and third contractors by name are returned, one row each

### Requirement: Every advertised facade column is selectable

Every column a facade catalog advertises — in `information_schema.columns`, `pg_attribute`, Flight `GetTables`
table schemas and `INFORMATION_SCHEMA.DIMENSIONS`, including dotted joined dimensions — SHALL be selectable alone
with a plain `SELECT <column> FROM <table>` and SHALL return its values, with the advertised wire type. A dimension
reached through a fanning join SHALL return one row per joined record. A list column SHALL also be groupable, one
group per element set.

#### Scenario: Every column of a mixed-type model
- **WHEN** a model has a list column, a JSON column, a UUID column, a decimal column, a bytes column, an interval
  column and joins to a to-one and a to-many model, and each advertised column is selected alone over the Postgres
  wire and over Arrow Flight
- **THEN** every selection succeeds and returns the column's values

#### Scenario: BI table preview
- **WHEN** a BI tool previews `contractors`, whose `skills` is a list, with `SELECT id, name, skills FROM contractors`
- **THEN** one row per contractor is returned, with `skills` as its stored list's JSON array text and a NULL list as NULL

#### Scenario: Grouping on a list
- **WHEN** a client runs `SELECT skills, COUNT(*) FROM contractors GROUP BY skills` over contractors whose `skills`
  are `['go', 'sql', 'go']`, `['sql', 'go']` and NULL
- **THEN** two rows are returned: `["go","sql"]` with 2, and NULL with 1

#### Scenario: Ordering a grouped list
- **WHEN** a client runs `SELECT skills, COUNT(*) FROM contractors GROUP BY skills ORDER BY skills`, or orders by the
  ordinal `1`
- **THEN** it fails with an error naming the list type

### Requirement: Date-part projections are extraction
In a facade `SELECT`, `YEAR(col)`, `QUARTER(col)`, `MONTH(col)`, `WEEK(col)`, `DAY(col)`, `HOUR(col)`, `MINUTE(col)` and
`SECOND(col)` (any case), `EXTRACT(<part> FROM col)` and `date_part('<part>', col)` SHALL project the integer extraction
`date_part('<part>', col)` of `queries/date-functions`, as a grouped dimension under one canonical alias shared by every
spelling, which `GROUP BY` and `ORDER BY` SHALL resolve by expression, alias or ordinal. `DATE_TRUNC('<unit>', col)`,
`TIMESTAMP_TRUNC` and the Metabase Sunday-week wrapper SHALL keep projecting time buckets. In `WHERE`, `MONTH(col)` and
its siblings SHALL mean extraction.

#### Scenario: Month of year
- **WHEN** a client runs `SELECT MONTH(ordered_at), COUNT(*) FROM orders GROUP BY 1 ORDER BY 1` over orders in March 2024 and March 2025
- **THEN** one row is returned for month 3 counting both years' orders

#### Scenario: Spellings agree
- **WHEN** a client runs the same query with `EXTRACT(MONTH FROM ordered_at)` and with `date_part('month', ordered_at)`
- **THEN** each returns the same rows under the same column name

#### Scenario: DATE_TRUNC keeps the bucket
- **WHEN** a client runs `SELECT DATE_TRUNC('month', ordered_at), COUNT(*) FROM orders GROUP BY 1`
- **THEN** one row is returned per month bucket, as before

#### Scenario: Extraction in WHERE
- **WHEN** a client runs `SELECT COUNT(*) FROM orders WHERE MONTH(ordered_at) = 3`
- **THEN** it counts the March orders of every year
