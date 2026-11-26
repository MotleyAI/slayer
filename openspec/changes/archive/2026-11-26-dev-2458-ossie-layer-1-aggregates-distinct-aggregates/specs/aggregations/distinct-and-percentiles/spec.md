## Purpose

Defines the DISTINCT aggregates and the discrete and approximate percentiles — their spellings, input and result types,
NULL and empty values — and that every percentile means the same on every Tier-1 database.

## ADDED Requirements

### Requirement: DISTINCT sum and average
The built-in aggregations `sum_distinct(x)` and `avg_distinct(x)` SHALL return the sum and the arithmetic mean of the
distinct non-NULL values of `x` over the aggregate's rows. In Mode B, `SUM(DISTINCT x)` and `AVG(DISTINCT x)` (keywords
in any case, `x` any aggregation source other than `*`) SHALL be exact equivalents of `sum_distinct(x)` and
`avg_distinct(x)`, and `MIN(DISTINCT x)` and `MAX(DISTINCT x)` exact equivalents of `min(x)` and `max(x)` — same result
key, SQL and values. Both SHALL accept numeric inputs, a boolean read as its integer; `sum_distinct` SHALL keep the
source's type and format as `sum` does (a boolean input giving an integer), and `avg_distinct` SHALL be fractional in
the source's units as `avg` is. Over no rows, or rows whose values are all NULL, each SHALL be NULL. Each SHALL take
`FILTER (WHERE …)`, `window=` and `partition_by=` as every aggregation does.

#### Scenario: Values over duplicated inputs
- **WHEN** `orders` holds six rows with `qty` 1, 2, 2, 1, 3 and NULL, and a query asks for `SUM(DISTINCT orders.qty)`, `sum_distinct(orders.qty)`, `AVG(DISTINCT orders.qty)`, `MIN(DISTINCT orders.qty)` and `MAX(DISTINCT orders.qty)`
- **THEN** the values are 6, 6, 2.0, 1 and 3 on DuckDB and SQLite

#### Scenario: Ossie spelling equals the native spelling
- **WHEN** one query asks for `SUM(DISTINCT orders.qty)` and another for `sum_distinct(orders.qty)`
- **THEN** both have the same result key, SQL and values, and `MIN(DISTINCT orders.qty)` has the result key and SQL of `min(orders.qty)`

#### Scenario: Filtered DISTINCT aggregate
- **WHEN** a query asks for `SUM(DISTINCT orders.qty) FILTER (WHERE orders.region = 'EU')`
- **THEN** the value is the sum of the distinct `qty` values of the EU rows, and groups without EU rows are NULL

#### Scenario: All-NULL group
- **WHEN** a query groups by `orders.status` and one status has only NULL `qty`
- **THEN** `sum_distinct(orders.qty)` and `avg_distinct(orders.qty)` are NULL for that status

#### Scenario: Boolean input
- **WHEN** a query asks for `sum_distinct(orders.is_paid)` over a BOOLEAN column holding true and false
- **THEN** the value is the integer 1

#### Scenario: A dimension the home does not determine
- **WHEN** `orders` joins `order_items` one-to-many and a query on `orders` groups `sum_distinct(orders.qty)` by `order_items.product`
- **THEN** each product's value is computed over the orders associated with it, each order counted once, on DuckDB and SQLite

### Requirement: Discrete percentile
The built-in aggregation `percentile_disc(x, p=…)` SHALL return the smallest non-NULL value `v` of `x` over the
aggregate's rows whose cumulative distribution among the non-NULL values is at least `p` — an observed value, of the
type and format of `x`. `p` SHALL be a numeric literal in [0, 1], positional or named, under the rules `percentile`
follows; `p = 0` SHALL give the smallest value. In Mode B, `PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)` SHALL be an
exact equivalent of `percentile_disc(x, p=p)`. `percentile_disc` SHALL accept every ordered type: numeric, text, date,
timestamp and boolean. Over no rows, or rows whose values are all NULL, it SHALL be NULL. It SHALL take
`FILTER (WHERE …)`, `window=` and `partition_by=` as every aggregation does.

#### Scenario: Ossie's value
- **WHEN** `orders.amount` holds 10, 20, 30, 40, 60 and NULL and a query asks for `PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY orders.amount)`
- **THEN** the value is 20

#### Scenario: Endpoints and duplicates
- **WHEN** a query asks for `percentile_disc(x, 0)`, `percentile_disc(x, 0.5)` and `percentile_disc(x, 1)` over the values 5, 5, 7, 9
- **THEN** the values are 5, 5 and 9

#### Scenario: Positional p equals named
- **WHEN** a measure is written `percentile_disc(price, 0.9)`
- **THEN** its SQL, result key and values equal those of `percentile_disc(price, p=0.9)`

#### Scenario: Text and date inputs
- **WHEN** a query asks for `percentile_disc(orders.status, 0.5)` and `percentile_disc(orders.order_date, 0.5)`
- **THEN** each returns an observed status and an observed date, typed TEXT and DATE

#### Scenario: Invalid p
- **WHEN** a measure is `percentile_disc(amount, 1.5)`, `percentile_disc(amount, -0.1)`, `percentile_disc(amount, NULL)`, `percentile_disc(amount, 'x')`, `percentile_disc(amount, TRUE)`, `percentile_disc(amount, amount)` or `percentile_disc(amount, 0.5, p=0.5)`
- **THEN** each is refused with a typed error naming `p` — when a model holding it is saved, and when a query reads it

#### Scenario: A dimension the home does not determine
- **WHEN** `orders` joins `order_items` one-to-many and a query on `orders` groups `percentile_disc(orders.amount, 0.5)` by `order_items.product`
- **THEN** each product's value is computed over the orders associated with it, each order counted once, on DuckDB and SQLite

### Requirement: Approximate percentile
The built-in aggregation `percentile_approx(x, p=…)` SHALL return an approximation of `percentile(x, p=…)` over numeric
inputs: the database's own approximate percentile where it has one taking an arbitrary `p`, and the exact `percentile`
everywhere else. `p` SHALL follow the rules of `percentile`. Its result SHALL be the float type `percentile` gives, on
every database. In Mode B, `APPROX_PERCENTILE(x, p)` SHALL be an exact equivalent of `percentile_approx(x, p=p)`. Over
no rows, or rows whose values are all NULL, it SHALL be NULL.

#### Scenario: Within tolerance on every Tier-1 database
- **WHEN** `APPROX_PERCENTILE(orders.amount, 0.5)` runs over the amounts 10, 20, 30, 40, 60 and NULL on each Tier-1 database
- **THEN** each value is within 1 of 30

#### Scenario: Exact where the database has no approximate percentile
- **WHEN** `percentile_approx(amount, 0.9)` runs on SQLite, Postgres, MySQL and BigQuery
- **THEN** each value equals `percentile(amount, 0.9)` there

#### Scenario: Float result over integer and decimal inputs
- **WHEN** `percentile_approx` runs over an INTEGER and a DECIMAL column on each database with a native approximate percentile
- **THEN** each result column has the float type `percentile` gives

### Requirement: Percentiles are exact on every Tier-1 database
`percentile`, `median` and `percentile_disc` SHALL return the exact value their definition gives on every Tier-1
database — SQLite, DuckDB, Postgres, MySQL, ClickHouse, SQL Server, Trino, Snowflake and BigQuery — on groups of any
size, and NULL over no rows or all-NULL rows; where a database has no exact translation of one for an input type, the
aggregation SHALL be refused with a typed error naming the aggregation and the database, never answered approximately.

#### Scenario: Large group on ClickHouse
- **WHEN** `percentile(x, 0.9)` and `median(x)` run on ClickHouse over a group of 10 000 rows
- **THEN** each equals the interpolated value Postgres returns over the same rows

#### Scenario: Empty and all-NULL groups on ClickHouse
- **WHEN** `percentile(x, 0.5)`, `median(x)` and `percentile_disc(x, 0.5)` run on ClickHouse over no rows, and over a group whose `x` values are all NULL, for a nullable and a non-nullable numeric `x` and for a date `x`
- **THEN** each value is NULL

#### Scenario: Every database agrees on the discrete percentile
- **WHEN** `percentile_disc(x, p)` runs on each Tier-1 database grouped and ungrouped, with NULL values, duplicates, `p` of 0 and 1, two percentiles in one query and a `FILTER`
- **THEN** every database returns the same values

#### Scenario: Text and date on every database
- **WHEN** `percentile_disc` runs over a text and a date column on each Tier-1 database
- **THEN** every database returns the same observed values

### Requirement: Ordered-set and DISTINCT spellings
In Mode B, `WITHIN GROUP (ORDER BY x)` (with an optional `ASC`) SHALL follow only `PERCENTILE_CONT(p)` and
`PERCENTILE_DISC(p)`; any other function before it, a `DESC`, or a fraction that is not exactly one argument SHALL be
rejected with an error naming the two accepted forms. A leading `DISTINCT` SHALL be accepted only in `count`, `sum`,
`avg`, `min`, `max` and `array_agg` over one operand other than `*`; any other `DISTINCT` SHALL be rejected with an
error listing the accepted forms. A DISTINCT or ordered-set aggregate written with `OVER (…)` SHALL be rejected with a
typed error.

#### Scenario: Unsupported ordered-set function
- **WHEN** a measure is `MODE() WITHIN GROUP (ORDER BY status)` or `PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount DESC)`
- **THEN** each is rejected naming `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` and `PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x)`

#### Scenario: Unsupported DISTINCT
- **WHEN** a measure is `stddev(DISTINCT amount)`, `sum(DISTINCT *)` or `sum(DISTINCT a, b)`
- **THEN** each is rejected with the error listing the accepted DISTINCT forms

#### Scenario: DISTINCT inside a window
- **WHEN** a measure is `SUM(DISTINCT amount) OVER (PARTITION BY region)`
- **THEN** it is rejected with a typed error

### Requirement: Default aggregations and types of the new built-ins
A field that declares no `allowed_aggregations` SHALL admit `sum_distinct`, `avg_distinct`, `percentile_disc` and
`percentile_approx` when numeric or boolean, and `percentile_disc` when text, date or timestamp. A sole primary key SHALL
admit none of them. A model's own aggregation named exactly like one of them SHALL keep answering in its model, as for
every built-in name.

#### Scenario: Text field defaults
- **WHEN** a text field with no `allowed_aggregations` is queried with `percentile_disc(name, 0.5)` and `sum_distinct(name)`
- **THEN** the first answers and the second is refused naming the field and the aggregation

#### Scenario: Boolean field defaults
- **WHEN** a boolean field with no `allowed_aggregations` is queried with `percentile_disc(is_paid, 0.5)`
- **THEN** it answers with an observed boolean

#### Scenario: A model's own sum_distinct
- **WHEN** model `t` defines the aggregation `sum_distinct` with formula `SUM({value}) * 100` and a query asks for `sum_distinct(a)`
- **THEN** the measure is the model's aggregation

#### Scenario: A stored granularity named like a new built-in
- **WHEN** a datasource stored before this release defines the custom granularity `percentile_disc`, used by a stored model's time dimension
- **THEN** both load, and the dimension buckets by the granularity renamed `percentile_disc_granularity`
