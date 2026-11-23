## ADDED Requirements

### Requirement: A bucket has one logical type on every dialect

A bucket `date_trunc(unit, x)` SHALL be a DATE when `x` is a DATE and `unit` is `day` or coarser (`day`, `week`, `week_sunday`, `month`, `quarter`, `year`, or a custom granularity whose base is one of them), and a TIMESTAMP otherwise (`x` a TIMESTAMP, or `unit` `hour`, `minute`, `second` or a custom granularity with one of those as its base). A sub-day bucket of a DATE SHALL be the TIMESTAMP at that day's midnight. The bucket SHALL have this type in every position — a time dimension, a row value in filters, aggregation sources, computed dimensions and arithmetic, the time spine, `time_shift` and consecutive periods — and SHALL have it physically on every Tier-1 dialect: a DATE bucket is returned as a date (on SQLite `YYYY-MM-DD` text) and a TIMESTAMP bucket as a naive timestamp (on SQLite `YYYY-MM-DD HH:MM:SS` text), never as a zoned timestamp and independent of session settings. A bucket of NULL or, on SQLite, of text that is not a valid date SHALL be NULL. A stage column carrying a bucket SHALL record the bucket's type.

#### Scenario: Month bucket of a DATE column

- **WHEN** a query has the time dimension `date_trunc('month', order_date)` over a DATE column
- **THEN** each value is a date (`2024-06-01`), by executed values on SQLite, DuckDB and Postgres, Postgres with a non-UTC session time zone

#### Scenario: Day bucket of a TIMESTAMP column

- **WHEN** a query has the time dimension `date_trunc('day', created_at)` over a TIMESTAMP column
- **THEN** each value is a timestamp at midnight (`2024-06-01 00:00:00`), on SQLite as `YYYY-MM-DD HH:MM:SS` text

#### Scenario: Hour bucket of a DATE column

- **WHEN** a query has the time dimension `date_trunc('hour', order_date)` over a DATE column
- **THEN** each value is the TIMESTAMP at midnight of the date, and the query plan reports TIMESTAMP

#### Scenario: Custom granularities follow their base

- **WHEN** a datasource defines `fiscal_year` (base `month`) and `quarter_hour` (base `minute`) and a query buckets a DATE column by each
- **THEN** the `fiscal_year` bucket is a DATE and the `quarter_hour` bucket a TIMESTAMP

#### Scenario: A bucket equals its own column at a boundary

- **WHEN** a filter is `date_trunc('day', created_at) = created_at` over a TIMESTAMP column
- **THEN** it keeps exactly the rows at midnight, by executed values on SQLite and DuckDB

#### Scenario: Buckets inside aggregates and date functions

- **WHEN** a query grouped by customer has the filter `max(date_trunc('month', created_at)) >= 'last month'`, and expression dimensions `date_diff('month', date_trunc('month', order_date), created_at)` and `date_add(date_trunc('month', created_at), 1, 'month')`
- **THEN** each binds and returns the values computed from the bucket's type, over DATE and TIMESTAMP sources alike, by executed values on SQLite and DuckDB

#### Scenario: A re-bucketed stage column keeps the bucket's type

- **WHEN** an inner stage projects `date_trunc('month', order_date)` and `date_trunc('hour', order_date)` over a DATE column, and the outer stage re-buckets, compares and applies `date_add` to them
- **THEN** the stage columns record DATE and TIMESTAMP respectively and the outer stage's executed values follow those types

#### Scenario: Sunday weeks

- **WHEN** a query buckets a DATE column and a TIMESTAMP column by `week_sunday`
- **THEN** each bucket starts on the Sunday on or before the value and is a DATE and a TIMESTAMP respectively

## MODIFIED Requirements

### Requirement: Date operands must be temporal
Every date operand (`ts`, `start`, `end`) SHALL have a determinable DATE or TIMESTAMP type. A
temporal operand SHALL be one of: a column (local, joined or stage) declared DATE or TIMESTAMP;
`min`, `max`, `first` or `last` of a temporal value; `date_add`; a bucket `date_trunc(unit, x)` of a
temporal value (typed per › A bucket has one logical type on every dialect); `current_date()` (DATE);
`now()` (TIMESTAMP); an ISO date literal; or `coalesce`, `ifnull`, `nullif`, `greatest`, `least` or an
`iif` / `CASE` whose value arguments are all temporal or NULL (DATE mixed with TIMESTAMP is
TIMESTAMP). Any other operand — a column of another or undeclared type, arithmetic, a number, a
non-date string — SHALL be rejected with a typed query error naming the operand and suggesting a
DATE / TIMESTAMP column type.

#### Scenario: Text column rejected
- **WHEN** a filter uses `date_part('month', status) = 1`
- **THEN** the query is rejected with a type error naming `status`

#### Scenario: Numeric column rejected
- **WHEN** a measure uses `max(date_part('year', amount))`
- **THEN** the query is rejected with a type error naming `amount`

#### Scenario: Aggregate of a timestamp accepted
- **WHEN** a measure uses `date_diff('day', min(created_at), max(created_at))`
- **THEN** it binds and returns the day span of the orders

#### Scenario: Coalesce with the clock accepted
- **WHEN** a measure uses `avg(date_diff('day', created_at, coalesce(shipped_at, now())))`
- **THEN** it binds, treating unshipped orders as shipped now

#### Scenario: Bucket accepted
- **WHEN** a dimension uses `date_add(date_trunc('month', order_date), 1, 'month')`
- **THEN** it binds and returns the start of the following month, a DATE

### Requirement: Date results carry their types
A value whose outermost operation is `date_part` or `date_diff` SHALL report type INT in the query
plan; `date_add` SHALL report DATE or TIMESTAMP per its result type; `date_trunc` DATE or TIMESTAMP per
› A bucket has one logical type on every dialect; `current_date()` DATE; `now()` TIMESTAMP — for
measures and expression dimensions alike.

#### Scenario: Dimension types
- **WHEN** a query has expression dimensions `date_part('month', created_at)` and `date_add(order_date, 1, 'month')`
- **THEN** the query plan reports INT for the first and DATE for the second

#### Scenario: Bucket types
- **WHEN** a query has expression dimensions `date_trunc('month', order_date)` and `date_trunc('hour', order_date)` over a DATE column
- **THEN** the query plan reports DATE for the first and TIMESTAMP for the second
