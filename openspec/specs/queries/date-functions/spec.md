# queries/date-functions Specification

## Purpose
Portable Mode-B date and time functions — extraction, differences, arithmetic and the
current date/time — that render to one pinned meaning on every supported dialect.

Scenarios below use a model `orders` with columns `id` (INT, primary key),
`order_date` (DATE), `created_at` (TIMESTAMP), `shipped_at` (TIMESTAMP, nullable),
`amount` (DOUBLE), `status` (TEXT), `sla_days` (DOUBLE).

## Requirements

### Requirement: Date functions are Mode-B scalars
The Mode-B scalar allowlist SHALL include `date_part(part, ts)`, `date_diff(unit, start, end)`,
`date_add(ts, n, unit)`, `current_date()` and `now()`, matched case-insensitively and usable in
every Mode-B position (measure formulas, filters, expression dimensions, order keys). Each SHALL
take exactly its listed number of arguments (`current_date` / `now` take none); a call with any
other argument count, or with keyword arguments, SHALL be rejected naming the function. The
one-argument calls `year(x)`, `quarter(x)`, `month(x)`, `week(x)`, `day(x)`, `hour(x)`, `minute(x)`
and `second(x)` SHALL be extraction (› Date-part aliases mean their date_part); a time bucket SHALL
be written `date_trunc(unit, x)` (› date_trunc is the granularity call).

#### Scenario: Extraction as an expression dimension
- **WHEN** a query `{"population": "orders", "dimensions": [{"field": "date_part('day_of_week', created_at)", "alias": "dow"}], "metrics": [{"metric": "count(*)"}]}` executes
- **THEN** the result has one row per weekday with key `dow` holding an integer 1–7

#### Scenario: Date functions in a filter
- **WHEN** a query on `orders` has filter `"date_diff('day', created_at, shipped_at) > 3"`
- **THEN** only orders shipped on a calendar day more than three days after creation are counted

#### Scenario: Case-insensitive names
- **WHEN** a filter spells `DATE_DIFF('day', created_at, shipped_at) > 3`
- **THEN** it binds to the same value as the lowercase spelling

#### Scenario: Wrong argument count
- **WHEN** a filter uses `date_add(created_at, 3)`
- **THEN** the query is rejected with an error naming `date_add` and its argument count

#### Scenario: Zero-argument clock calls
- **WHEN** a filter uses `created_at <= now()` or `order_date <= current_date()`
- **THEN** both bind and execute; `now(1)` is rejected for its argument count

#### Scenario: Bucket form unchanged
- **WHEN** a query lists `"date_trunc('month', created_at)"` in `dimensions`
- **THEN** it is still the month time bucket, not a month number

#### Scenario: Month call is month of year
- **WHEN** a query lists `"month(created_at)"` in `dimensions` with measure `count(*)`
- **THEN** it groups by the month number 1–12 (rows of every year's March share one group), not by month bucket

### Requirement: Unit and part arguments are closed string literals
The `part` argument of `date_part` and the `unit` argument of `date_diff` / `date_add` SHALL be a
string literal (the `part` of `date_part` may instead be a bare word, below), matched
case-insensitively, from that function's closed set:
- `date_part`: `year`, `iso_year`, `quarter`, `month`, `week`, `day`, `day_of_week`,
  `day_of_year`, `hour`, `minute`, `second`, `millisecond`;
- `date_diff` and `date_add`: `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`,
  `quarter`, `year`.
The `part` of `date_part` MAY also be written as a bare word from that set or from the date-part
aliases, as `EXTRACT` takes it (Ossie's `DATE_PART(MONTH, x)`), with the same meaning as its quoted
spelling.
Any other value, and any non-literal (a column, a `{variable}` that does not substitute to a listed
word, an expression), SHALL be rejected before binding with an error listing the accepted values.
Differently-cased spellings of the same unit SHALL intern to the same value.

#### Scenario: Unknown unit rejected
- **WHEN** a filter uses `date_diff('fortnight', created_at, shipped_at) > 1`
- **THEN** the query is rejected with an error listing the accepted `date_diff` units

#### Scenario: Extraction-only part rejected as a difference unit
- **WHEN** a filter uses `date_diff('day_of_week', created_at, shipped_at) > 1`, and another uses `date_add(created_at, 5, 'millisecond')`
- **THEN** each query is rejected with an error listing the accepted units of its function

#### Scenario: Column as a unit rejected
- **WHEN** a filter uses `date_part(status, created_at) = 1`
- **THEN** the query is rejected because the part must be a string literal

#### Scenario: Unit case folds
- **WHEN** one measure uses `date_part('MONTH', created_at)` and another `date_part('month', created_at)`
- **THEN** both resolve to the same value

#### Scenario: Bare part word
- **WHEN** one measure uses `date_part(MONTH, created_at)` and another `date_part('month', created_at)`
- **THEN** both resolve to the same value

### Requirement: date_part extracts calendar components with ISO week conventions
`date_part(part, ts)` SHALL return an integer: `year`, `quarter` (1–4), `month` (1–12), `day`
(day of month), `day_of_year` (1–366), `hour` (0–23), `minute`, `second` (whole seconds),
`millisecond` (the millisecond component: the fractional second truncated to whole milliseconds,
0–999, never rounded and never including the whole seconds);
`day_of_week` SHALL be ISO-8601 numbering, Monday = 1 through Sunday = 7, whatever the database's
session settings; `week` SHALL be the ISO-8601 week number (weeks start Monday, week 1 contains
January 4th) and `iso_year` the ISO year that week belongs to. Parts finer than a day on a DATE
operand SHALL return 0. Every part SHALL return the same value on every Tier-1 dialect.

#### Scenario: Sunday is day 7
- **WHEN** `date_part('day_of_week', '2024-06-02')` (a Sunday) is evaluated
- **THEN** the result is 7, and for `'2024-06-03'` (a Monday) it is 1

#### Scenario: ISO week across a year boundary
- **WHEN** `date_part('week', '2024-12-30')` and `date_part('iso_year', '2024-12-30')` are evaluated
- **THEN** the results are 1 and 2025, while `date_part('year', '2024-12-30')` is 2024

#### Scenario: Hour of a DATE
- **WHEN** `date_part('hour', order_date)` is evaluated
- **THEN** the result is 0 for every row

#### Scenario: Millisecond is the truncated component
- **WHEN** `date_part('millisecond', created_at)` and `date_part('second', created_at)` are evaluated over rows holding `2024-12-30 10:15:42.123`, `2024-12-30 10:15:42.001999`, `2024-12-30 10:15:42.999999`, `2024-12-30 10:15:42` and `1969-07-20 20:17:40.5`
- **THEN** the milliseconds are 123, 1, 999, 0 and 500 and the seconds 42, 42, 42, 42 and 40, on every Tier-1 dialect

#### Scenario: Millisecond of a DATE
- **WHEN** `date_part('millisecond', order_date)` is evaluated
- **THEN** the result is 0 for every row

### Requirement: date_diff counts calendar boundaries crossed
`date_diff(unit, start, end)` SHALL return the integer number of `unit` boundaries crossed going
from `start` to `end`: the difference between `end` and `start` each truncated to `unit`,
expressed in whole units; it SHALL be negative when `end` precedes `start` and 0 when both fall in
the same unit. `week` boundaries are Mondays, `week_sunday` boundaries are Sundays; `quarter` and
`month` boundaries are the first day of a quarter / month; `day` boundaries are midnights; `hour`,
`minute` and `second` boundaries are the starts of those units. When one operand is a DATE and the
other a TIMESTAMP, the DATE SHALL be treated as that day's midnight.

#### Scenario: Month boundary one day apart
- **WHEN** `date_diff('month', '2024-01-31', '2024-02-01')` is evaluated
- **THEN** the result is 1

#### Scenario: Same month
- **WHEN** `date_diff('month', '2024-02-01', '2024-02-29')` is evaluated
- **THEN** the result is 0

#### Scenario: Year boundary
- **WHEN** `date_diff('year', '2024-12-31', '2025-01-01')` is evaluated
- **THEN** the result is 1

#### Scenario: Reversed operands are negative
- **WHEN** `date_diff('month', '2024-02-01', '2024-01-31')` is evaluated
- **THEN** the result is -1

#### Scenario: Day boundary on timestamps
- **WHEN** `date_diff('day', '2024-03-01 23:59:00', '2024-03-02 00:01:00')` is evaluated
- **THEN** the result is 1

#### Scenario: Monday and Sunday weeks
- **WHEN** `date_diff('week', '2024-06-02', '2024-06-03')` (Sunday to Monday) is evaluated
- **THEN** the result is 1, and `date_diff('week_sunday', '2024-06-02', '2024-06-03')` is 0

#### Scenario: Mixed DATE and TIMESTAMP
- **WHEN** `date_diff('hour', order_date, created_at)` is evaluated for a row with `order_date` 2024-03-01 and `created_at` 2024-03-01 05:30:00
- **THEN** the result is 5

### Requirement: date_add offsets by calendar units and clamps at month-end
`date_add(ts, n, unit)` SHALL return `ts` moved by `n` units (backward when `n` is negative).
`week` and `week_sunday` SHALL move by 7 days per unit and `quarter` by 3 months per unit. A
`month`, `quarter` or `year` move that lands on a day the target month does not have SHALL clamp
to that month's last day. The time of day SHALL be preserved. The result SHALL be a DATE when
`ts` is a DATE and `unit` is `day` or coarser, and a TIMESTAMP otherwise.

#### Scenario: Month-end clamp forward
- **WHEN** `date_add('2024-01-31', 1, 'month')` is evaluated
- **THEN** the result is the DATE 2024-02-29

#### Scenario: Month-end clamp backward
- **WHEN** `date_add('2024-03-31', -1, 'month')` is evaluated
- **THEN** the result is the DATE 2024-02-29

#### Scenario: Leap day plus a year
- **WHEN** `date_add('2024-02-29', 1, 'year')` is evaluated
- **THEN** the result is the DATE 2025-02-28

#### Scenario: Sub-day unit on a DATE
- **WHEN** `date_add(order_date, 2, 'hour')` is evaluated for `order_date` 2024-03-01
- **THEN** the result is the TIMESTAMP 2024-03-01 02:00:00

#### Scenario: Time of day kept
- **WHEN** `date_add('2024-01-31 10:15:00', 1, 'month')` is evaluated
- **THEN** the result is the TIMESTAMP 2024-02-29 10:15:00

### Requirement: date_add counts are integers
A literal count SHALL be an integral number (negative allowed); a fractional literal, a boolean,
a NULL literal or a string literal SHALL be rejected naming `date_add`. A computed count (a column,
arithmetic, an aggregate) SHALL be truncated toward zero before it is applied, identically on
every dialect; a computed count whose type is known to be non-numeric (a TEXT, BOOLEAN, DATE or
TIMESTAMP column, or a date-valued expression) SHALL be rejected. A computed count that evaluates
to NULL SHALL yield NULL.

#### Scenario: Computed count
- **WHEN** a query projects `date_add(order_date, sla_days, 'day')` for a row with `sla_days` 2.7 and `order_date` 2024-03-01
- **THEN** the result is 2024-03-03, and for `sla_days` -2.7 it is 2024-02-28

#### Scenario: Fractional literal rejected
- **WHEN** a filter uses `date_add(order_date, 1.5, 'day') > '2024-01-01'`
- **THEN** the query is rejected naming `date_add`

#### Scenario: Non-numeric count rejected
- **WHEN** a filter uses `date_add(order_date, status, 'day') > '2024-01-01'`
- **THEN** the query is rejected naming `date_add`

#### Scenario: NULL computed count
- **WHEN** `date_add(order_date, sla_days, 'day')` is evaluated for a row with `sla_days` NULL
- **THEN** the result is NULL

### Requirement: The interval spelling is date_add
`x + interval(n, unit)`, `interval(n, unit) + x` and `x - interval(n, unit)` SHALL mean exactly
`date_add(x, n, unit)` (for subtraction, `date_add(x, -n, unit)`) and SHALL resolve to the same
value as that call, sharing its result key and SQL. The SQL interval literals `INTERVAL '<n> <unit>'`,
`INTERVAL '<n>' <unit>` and `INTERVAL <n> <unit>` (one unit; keyword and unit case-insensitive; a
unit's plural form accepted) SHALL mean `interval(n, unit)` in all of these positions. Chains SHALL
apply left to right. `interval` SHALL follow the same count and unit rules as `date_add`. An interval
in any other position — standalone, as the left side of `-`, negated, multiplied, compared, nested in
another interval, added to another interval, or as a function argument — SHALL be rejected before
binding with an error pointing to `date_add`, and an interval literal naming more than one unit
(`INTERVAL '1 day 2 hours'`) SHALL be rejected with an error suggesting a chain.

#### Scenario: Operator spelling equals date_add
- **WHEN** one measure uses `max(shipped_at - interval(2, 'day'))` and another `max(date_add(shipped_at, -2, 'day'))`
- **THEN** both resolve to the same value and render identical SQL

#### Scenario: Chained intervals
- **WHEN** a filter uses `created_at + interval(1, 'month') + interval(2, 'hour') < now()`
- **THEN** it binds as `date_add(date_add(created_at, 1, 'month'), 2, 'hour') < now()`

#### Scenario: Commuted addition
- **WHEN** a filter uses `interval(7, 'day') + order_date > current_date()`
- **THEN** it binds as `date_add(order_date, 7, 'day') > current_date()`

#### Scenario: Stray interval rejected
- **WHEN** a filter uses `interval(7, 'day') - order_date > 0`, `-interval(7, 'day') + order_date > order_date`, or `order_date + interval(1, 'day') * 2 > order_date`
- **THEN** each is rejected with an error pointing to `date_add`

#### Scenario: SQL interval literals equal date_add
- **WHEN** filters use `order_date + INTERVAL '7 days' > CURRENT_DATE`, `order_date + INTERVAL '7' DAY > CURRENT_DATE` and `order_date + INTERVAL 7 DAY > CURRENT_DATE`
- **THEN** each binds as `date_add(order_date, 7, 'day') > current_date()`

#### Scenario: Multi-unit interval literal rejected
- **WHEN** a filter uses `order_date + INTERVAL '1 day 2 hours' > now()`
- **THEN** it is rejected with an error suggesting `+ INTERVAL '1 day' + INTERVAL '2 hours'`

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

### Requirement: ISO literals are typed date values in date positions
A string literal in a temporal operand position — directly, or as a value argument of a
`coalesce`, `ifnull`, `nullif`, `greatest`, `least` or `iif` / `CASE` in such a position — SHALL
bind as a date value: `'YYYY-MM-DD'` as a DATE and an instant in the `queries/time-points` grammar
(`YYYY-MM-DD HH:MM[:SS[.fraction]]`, space or `T` separator) as a TIMESTAMP. A string in such a
position that is not one of those shapes or not a real calendar value SHALL be rejected with a typed
query error. `{variable}` placeholders SHALL work in these positions. Outside date positions a string
literal keeps its string meaning, except a time point compared with a temporal operand
(`queries/time-points`).

#### Scenario: Literal anchor
- **WHEN** a filter uses `date_diff('day', '2024-01-01', created_at) < 30`
- **THEN** it counts orders created in the first 30 calendar days of 2024 (and earlier)

#### Scenario: Literal inside coalesce
- **WHEN** a measure uses `min(date_part('year', coalesce(shipped_at, '2024-01-01')))`
- **THEN** it binds, and unshipped orders contribute 2024

#### Scenario: Invalid calendar value
- **WHEN** a filter uses `date_diff('day', '2024-02-30', created_at) > 0`
- **THEN** the query is rejected with a type error naming the literal

#### Scenario: Placeholder in a date position
- **WHEN** a filter `date_diff('day', '{launch}', created_at) >= 0` runs with variables `{"launch": "2024-05-01"}`
- **THEN** it counts orders created on or after 2024-05-01

#### Scenario: Minute-precision instant
- **WHEN** a dimension uses `date_add('2024-01-31 10:15', 1, 'month')`
- **THEN** it is the TIMESTAMP `2024-02-29 10:15:00`

### Requirement: Clock functions read the database clock
Without a time zone (`queries/time-zones`), `current_date()` SHALL return the database's current
date as a DATE and `now()` its current timestamp as a TIMESTAMP, evaluated by the database at
execution time (on SQLite, in UTC; elsewhere in the database session's time zone). Under a stage's
effective time zone, `now()` SHALL be the engine's one clock reading for the execution taken in the
zone's wall clock and `current_date()` its date, each rendered as a literal.

#### Scenario: Recent orders
- **WHEN** a filter `created_at >= date_add(current_date(), -30, 'day')` runs
- **THEN** it keeps orders created within the last 30 days by the database's date

#### Scenario: Clock functions in the query zone
- **WHEN** the clock reads the instant `2025-01-01 03:30:00Z` and a query with `time_zone: America/New_York` has the dimensions `current_date()` and `now()`
- **THEN** they are the DATE `2024-12-31` and the TIMESTAMP `2024-12-31 22:30:00`, rendered as literals

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

### Requirement: Date functions agree across dialects
Every date function, part and unit SHALL produce the same result on every Tier-1 dialect
(SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, Snowflake, BigQuery, Trino), independent of
session settings such as the first day of the week. On SQLite, where a DATE / TIMESTAMP column can
hold text that is not a valid date, a date function over such a value SHALL return NULL. Presto
and Athena SHALL render every date function the way Trino does.

#### Scenario: Weekday independent of session settings
- **WHEN** `date_part('day_of_week', order_date)` runs on SQL Server with any `DATEFIRST` setting
- **THEN** Monday is 1 and Sunday is 7

#### Scenario: SQLite malformed stored date
- **WHEN** `date_add(order_date, 1, 'month')` runs on SQLite for a row whose `order_date` text is `'not a date'`
- **THEN** the result for that row is NULL and the query succeeds

#### Scenario: Trino sub-day offset of a DATE
- **WHEN** `date_add(order_date, 2, 'hour')` runs on Trino for a row whose `order_date` is 2024-03-01
- **THEN** the result is the TIMESTAMP 2024-03-01 02:00:00

#### Scenario: Trino ISO calendar parts
- **WHEN** `date_part('iso_year', d)` and `date_part('day_of_week', d)` run on Trino for
  2024-12-30 (a Monday in ISO week 1 of 2025)
- **THEN** the results are 2025 and 1

#### Scenario: Presto and Athena render like Trino
- **WHEN** a query using `date_add`, `date_diff` and `date_part` is rendered for a `presto` or
  `athena` datasource
- **THEN** its interval counts, date-part fields, day and second gaps, and DATE-to-TIMESTAMP
  promotion are those rendered for Trino

### Requirement: Clock-dependent queries bypass the result cache
A query whose computation includes `now()` in any stage, or `current_date()` in a stage without a
time zone (`queries/time-zones`), SHALL neither be served from nor stored in the engine's result
cache, including on refresh; other queries, including those with `current_date()` only under a time
zone, SHALL cache as before.

#### Scenario: Clock query not cached
- **WHEN** a query with filter `created_at >= date_add(current_date(), -30, 'day')` is executed twice with caching enabled
- **THEN** both executions run against the database

#### Scenario: Plain query still cached
- **WHEN** a query with filter `date_diff('day', created_at, shipped_at) > 3` is executed twice with caching enabled
- **THEN** the second execution is served from the cache

#### Scenario: Zoned current_date caches
- **WHEN** a query with `time_zone: UTC` and filter `created_at >= date_add(current_date(), -30, 'day')` is executed twice with caching enabled and a clock reading the same day
- **THEN** the second execution is served from the cache

#### Scenario: Zoned now still bypasses
- **WHEN** a query with `time_zone: UTC` and the dimension `now()` is executed twice with caching enabled
- **THEN** both executions run against the database

### Requirement: Date function names are reserved
A model custom aggregation or custom granularity named `date_part`, `date_diff`, `date_add`,
`interval`, `current_date`, `now`, `extract`, or any date-part alias (`isodow`, `day_of_week`, `doy`,
`dayofyear`, `day_of_year`, `isoyear`, `iso_year`, `isoweek`, `iso_week`, `weekofyear`, `dayofmonth`,
`day_of_month`) (in any case) SHALL be rejected at model validation.

#### Scenario: Shadowing aggregation rejected
- **WHEN** a model declares a custom aggregation named `date_diff`
- **THEN** model validation fails naming the reserved scalar function

#### Scenario: Alias-named aggregation rejected
- **WHEN** a model declares a custom aggregation named `isodow`
- **THEN** model validation fails naming the reserved date-part alias

### Requirement: Date-part aliases mean their date_part
Each of the following spellings SHALL be accepted, case-insensitively, as an exact equivalent of
`date_part` with the listed canonical part — same value, same result key, same SQL — in every
Mode-B position:
- `year` → `year`; `quarter` → `quarter`; `month` → `month`; `week` → `week`; `day` → `day`;
  `hour` → `hour`; `minute` → `minute`; `second` → `second`;
- `isodow`, `day_of_week` → `day_of_week`;
- `doy`, `dayofyear`, `day_of_year` → `day_of_year`;
- `isoyear`, `iso_year` → `iso_year`;
- `isoweek`, `iso_week`, `weekofyear` → `week`;
- `dayofmonth`, `day_of_month` → `day`.

Each alias SHALL be accepted as a one-argument function `<alias>(x)` and as the part argument
`date_part('<alias>', x)`. An alias function call with any other argument count, or with keyword
arguments, SHALL be rejected naming the function. In a `dimensions` entry an alias call SHALL be an
ordinary expression dimension of type INT, with no warning.

The part word `dayofweek` SHALL mean `day_of_week` as the part argument of `date_part` and
`extract`, and the canonical part `millisecond` SHALL be accepted there; neither SHALL be a
function (› Ambiguous weekday names name both conventions, › Part-only words have no function form).

#### Scenario: Alias function as a dimension
- **WHEN** a query on `orders` has the expression dimension `isodow(created_at)` named `dow` and measures `count(*)`
- **THEN** its columns, rows and SQL equal those of the same query with `date_part('day_of_week', created_at)`

#### Scenario: Every alias in every position
- **WHEN** each alias is used as a function and as a `date_part` part, in a dimension, a filter, an aggregation input and an order key
- **THEN** each query returns the same columns, rows and SQL as its canonical `date_part` twin

#### Scenario: Alias part inside date_part
- **WHEN** a filter uses `date_part('ISOWEEK', created_at) = 1`
- **THEN** it binds to the same value as `date_part('week', created_at) = 1`

#### Scenario: Wrong alias arity
- **WHEN** a filter uses `isodow(created_at, 1) = 1`
- **THEN** the query is rejected with an error naming `isodow` and its argument count

#### Scenario: Granularity call keeps its bucket meaning
- **WHEN** a query lists `"date_trunc('week', created_at)"` in `dimensions`, and another lists `"week(created_at)"`
- **THEN** the first is the week time bucket and the second is the ISO week number

#### Scenario: Extraction names equal their date_part twin
- **WHEN** `year(x)`, `quarter(x)`, `month(x)`, `week(x)`, `day(x)`, `hour(x)`, `minute(x)` and `second(x)` are each projected as a dimension and compared with `date_part('<part>', x)`, on SQLite, DuckDB and Postgres
- **THEN** each has the twin's result key, SQL and values; for `created_at` 2024-12-30 10:15:42 they are 2024, 4, 12, 1, 30, 10, 15 and 42

#### Scenario: Week is the ISO week number
- **WHEN** a filter uses `week(created_at) = 1`
- **THEN** it keeps exactly the rows `date_part('week', created_at) = 1` keeps, including 2024-12-30

#### Scenario: Ossie's DAYOFWEEK part is the ISO day
- **WHEN** dimensions are `EXTRACT(DAYOFWEEK FROM created_at)`, `DATE_PART('dayofweek', created_at)` and `date_part(DAYOFWEEK, created_at)` over rows on Sunday 2024-12-29 and Monday 2024-12-30
- **THEN** each has the result key, SQL and values of `date_part('day_of_week', created_at)`: 7 and 1, on every Tier-1 dialect

### Requirement: extract is the SQL spelling of date_part
`extract(<part> from x)` SHALL mean exactly `date_part('<part>', x)` — same value, same result key,
same SQL — for every canonical part and every date-part alias, the part written bare or as a string
literal, `extract` and `from` matched case-insensitively, `x` any temporal operand (including nested
calls and expressions). A part outside the canonical parts and aliases SHALL be rejected with the
error listing the accepted parts. Text inside string literals SHALL never be read as `extract` or
`from`.

#### Scenario: Extract with an alias part
- **WHEN** a query on `orders` has the expression dimension `extract(isodow from created_at)`
- **THEN** its columns, rows and SQL equal those of `date_part('day_of_week', created_at)`

#### Scenario: Extract over a nested operand
- **WHEN** a measure uses `max(EXTRACT(day FROM coalesce(shipped_at, created_at)))`
- **THEN** it equals `max(date_part('day', coalesce(shipped_at, created_at)))`

#### Scenario: Unknown extract part
- **WHEN** a filter uses `extract(epoch from created_at) > 0`
- **THEN** the query is rejected with an error listing the accepted parts

#### Scenario: Keyword text in a string literal
- **WHEN** a filter uses `status == 'extract(day from x)'`
- **THEN** the literal is compared unchanged

### Requirement: Ambiguous weekday names name both conventions
`dow` and `weekday` SHALL NOT heal: as a `date_part` or `extract` part they SHALL be rejected, and
as a function call that resolves to no aggregation (including a model's custom aggregation of that
name) they SHALL be rejected; in both cases the error SHALL state that the name has conflicting
weekday numberings across databases and give `date_part('day_of_week', x)` (ISO, Monday = 1 …
Sunday = 7) and `date_part('day_of_week', x) % 7` (Sunday = 0 … Saturday = 6). `dayofweek` SHALL be
rejected the same way as a function call that resolves to no aggregation, the error also giving
`EXTRACT(DAYOFWEEK FROM x)`; as a part it SHALL mean `day_of_week` (› Date-part aliases mean their
date_part).

#### Scenario: dow as a function
- **WHEN** a query on `orders` has the expression dimension `dow(created_at)`
- **THEN** the query is rejected with an error giving both `date_part('day_of_week', x)` and `date_part('day_of_week', x) % 7`

#### Scenario: dayofweek as an extract part
- **WHEN** a filter uses `extract(dayofweek from created_at) = 1`
- **THEN** it keeps exactly the Monday rows, as `date_part('day_of_week', created_at) = 1` does

#### Scenario: Custom aggregation named weekday still works
- **WHEN** a model declares a custom aggregation `weekday` and a measure uses `weekday(amount)`
- **THEN** the measure renders the model's `weekday` aggregation

#### Scenario: dayofweek as a function
- **WHEN** a query on `orders` has the expression dimension `DAYOFWEEK(created_at)`
- **THEN** the query is rejected with an error giving `EXTRACT(DAYOFWEEK FROM x)`, `date_part('day_of_week', x)` and `date_part('day_of_week', x) % 7`

#### Scenario: weekday and dow as parts
- **WHEN** filters use `extract(dow from created_at) = 1` and `date_part('weekday', created_at) = 1`
- **THEN** each query is rejected with the explanatory error

### Requirement: SQL date literals and clock keywords
`DATE '<literal>'` SHALL be a DATE value, and `TIMESTAMP '<literal>'` and `TIMESTAMP_NTZ '<literal>'`
the same TIMESTAMP value, accepting exactly the shapes and calendar checks of ISO literals in date
positions, in every position; a malformed or impossible value SHALL be rejected with the same typed
query error. The keywords SHALL match case-insensitively. `CURRENT_DATE` SHALL mean `current_date()`
and `CURRENT_TIMESTAMP` SHALL mean `now()`, sharing their result keys, SQL and result-cache bypass.

#### Scenario: Typed literals
- **WHEN** a filter uses `created_at >= TIMESTAMP '2024-01-01 00:00'` and a dimension uses `date_diff('day', DATE '2024-01-01', created_at)`
- **THEN** both bind with typed temporal literals and execute

#### Scenario: Impossible typed literal
- **WHEN** a filter uses `created_at >= DATE '2024-02-30'`
- **THEN** the query is rejected with the type error naming the literal

#### Scenario: Clock keywords
- **WHEN** one filter uses `created_at >= CURRENT_DATE` and another `created_at >= current_date()`
- **THEN** both render identical SQL and neither query is served from or stored in the result cache

#### Scenario: TIMESTAMP_NTZ literal
- **WHEN** dimensions are `TIMESTAMP_NTZ '2024-01-15 10:30:00'`, `timestamp_ntz '2024-01-15'` and `TIMESTAMP_NTZ '2024-01-15 10:30:00.25'`
- **THEN** they have the values and SQL of the same literals spelled `TIMESTAMP`: 2024-01-15 10:30:00, 2024-01-15 00:00:00 and 2024-01-15 10:30:00.25, on every Tier-1 dialect

#### Scenario: Malformed TIMESTAMP_NTZ literal
- **WHEN** filters use `created_at >= TIMESTAMP_NTZ '2024-13-01 00:00'` and `created_at >= TIMESTAMP_NTZ 'soon'`
- **THEN** each query is rejected with the type error naming the literal

### Requirement: date_trunc is the granularity call

`date_trunc(unit, x)` SHALL be the only Mode-B spelling of a time bucket, with the same bucket, type and behaviour in every position: a row value in filters, aggregation sources, computed dimensions and arithmetic, and a time dimension in a `dimensions` / `time_dimensions` entry or an `order` key. `unit` SHALL be a string literal, matched case-insensitively, naming a built-in granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) or a custom granularity defined on the query's datasource (`queries/custom-granularities`); anything else SHALL be rejected with the unknown-granularity error listing the built-in and the datasource's granularities. A unit SHALL never be resolved as a function or aggregation name, so a model aggregation named like a custom granularity never changes what `date_trunc` means. `date_trunc` SHALL be reserved like the other date function names.

#### Scenario: Row value

- **WHEN** a filter is `date_trunc('month', created_at) = '2025-03'`
- **THEN** it keeps exactly the rows whose `created_at` falls in March 2025

#### Scenario: Time dimension

- **WHEN** a query's `dimensions` entry is `"date_trunc('month', created_at)"`
- **THEN** it is the month time dimension on `created_at`, with result key `orders.created_at`

#### Scenario: Unknown unit

- **WHEN** a dimension is `date_trunc('fortnight', created_at)`
- **THEN** the query fails with the unknown-granularity error

#### Scenario: Sunday weeks and custom units

- **WHEN** a datasource defines `fiscal_year` and a query has dimensions `date_trunc('week_sunday', created_at)` and `date_trunc('FISCAL_YEAR', order_date)`
- **THEN** they are the `week_sunday` and `fiscal_year` time dimensions

#### Scenario: Unit never captured by an aggregation

- **WHEN** a datasource defines the granularity `fiscal_year`, the model also defines a custom aggregation `fiscal_year`, and a query has dimension `date_trunc('fiscal_year', order_date)` and measure `fiscal_year(amount)`
- **THEN** the dimension buckets by the fiscal year and the measure is the model's aggregation

### Requirement: Granularity names have no call form
A call `g(x)` whose callee `g` is `week_sunday` or a custom granularity of the query's datasource, and which resolves to no aggregation, SHALL fail in every position with a typed error whose remedy is `date_trunc('<g>', x)`, spelled with the call's own argument.

#### Scenario: Sunday-week call
- **WHEN** a query lists `"week_sunday(created_at)"` in `dimensions`
- **THEN** it fails with the typed error suggesting `date_trunc('week_sunday', created_at)`

#### Scenario: Custom granularity call
- **WHEN** a datasource defines `fiscal_year` and a filter uses `fiscal_year(order_date) = '2024'`
- **THEN** the query fails with the typed error suggesting `date_trunc('fiscal_year', order_date)`

### Requirement: Extraction compared with a date-shaped literal is refused
A comparison (`=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, a `BETWEEN` bound or an `IN` list element) between a `date_part` value, whatever its spelling, and a date-shaped string literal SHALL fail before SQL is generated with a typed error naming the literal and the `date_trunc('<part>', x)` remedy. A date-shaped literal SHALL be an instant, a period literal other than a bare year (`YYYY-Qn`, `YYYY-MM`, `YYYY-Www`, `YYYY-MM-DD`), or a relative token, as defined by `queries/time-points`. A bare-year string and a numeric string SHALL keep comparing as numbers.

#### Scenario: Month compared with a month literal
- **WHEN** a filter is `month(created_at) = '2025-03'`
- **THEN** the query fails with the typed error naming `'2025-03'` and suggesting `date_trunc('month', created_at) = '2025-03'`

#### Scenario: Relative token and BETWEEN
- **WHEN** filters are `date_part('year', created_at) = 'this year'` and `month(created_at) BETWEEN '2025-01' AND '2025-03'`
- **THEN** each fails with the same typed error

#### Scenario: Numeric strings stay legal
- **WHEN** filters are `year(created_at) = '2025'` and `month(created_at) IN ('03', '04')`
- **THEN** both bind, keeping the 2025 rows and the March and April rows of every year respectively

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

### Requirement: Part-only words have no function form
`millisecond(x)` SHALL NOT be a function: a call that resolves to no aggregation (including a
model's custom aggregation of that name) SHALL be rejected with an error giving
`EXTRACT(MILLISECOND FROM x)` and `date_part('millisecond', x)`. A model's custom aggregation named
`millisecond` or `dayofweek` SHALL keep working.

#### Scenario: millisecond as a function
- **WHEN** a query on `orders` has the expression dimension `MILLISECOND(created_at)`
- **THEN** the query is rejected with an error giving `EXTRACT(MILLISECOND FROM x)` and `date_part('millisecond', x)`

#### Scenario: Custom aggregation named millisecond still works
- **WHEN** a model declares a custom aggregation `millisecond` and a measure uses `millisecond(amount)`
- **THEN** the measure renders the model's `millisecond` aggregation

### Requirement: TO_DATE and TO_TIMESTAMP parse ISO strings
`TO_DATE(s)` SHALL mean `CAST(s AS DATE)` and `TO_TIMESTAMP(s)` SHALL mean `CAST(s AS TIMESTAMP_NTZ)`,
matched case-insensitively, with that cast's value, result key, SQL and errors on every dialect.
A call with a second (format) argument, or with any other argument count, SHALL be rejected at plan
time with the unknown-function error naming the function and the form (format parsing is Ossie
EXPERIMENTAL). Inside a model that declares a custom aggregation of the same name, a call SHALL be
that aggregation.

#### Scenario: Parse ISO strings
- **WHEN** dimensions are `TO_DATE('2024-01-15')` and `TO_TIMESTAMP('2024-01-15 10:30:00')`
- **THEN** they yield the DATE 2024-01-15 and the TIMESTAMP 2024-01-15 10:30:00 on every Tier-1 dialect, as `CAST('2024-01-15' AS DATE)` and `CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)` do

#### Scenario: Format argument refused
- **WHEN** a dimension is `TO_DATE(code, 'YYYY-MM-DD')` or `TO_TIMESTAMP(code, 'YYYY-MM-DD HH24:MI:SS')`
- **THEN** the query fails at plan time with the unknown-function error naming the two-argument form

#### Scenario: Custom aggregation named to_date
- **WHEN** a model declares a custom aggregation `to_date` and a measure uses `to_date(amount)`
- **THEN** the measure renders the model's `to_date` aggregation
