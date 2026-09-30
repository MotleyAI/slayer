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
functional time-bucket form `gran(col)` (e.g. `month(created_at)`) SHALL keep its bucket meaning.

#### Scenario: Extraction as an expression dimension
- **WHEN** a query `{"source_model": "orders", "dimensions": [{"expression": "date_part('day_of_week', created_at)", "name": "dow"}], "measures": [{"formula": "count(*)"}]}` executes
- **THEN** the result has one row per weekday with key `orders.dow` holding an integer 1–7

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
- **WHEN** a query lists `"month(created_at)"` in `dimensions`
- **THEN** it is still the month time bucket, not a month number

### Requirement: Unit and part arguments are closed string literals
The `part` argument of `date_part` and the `unit` argument of `date_diff` / `date_add` SHALL be a
string literal, matched case-insensitively, from that function's closed set:
- `date_part`: `year`, `iso_year`, `quarter`, `month`, `week`, `day`, `day_of_week`,
  `day_of_year`, `hour`, `minute`, `second`;
- `date_diff` and `date_add`: `second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`,
  `quarter`, `year`.
Any other value, and any non-literal (a column, a `{variable}` that does not substitute to a listed
word, an expression), SHALL be rejected before binding with an error listing the accepted values.
Differently-cased spellings of the same unit SHALL intern to the same value.

#### Scenario: Unknown unit rejected
- **WHEN** a filter uses `date_diff('fortnight', created_at, shipped_at) > 1`
- **THEN** the query is rejected with an error listing the accepted `date_diff` units

#### Scenario: Extraction-only part rejected as a difference unit
- **WHEN** a filter uses `date_diff('day_of_week', created_at, shipped_at) > 1`
- **THEN** the query is rejected with an error listing the accepted `date_diff` units

#### Scenario: Column as a unit rejected
- **WHEN** a filter uses `date_part(status, created_at) = 1`
- **THEN** the query is rejected because the part must be a string literal

#### Scenario: Unit case folds
- **WHEN** one measure uses `date_part('MONTH', created_at)` and another `date_part('month', created_at)`
- **THEN** both resolve to the same value

### Requirement: date_part extracts calendar components with ISO week conventions
`date_part(part, ts)` SHALL return an integer: `year`, `quarter` (1–4), `month` (1–12), `day`
(day of month), `day_of_year` (1–366), `hour` (0–23), `minute`, `second` (whole seconds);
`day_of_week` SHALL be ISO-8601 numbering, Monday = 1 through Sunday = 7; `week` SHALL be the
ISO-8601 week number (weeks start Monday, week 1 contains January 4th) and `iso_year` the ISO
year that week belongs to. Parts finer than a day on a DATE operand SHALL return 0.

#### Scenario: Sunday is day 7
- **WHEN** `date_part('day_of_week', '2024-06-02')` (a Sunday) is evaluated
- **THEN** the result is 7, and for `'2024-06-03'` (a Monday) it is 1

#### Scenario: ISO week across a year boundary
- **WHEN** `date_part('week', '2024-12-30')` and `date_part('iso_year', '2024-12-30')` are evaluated
- **THEN** the results are 1 and 2025, while `date_part('year', '2024-12-30')` is 2024

#### Scenario: Hour of a DATE
- **WHEN** `date_part('hour', order_date)` is evaluated
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
value as that call, sharing its result key and SQL. Chains SHALL apply left to right. `interval`
SHALL follow the same count and unit rules as `date_add`. `interval(...)` in any other position —
standalone, as the left side of `-`, negated, multiplied, compared, nested in another `interval`,
added to another `interval`, or as a function argument — SHALL be rejected before binding with an
error pointing to `date_add`.

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

### Requirement: Date operands must be temporal
Every date operand (`ts`, `start`, `end`) SHALL have a determinable DATE or TIMESTAMP type. A
temporal operand SHALL be one of: a column (local, joined or stage) declared DATE or TIMESTAMP;
`min`, `max`, `first` or `last` of a temporal value; `date_add`; `current_date()` (DATE); `now()`
(TIMESTAMP); an ISO date literal; or `coalesce`, `ifnull`, `nullif`, `greatest`, `least` or an
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

### Requirement: ISO literals are typed date values in date positions
A string literal in a temporal operand position — directly, or as a value argument of a
`coalesce`, `ifnull`, `nullif`, `greatest`, `least` or `iif` / `CASE` in such a position — SHALL
bind as a date value: `'YYYY-MM-DD'` as a DATE and `'YYYY-MM-DD HH:MM:SS'` (with a space or `T`
separator) as a TIMESTAMP. A string in such a position that is not one of those shapes or not a
real calendar value SHALL be rejected with a typed query error. `{variable}` placeholders SHALL
work in these positions. String literals outside date positions are unchanged.

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

### Requirement: Clock functions read the database clock
`current_date()` SHALL return the database's current date as a DATE and `now()` its current
timestamp as a TIMESTAMP, evaluated by the database at execution time (on SQLite, in UTC; elsewhere
in the database session's time zone).

#### Scenario: Recent orders
- **WHEN** a filter `created_at >= date_add(current_date(), -30, 'day')` runs
- **THEN** it keeps orders created within the last 30 days by the database's date

### Requirement: Date results carry response types
A value whose outermost operation is `date_part` or `date_diff` SHALL report type INT in response
metadata; `date_add` SHALL report DATE or TIMESTAMP per its result type; `current_date()` DATE;
`now()` TIMESTAMP — for measures and expression dimensions alike.

#### Scenario: Dimension types
- **WHEN** a query has expression dimensions `date_part('month', created_at)` and `date_add(order_date, 1, 'month')`
- **THEN** response metadata reports INT for the first and DATE for the second

### Requirement: Date functions agree across dialects
Every date function, part and unit SHALL produce the same result on every Tier-1 dialect
(SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, Snowflake, BigQuery), independent of
session settings such as the first day of the week. On SQLite, where a DATE / TIMESTAMP column can
hold text that is not a valid date, a date function over such a value SHALL return NULL.

#### Scenario: Weekday independent of session settings
- **WHEN** `date_part('day_of_week', order_date)` runs on SQL Server with any `DATEFIRST` setting
- **THEN** Monday is 1 and Sunday is 7

#### Scenario: SQLite malformed stored date
- **WHEN** `date_add(order_date, 1, 'month')` runs on SQLite for a row whose `order_date` text is `'not a date'`
- **THEN** the result for that row is NULL and the query succeeds

### Requirement: Clock-dependent queries bypass the result cache
A query whose computation includes `now()` or `current_date()` in any stage SHALL neither be served
from nor stored in the engine's result cache, including on refresh; other queries SHALL cache as
before.

#### Scenario: Clock query not cached
- **WHEN** a query with filter `created_at >= date_add(current_date(), -30, 'day')` is executed twice with caching enabled
- **THEN** both executions run against the database

#### Scenario: Plain query still cached
- **WHEN** a query with filter `date_diff('day', created_at, shipped_at) > 3` is executed twice with caching enabled
- **THEN** the second execution is served from the cache

### Requirement: Date function names are reserved
A model custom aggregation named `date_part`, `date_diff`, `date_add`, `interval`, `current_date` or
`now` (in any case) SHALL be rejected at model validation.

#### Scenario: Shadowing aggregation rejected
- **WHEN** a model declares a custom aggregation named `date_diff`
- **THEN** model validation fails naming the reserved scalar function
