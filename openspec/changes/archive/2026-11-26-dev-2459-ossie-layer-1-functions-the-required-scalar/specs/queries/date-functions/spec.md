## MODIFIED Requirements

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

## ADDED Requirements

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
