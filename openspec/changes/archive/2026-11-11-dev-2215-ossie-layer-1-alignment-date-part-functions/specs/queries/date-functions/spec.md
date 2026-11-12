## MODIFIED Requirements

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
- **WHEN** a query lists `"date_trunc('month', created_at)"` in `dimensions`
- **THEN** it is still the month time bucket, not a month number

#### Scenario: Month call is month of year
- **WHEN** a query lists `"month(created_at)"` in `dimensions` with measure `count(*)`
- **THEN** it groups by the month number 1–12 (rows of every year's March share one group), not by month bucket

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

## ADDED Requirements

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
