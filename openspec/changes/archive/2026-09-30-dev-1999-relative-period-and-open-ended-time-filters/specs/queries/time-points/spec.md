## Purpose

Defines time points — instants, period literals and relative tokens — and how a string literal compared with a temporal expression, or used as a date range bound, is interpreted, typed, lowered and snapped, identically in every query position and on every dialect.

## ADDED Requirements

### Requirement: A time literal is an instant or a period

A string literal compared with a temporal operand, or used as a `date_range` element, SHALL be read as a time point. An **instant** is an ISO date-time with a time part (`YYYY-MM-DD HH:MM[:SS[.fraction]]`, at most six fractional digits, `T` accepted as the separator; a zone offset is not accepted). A **period** is a half-open interval `[start, next_start)` written as a period literal — `YYYY` (year), `YYYY-Qn` (quarter, n in 1..4), `YYYY-MM` (month), `YYYY-Www` (ISO-8601 week, Monday-anchored, regardless of any time dimension's granularity), `YYYY-MM-DD` (day) — or as a relative token. A string compared with a temporal operand that is none of these, or a period literal naming a non-existent period (month 13, `2025-02-29`, week 53 of a year with 52 ISO weeks), SHALL fail with a typed error listing the accepted forms.

#### Scenario: Period literals expand to half-open ranges

- **WHEN** a filter compares a TIMESTAMP column with `'2025'`, `'2025-Q1'`, `'2025-03'`, `'2025-W05'` and `'2025-03-15'` using `=`
- **THEN** the rows matched are exactly those in `[2025-01-01, 2026-01-01)`, `[2025-01-01, 2025-04-01)`, `[2025-03-01, 2025-04-01)`, `[2025-01-27, 2025-02-03)` and `[2025-03-15, 2025-03-16)` respectively, by executed values on SQLite and DuckDB

#### Scenario: ISO week 53 and leap day

- **WHEN** a filter uses `'2020-W53'` or `'2024-02-29'`
- **THEN** they resolve to `[2020-12-28, 2021-01-04)` and `[2024-02-29, 2024-03-01)`

#### Scenario: Non-existent periods are rejected

- **WHEN** a filter compares a temporal column with `'2021-W53'`, `'2025-02-29'`, `'2025-13'` or `'2025-Q5'`
- **THEN** the query fails with the typed time-literal error listing the accepted forms

#### Scenario: Unparseable string against a temporal operand is rejected

- **WHEN** a filter compares a TIMESTAMP column with `'last fortnight'` or `'2025/01/01'`
- **THEN** the query fails with the typed time-literal error listing the accepted forms, and no SQL is executed

#### Scenario: An instant keeps a plain comparison

- **WHEN** a filter is `ts <= '2024-12-31 12:00:00'` or `ts = '2024-06-01T10:00:00'`
- **THEN** it matches exactly the rows the plain comparison against that instant matches

#### Scenario: Zone offsets are rejected

- **WHEN** a TIMESTAMP column is compared with `'2025-01-01 10:00:00Z'` or `'2025-01-01 10:00:00+02:00'`
- **THEN** the query fails with the typed time-literal error

### Requirement: Relative tokens resolve against one clock reading

A relative token SHALL be matched case-insensitively with whitespace collapsed, and SHALL denote a period computed from "now", read once per query execution from the engine's clock (the SLayer host's local wall-clock time; arithmetic is naive wall-clock). The grammar is closed: `today`, `yesterday`, `tomorrow`; `this <unit>`, `last <unit>`, `next <unit>` (the current, previous and next unit); `last N <units>` and `next N <units>` (the N whole units immediately before or after the current unit, excluding the current unit); `N <units> ago` and `N <units> from now` (the single unit N steps before or after the current one); `week to date`, `month to date`, `quarter to date`, `year to date` (`[start of the current unit, start of tomorrow)`). `<unit>` is any time granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) with an optional plural `s`; `week` is Monday-anchored and `week_sunday` Sunday-anchored. N is a positive integer. The clock SHALL be the engine's own; SQL-side `now()` / `current_date()` (`queries/date-functions`) read the database clock.

#### Scenario: Calendar tokens

- **WHEN** the clock reads `2026-09-29 12:00:00` (a Tuesday) and filters use `today`, `yesterday`, `this month`, `last month`, `this quarter`, `last year`, `this week`, `this week_sunday`
- **THEN** they resolve to `[2026-09-29, 2026-09-30)`, `[2026-09-28, 2026-09-29)`, `[2026-09-01, 2026-10-01)`, `[2026-08-01, 2026-09-01)`, `[2026-07-01, 2026-10-01)`, `[2025-01-01, 2026-01-01)`, `[2026-09-28, 2026-10-05)` and `[2026-09-27, 2026-10-04)` respectively

#### Scenario: last N excludes the current unit

- **WHEN** the clock reads `2026-09-29 12:00:00` and filters use `last 7 days`, `last 3 months`, `next 2 days` and `last 6 hours`
- **THEN** they resolve to `[2026-09-22, 2026-09-29)`, `[2026-06-01, 2026-09-01)`, `[2026-09-30, 2026-10-02)` and `[2026-09-29 06:00:00, 2026-09-29 12:00:00)`

#### Scenario: Single-unit offsets and to-date tokens

- **WHEN** the clock reads `2026-09-29 12:00:00` and filters use `3 months ago`, `2 days from now`, `year to date` and `week to date`
- **THEN** they resolve to `[2026-06-01, 2026-07-01)`, `[2026-10-01, 2026-10-02)`, `[2026-01-01, 2026-09-30)` and `[2026-09-28, 2026-09-30)`

#### Scenario: Rollover and sub-second clock readings

- **WHEN** the clock reads `2027-01-01 00:00:00.500` and filters use `last month`, `last quarter`, `yesterday` and `last 1 second`
- **THEN** they resolve to `[2026-12-01, 2027-01-01)`, `[2026-10-01, 2027-01-01)`, `[2026-12-31, 2027-01-01)` and `[2026-12-31 23:59:59, 2027-01-01 00:00:00)`

#### Scenario: Case and whitespace are normalised

- **WHEN** a filter uses `'Last   Month'`
- **THEN** it resolves exactly as `'last month'`

#### Scenario: The clock is read once per execution

- **WHEN** a multi-stage query whose stages and a spliced query-backed model all use relative tokens is executed with a clock that returns a later value on each call
- **THEN** the clock is called exactly once and every stage resolves against that one reading

#### Scenario: A new day yields new SQL

- **WHEN** the same query using `last 7 days` is prepared with clocks reading two different days
- **THEN** the generated SQL differs, so a cached result for one day is never served for the other

#### Scenario: Relative tokens keep the result cache

- **WHEN** `ordered_at >= 'last 7 days'` is executed twice with caching on and a clock reading the same day
- **THEN** the second run is served from the cache

### Requirement: Temporal operands and time-literal typing

A temporal operand SHALL be an operand that `queries/date-functions` ("Date operands must be temporal") types DATE or TIMESTAMP — including `min`, `max`, `first` or `last` with `partition_by=` or a window, and with an ISO string literal that is a value argument of a conditional in the operand typed as a date value when the whole operand then types temporal — or a granularity call `gran(col)` on a temporal column as the comparison's direct operand. Time-point semantics SHALL apply only to comparisons with a temporal operand. A relative token, or a single-string `in` / `not in`, against an operand that is not temporal SHALL fail with the date-operand type error date functions raise, naming the operand and the remedy (declare the column's type). A plain date or date-time string compared with a non-temporal operand SHALL keep its existing plain-literal meaning. A sub-day point (an instant with a non-midnight time part, or a relative token at `hour`, `minute` or `second`) against a DATE operand SHALL fail with a typed error.

#### Scenario: Temporal aggregate in a measure filter

- **WHEN** a query grouped by customer has the filter `max(ordered_at) >= 'last month'`
- **THEN** only customers whose latest order falls on or after the start of last month are returned, by executed values on SQLite and DuckDB

#### Scenario: Joined, derived and stage columns are temporal

- **WHEN** filters compare `customers.created_at` (joined), a derived TIMESTAMP column, and a stage column recorded as TIMESTAMP with `'2025-Q1'` using `=`
- **THEN** each restricts to `[2025-01-01, 2025-04-01)` identically to the same filter on a base column

#### Scenario: Relative token against an untyped column

- **WHEN** a filter compares a column with no recorded type with `'last month'`
- **THEN** the query fails with the typed error naming the column and the remedy

#### Scenario: Date string against a text column keeps its meaning

- **WHEN** a filter is `code = '2025'` on a TEXT column
- **THEN** it matches rows whose value is the string `2025`, as before

#### Scenario: Sub-day point against a DATE column

- **WHEN** a filter compares a DATE column with `'last 6 hours'` or `'2025-01-01 10:00:00'`
- **THEN** the query fails with the typed error stating that the column has day resolution

#### Scenario: Date-function, interval and conditional operands

- **WHEN** the clock reads `2026-09-29 12:00:00` and filters are `date_add(ordered_at, 1, 'day') >= 'last month'`, `ordered_at - interval(1, 'day') < '2025-Q1'` and `coalesce(shipped_at, ordered_at) in '2025-Q1'`
- **THEN** they match exactly the rows with `ordered_at >= 2026-07-31`, `ordered_at < 2025-01-02`, and `coalesce(shipped_at, ordered_at)` in `[2025-01-01, 2025-04-01)` respectively, by executed values on SQLite and DuckDB

#### Scenario: ISO literal inside a conditional operand

- **WHEN** a filter is `coalesce(shipped_at, '2099-12-31') >= 'last month'`
- **THEN** unshipped orders match

#### Scenario: Bad date-function operand is named

- **WHEN** a filter is `date_add(status, 1, 'day') >= 'last month'` on a TEXT `status`
- **THEN** the query fails with the date-operand type error naming `status`

### Requirement: Comparisons against a period use period semantics

A comparison between a temporal operand `x` and a period `P` SHALL mean: `x >= P` ⇔ `x >= start(P)`; `x > P` ⇔ `x >= next_start(P)`; `x < P` ⇔ `x < start(P)`; `x <= P` ⇔ `x < next_start(P)`; `x = P` ⇔ `start(P) <= x < next_start(P)`; `x != P` ⇔ `x < start(P) or x >= next_start(P)`. A literal on the left of the operator SHALL mean the mirrored comparison. The same filter SHALL return the same rows on every dialect.

#### Scenario: Date-only bounds cover whole days

- **WHEN** a TIMESTAMP column holds rows at `2024-01-01 00:00`, `2024-01-01 10:00`, `2024-12-31 00:00` and `2024-12-31 10:00`, and filters are `ts <= '2024-12-31'`, `ts > '2024-01-01'`, `ts = '2024-01-01'` and `ts != '2024-01-01'`
- **THEN** they match all four rows, the two Dec-31 rows, the two Jan-1 rows, and the two Dec-31 rows respectively, identically on SQLite and DuckDB

#### Scenario: Literal on the left mirrors the comparison

- **WHEN** a filter is `'2025-Q1' <= ts`
- **THEN** it matches exactly the rows `ts >= '2025-Q1'` matches

#### Scenario: Relative bound in a comparison

- **WHEN** the clock reads `2026-09-29 12:00:00` and a filter is `ts >= 'last month'`
- **THEN** it matches exactly the rows with `ts >= 2026-08-01`

### Requirement: Single-string in tests period membership

`x in '<time point>'` SHALL mean `x = P` and `x not in '<time point>'` SHALL mean `x != P`. An `in` / `not in` whose right-hand side is a tuple or list SHALL keep its value-list meaning, even when its single element is a period literal.

#### Scenario: Period membership

- **WHEN** filters are `ordered_at in '2025-Q1'` and `ordered_at not in 'this month'`
- **THEN** they match the rows inside, respectively outside, the period

#### Scenario: Tuple keeps list meaning

- **WHEN** a filter on a TEXT column is `code in ('2025-Q1',)`
- **THEN** it matches rows whose value equals the string `2025-Q1`

### Requirement: Sub-day bounds are independent of SQLite timestamp spelling

On SQLite, comparisons between a temporal operand and a time point SHALL return the same rows whether values are stored as date-only ISO text or as ISO timestamps with a space or `T` separator; sub-day comparisons resolve to milliseconds.

#### Scenario: T-separated storage

- **WHEN** a SQLite table stores `2025-03-01T09:30:00` and `2025-03-01T10:30:00`, and the filter is `ts < '2025-03-01 10:00:00'`
- **THEN** exactly the `09:30` row is matched, the same as with space-separated storage

#### Scenario: Date-only text in a TIMESTAMP column

- **WHEN** a SQLite TIMESTAMP column stores `2025-03-01` and `2025-03-01 00:00:00`, and the filters are `ts = '2025-03-01'` and `ts >= '2025-03-01 00:00:00'`
- **THEN** both rows match each filter

### Requirement: A granularity call is a row-level expression in every position

A call `gran(col)` — `gran` a time granularity (case-insensitive) and `col` a bare or dotted column reference — SHALL denote `col` truncated to that granularity, with the same per-dialect bucketing as a time dimension, wherever a row-level expression is legal: filters, aggregation sources and parameters, computed-dimension sub-expressions and arithmetic. A whole `dimensions` or `time_dimensions` entry of that shape SHALL keep its time-dimension rewrite, and an order key of that shape SHALL keep its projected-time-dimension rule. A comparison between `gran(col)` and a time point SHALL return exactly the rows of the equivalent bound on `col`. A granularity callee with any other argument shape (`month()`, `month(a, b)`, `month(upper(x))`, `month(*)`, keyword arguments) SHALL fail with the typed error naming the required `gran(col)` shape and the valid granularities.

#### Scenario: Granularity call in a filter

- **WHEN** a filter is `month(created_at) >= '2024-03-15'`
- **THEN** it matches exactly the rows with `created_at >= 2024-04-01`, by executed values on SQLite and DuckDB

#### Scenario: Granularity calls compared with each other

- **WHEN** a filter is `month(shipped_at) = month(ordered_at)`
- **THEN** it matches the rows shipped in the month they were ordered

#### Scenario: Granularity call inside a measure and a computed dimension

- **WHEN** a query has the measure `count_distinct(month(created_at))` and the computed dimension `{"expression": "year(created_at)", "name": "y"}`
- **THEN** the measure counts distinct months and the dimension groups by year bucket, by executed values on SQLite and DuckDB

#### Scenario: Wrong-shape granularity call in a filter

- **WHEN** a filter contains `month()`, `month(a, b)`, `month(upper(x))` or `month(*)`
- **THEN** the query fails with the typed error naming the `gran(col)` shape and the valid granularities

#### Scenario: Granularity call against an instant

- **WHEN** filters are `month(created_at) = '2025-03-15 10:00:00'` and `month(created_at) = '2025-03-01 00:00:00'`
- **THEN** the first matches no rows and the second matches the March rows

### Requirement: Time bounds from any spelling are frame bounds

A time bound on a time dimension's column SHALL narrow the visible buckets without clipping the rows a trailing window or `time_shift` reads, whether written as a `date_range`, a comparison against a time point, a period `=` / single-string `in`, or a comparison of `gran(col)` with a time point. A comparison against an instant with `=`, and any `!=` / `not in` against a period, SHALL remain an ordinary row filter.

#### Scenario: Relative bound does not clip a trailing window

- **WHEN** a monthly query with `sum(revenue, window='90d')` has the filter `created_at in 'last 3 months'`
- **THEN** the earliest visible month's value includes rows from before its start, and equals the value produced with `date_range: "last 3 months"` and with explicit `>=` / `<` bounds

#### Scenario: Period bound does not clip time_shift

- **WHEN** a monthly query with `time_shift(sum(revenue), -1)` has the filter `month(created_at) >= '2025-01'`
- **THEN** the January row's shifted value is December's total

#### Scenario: Period equality is a frame bound, instant equality is not

- **WHEN** a windowed query filters `created_at = '2024-06-01'`, and another filters `created_at = '2024-06-01 00:00:00'`
- **THEN** the first reaches back before June 1 for its window like a range bound, and the second restricts the window's input rows to that instant

### Requirement: whole_periods_only snaps frame bounds to earlier bucket boundaries

With `whole_periods_only`, for each column carrying time dimensions (model or stage), every frame bound on that column SHALL snap to the latest bucket boundary at or before it, where the effective upper bound is the earlier of the stated exclusive upper bound and now (now when no upper bound is stated). The boundary SHALL be the earliest among the floors at each granularity of that column's time dimensions. Bounds under `or` / `not`, and bounds on other columns, SHALL be untouched. When two time dimensions on one column have granularities of which neither nests into the other (`week` with `month`), a structured warning SHALL name the pair.

#### Scenario: Snapping at month granularity

- **WHEN** the clock reads `2026-09-29 12:00:00`, `whole_periods_only` is set, and a monthly time dimension has `date_range` `["2025-01-15", "2025-03-10"]`, `["2025-01-01", "2025-03-31"]`, `"this year"`, or no range
- **THEN** the returned months are Jan–Feb 2025, Jan–Mar 2025, Jan–Aug 2026, and every month before September 2026 respectively, each computed over its whole month

#### Scenario: Current incomplete bucket excluded at sub-day granularity

- **WHEN** the clock reads `2026-09-29 12:30:00` and an hourly query with `whole_periods_only` has rows at `2026-09-28 15:00`, `2026-09-29 11:10` and `2026-09-29 12:10`
- **THEN** the `15:00` and `11:00` buckets are returned and the `12:00` bucket is not

#### Scenario: Snapped filter bounds

- **WHEN** a monthly query with `whole_periods_only` has the filter `created_at >= '2025-01-15' and status = 'paid'`
- **THEN** the lower bound snaps to `2025-01-01` and the `status` condition is unchanged

#### Scenario: Two granularities on one column

- **WHEN** a query with `whole_periods_only` has day and month time dimensions on `created_at` and the lower bound `2025-01-15`
- **THEN** the lower bound snaps to `2025-01-01` for both, in either declaration order

#### Scenario: Non-nesting granularities warn

- **WHEN** a query with `whole_periods_only` has week and month time dimensions on one column
- **THEN** it executes and returns a structured warning naming the two granularities

#### Scenario: Lower bound in the future

- **WHEN** a query with `whole_periods_only` has a lower bound after now
- **THEN** the result is empty

#### Scenario: whole_periods_only with a trailing window

- **WHEN** a monthly query with `sum(revenue, window='90d')` and `whole_periods_only` has `date_range: "last 3 months"`
- **THEN** it returns the three complete months, and the earliest month's window reaches before its start

### Requirement: Time bounds render on every Tier-1 dialect

The SQL emitted for time-point comparisons, `date_range` bounds and granularity calls in expressions SHALL render on every Tier-1 dialect (SQLite, DuckDB, Postgres, MySQL, ClickHouse, BigQuery, Snowflake, T-SQL) as valid SQL that compares against the resolved bounds.

#### Scenario: Emission per dialect

- **WHEN** a query with `date_range: "2025-Q1"`, the filter `ts > 'last 6 hours'` and the filter `month(ts) = month(shipped_at)` is rendered for each Tier-1 dialect
- **THEN** each rendering is valid for that dialect and compares against `2025-01-01`, `2025-04-01` and the resolved sub-day bound
