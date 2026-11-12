## MODIFIED Requirements

### Requirement: A time literal is an instant or a period

A string literal compared with a temporal operand, or used as a `date_range` element, SHALL be read as a time point. An **instant** is an ISO date-time with a time part (`YYYY-MM-DD HH:MM[:SS[.fraction]]`, at most six fractional digits, `T` accepted as the separator; a zone offset is not accepted), or an ISO date `YYYY-MM-DD` compared with a temporal operand by `=`, `!=`, `<`, `<=`, `>`, `>=`, `BETWEEN` or `NOT BETWEEN`, which is the instant at its midnight. A **period** is a half-open interval `[start, next_start)` written as a period literal — `YYYY` (year), `YYYY-Qn` (quarter, n in 1..4), `YYYY-MM` (month), `YYYY-Www` (ISO-8601 week, Monday-anchored, regardless of any time dimension's granularity), and `YYYY-MM-DD` (day) only as a `date_range` element or as the right-hand side of a single-string `in` / `not in` — or as a relative token. The same reading SHALL apply to a value bound into the comparison through a variable. A string compared with a temporal operand that is none of these, or a period literal naming a non-existent period (month 13, `2025-02-29`, week 53 of a year with 52 ISO weeks), SHALL fail with a typed error listing the accepted forms.

#### Scenario: Period literals expand to half-open ranges

- **WHEN** a filter compares a TIMESTAMP column with `'2025'`, `'2025-Q1'`, `'2025-03'` and `'2025-W05'` using `=`, and another is `ts in '2025-03-15'`
- **THEN** the rows matched are exactly those in `[2025-01-01, 2026-01-01)`, `[2025-01-01, 2025-04-01)`, `[2025-03-01, 2025-04-01)`, `[2025-01-27, 2025-02-03)` and `[2025-03-15, 2025-03-16)` respectively, by executed values on SQLite and DuckDB

#### Scenario: ISO week 53 and leap day

- **WHEN** a filter uses `= '2020-W53'` or `in '2024-02-29'`
- **THEN** they resolve to `[2020-12-28, 2021-01-04)` and `[2024-02-29, 2024-03-01)`

#### Scenario: Non-existent periods are rejected

- **WHEN** a filter compares a temporal column with `'2021-W53'`, `'2025-02-29'`, `'2025-13'` or `'2025-Q5'`
- **THEN** the query fails with the typed time-literal error listing the accepted forms

#### Scenario: Unparseable string against a temporal operand is rejected

- **WHEN** a filter compares a TIMESTAMP column with `'last fortnight'` or `'2025/01/01'`
- **THEN** the query fails with the typed time-literal error listing the accepted forms, and no SQL is executed

#### Scenario: An instant keeps a plain comparison

- **WHEN** a filter is `ts <= '2024-12-31 12:00:00'`, `ts = '2024-06-01T10:00:00'` or `ts <= '2024-12-31'`
- **THEN** it matches exactly the rows the plain comparison against that instant matches, the date-only form against `2024-12-31 00:00:00`

#### Scenario: Zone offsets are rejected

- **WHEN** a TIMESTAMP column is compared with `'2025-01-01 10:00:00Z'` or `'2025-01-01 10:00:00+02:00'`
- **THEN** the query fails with the typed time-literal error

#### Scenario: Date-only literal inside a measure

- **WHEN** a measure is `count(*) FILTER (WHERE created_at <= '2024-04-10')`, and another is `sum(CASE WHEN created_at > '2024-04-10' THEN 1 ELSE 0 END)`, over orders at `2024-03-05 08:00` and `2024-04-10 09:30`, both as query measures and as saved model measures
- **THEN** they yield 1 and 1, on SQLite and DuckDB

#### Scenario: Date-only literal against a DATE column

- **WHEN** a filter is `order_date <= '2024-04-10'` on a DATE column
- **THEN** it matches the rows dated on or before 2024-04-10, as before

#### Scenario: Date-only value bound through a variable

- **WHEN** a filter is `created_at <= {cutoff}` and the variable `cutoff` is `'2024-04-10'`, whether as a saved default or a value passed at execution
- **THEN** it matches the rows at or before `2024-04-10 00:00:00`

### Requirement: Comparisons against a period use period semantics

A comparison between a temporal operand `x` and a period `P` SHALL mean: `x >= P` ⇔ `x >= start(P)`; `x > P` ⇔ `x >= next_start(P)`; `x < P` ⇔ `x < start(P)`; `x <= P` ⇔ `x < next_start(P)`; `x = P` ⇔ `start(P) <= x < next_start(P)`; `x != P` ⇔ `x < start(P) or x >= next_start(P)`. `x BETWEEN a AND b` SHALL mean `x >= a and x <= b` and `x NOT BETWEEN a AND b` its negation, each bound read by its own kind. A literal on the left of the operator SHALL mean the mirrored comparison. A date-only literal in these comparisons is an instant, not a period. The same filter SHALL return the same rows on every dialect.

#### Scenario: Date-only bounds cover whole days

- **WHEN** a TIMESTAMP column holds rows at `2024-01-01 00:00`, `2024-01-01 10:00`, `2024-12-31 00:00` and `2024-12-31 10:00`, and filters are `ts <= '2024-12-31'`, `ts > '2024-01-01'`, `ts = '2024-01-01'`, `ts != '2024-01-01'` and `ts BETWEEN '2024-01-01' AND '2024-12-31'`
- **THEN** they match the first three rows, the last three rows, the `2024-01-01 00:00` row, the last three rows, and the first three rows respectively — the plain comparisons against midnight — identically on SQLite and DuckDB

#### Scenario: Literal on the left mirrors the comparison

- **WHEN** a filter is `'2025-Q1' <= ts`
- **THEN** it matches exactly the rows `ts >= '2025-Q1'` matches

#### Scenario: Relative bound in a comparison

- **WHEN** the clock reads `2026-09-29 12:00:00` and a filter is `ts >= 'last month'`
- **THEN** it matches exactly the rows with `ts >= 2026-08-01`

#### Scenario: Whole day through single-string in

- **WHEN** the same rows are filtered by `ts in '2024-12-31'` and `ts not in '2024-12-31'`
- **THEN** they match the two Dec-31 rows and the two Jan-1 rows respectively

### Requirement: Time bounds from any spelling are frame bounds

A time bound on a time dimension's column SHALL narrow the visible buckets without clipping the rows a trailing window or `time_shift` reads, whether written as a `date_range`, a range comparison (`<`, `<=`, `>`, `>=`, `BETWEEN`) against a time point — an instant, date-only included, or a period — a period `=` / single-string `in`, or a comparison of `date_trunc(unit, col)` with a time point. A comparison against an instant with `=`, a date-only `=` included, and any `!=` / `not in` against a period, SHALL remain an ordinary row filter.

#### Scenario: Relative bound does not clip a trailing window

- **WHEN** a monthly query with `sum(revenue, window='90d')` has the filter `created_at in 'last 3 months'`
- **THEN** the earliest visible month's value includes rows from before its start, and equals the value produced with `date_range: "last 3 months"` and with explicit `>=` / `<` bounds

#### Scenario: Period bound does not clip time_shift

- **WHEN** a monthly query with `time_shift(sum(revenue), -1)` has the filter `date_trunc('month', created_at) >= '2025-01'`
- **THEN** the January row's shifted value is December's total

#### Scenario: Period equality is a frame bound, instant equality is not

- **WHEN** a windowed query filters `created_at in '2024-06-01'`, and others filter `created_at = '2024-06-01 00:00:00'` and `created_at = '2024-06-01'`
- **THEN** the first reaches back before June 1 for its window like a range bound, and the other two restrict the window's input rows to that instant

#### Scenario: Date-only range bound is a frame bound

- **WHEN** a monthly query with `sum(revenue, window='90d')` has the filter `created_at >= '2025-01-01'`
- **THEN** the January value includes rows from before January 1, exactly as with `created_at >= '2025-01-01 00:00:00'`

## ADDED Requirements

### Requirement: A date-only comparison that changed meaning is announced

A query, and a save of a model or query, in which a date-only literal is compared with a TIMESTAMP operand by `<=`, `>`, `=`, `!=`, `BETWEEN` or `NOT BETWEEN` SHALL carry one informational structured warning per spelling per query or save — also when the date-only value arrives through a variable, which the warning then names. The warning SHALL state that the literal is the instant at midnight and name the whole-day spelling (`x in 'YYYY-MM-DD'`, or `x < '<next day>'`). It SHALL NOT be a `FutureWarning`. A comparison by `>=` or `<`, against a DATE operand, against a non-temporal operand, or with an instant carrying a time part SHALL NOT emit it.

#### Scenario: Inclusive upper bound is announced

- **WHEN** a query on a TIMESTAMP `created_at` has the filter `created_at <= '2024-12-31'`
- **THEN** the response carries exactly one structured warning naming `created_at in '2024-12-31'` and `created_at < '2025-01-01'`, and no `FutureWarning` is emitted

#### Scenario: Unaffected comparisons are silent

- **WHEN** queries filter `created_at >= '2024-01-01'`, `created_at < '2025-01-01'`, `order_date <= '2024-12-31'` on a DATE column, `code = '2024-12-31'` on a TEXT column, and `created_at <= '2024-12-31 00:00:00'`
- **THEN** none of them carries the warning

#### Scenario: Variable-bound value names the variable

- **WHEN** a query filters `created_at <= {cutoff}` with `cutoff` = `'2024-12-31'`
- **THEN** the response carries the warning naming the variable `cutoff`

#### Scenario: A saved measure is announced at save

- **WHEN** a model is saved with the measure `count(*) FILTER (WHERE created_at = '2024-04-10')`
- **THEN** the save result carries the warning once
