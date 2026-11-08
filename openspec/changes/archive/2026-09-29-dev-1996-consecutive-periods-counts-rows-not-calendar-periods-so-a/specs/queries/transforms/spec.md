## ADDED Requirements

### Requirement: consecutive_periods counts calendar periods

`consecutive_periods(p)` SHALL count consecutive calendar buckets of the
query's active time bucket in which `p` holds. A bucket with no row in its
series is a gap and SHALL break the run exactly as a false or NULL predicate
does; the next bucket where `p` holds after a gap SHALL return 1. Runs SHALL be
counted per combination of the query's non-time dimensions. The series SHALL be
the query's own rows: the query's date range bounds it (no bucket before the
range contributes), a row-level filter that removes every row of a bucket makes
that bucket a gap, and a measure-typed filter SHALL NOT change any surviving
row's streak. A NULL time bucket SHALL be adjacent to no bucket: it returns 1
where `p` holds and 0 otherwise, and it never extends or starts another
bucket's run. Row-based `lag` / `lead` SHALL keep stepping over present rows
when nested with `consecutive_periods` in either direction.

Scenarios below use `orders` with one order per month for customer 1 in
2024-01/02/03/12 and 2025-01/02/03/05/06 with amounts 150, 500, 120, 220, 140,
200, 140, 225, 210 (April–November 2024 and April 2025 empty), and for
customer 2 in 2024-02/03/04/06/07/08 with amounts 300, 50, 300, 80, 300, 300
(May 2024 empty). "Month series of customer 1" means the query filters
`customer_id = 1` (or its data stands alone).

#### Scenario: Gaps in the middle of a series break the run

- **WHEN** a query over customer 1's month series requests
  `consecutive_periods(sum(amount) > 100)`
- **THEN** the values are 1, 2, 3, 1, 2, 3, 4, 1, 2 in month order — 2024-12
  and 2025-05 start new runs after the empty months — by executed values on
  SQLite and DuckDB

#### Scenario: Per-group series with different starts and ends

- **WHEN** a query grouped by `customer_id` and month requests
  `consecutive_periods(sum(amount) > 100)`
- **THEN** customer 1 reads 1, 2, 3, 1, 2, 3, 4, 1, 2 and customer 2 reads
  Feb 1, Mar 0, Apr 1, Jun 0, Jul 1, Aug 2 — customer 2's later start and
  earlier end never affect customer 1, and the false June after the empty May
  returns 0 — by executed values on SQLite and DuckDB

#### Scenario: Date range bounds the series

- **WHEN** the customer 1 query restricts the month time dimension with
  `"date_range": ["2025-02-01", "2025-06-30"]`
- **THEN** the values are Feb 1, Mar 2, May 1, Jun 2 — the true January
  outside the range does not extend the February run

#### Scenario: Row filter that empties a bucket makes a gap

- **WHEN** the customer 1 query adds the row filter `amount != 500`
- **THEN** February 2024 has no row and the values are 2024-01 1, 2024-03 1,
  2024-12 1, 2025-01 2, 2025-02 3, 2025-03 4, 2025-05 1, 2025-06 2

#### Scenario: Measure filter never changes a surviving streak

- **WHEN** the customer 1 query adds the measure-typed filter
  `sum(amount) > 200`
- **THEN** the surviving rows keep their unfiltered streaks: 2024-02 2,
  2024-12 1, 2025-05 1, 2025-06 2

#### Scenario: Predicate over a nested calendar transform

- **WHEN** the customer 1 query requests
  `consecutive_periods(change(sum(amount)) > 0)`
- **THEN** the values are 0, 1, 0, 0, 0, 1, 0, 0, 0 — `change` is NULL after
  each gap, so the gap-adjacent months are false

#### Scenario: Calendar transform over the streak

- **WHEN** the customer 1 query requests
  `change(consecutive_periods(sum(amount) > 100))`
- **THEN** the values are NULL, 1, 1, NULL, 1, 1, 1, NULL, 1 — NULL at the
  first month and after each gap

#### Scenario: Row-based lag over the streak

- **WHEN** the customer 1 query requests
  `lag(consecutive_periods(sum(amount) > 100))`
- **THEN** the values are NULL, 1, 2, 3, 1, 2, 3, 4, 1 — `lag` reads the
  previous present row's streak across each gap

#### Scenario: Streak over a row-based lag

- **WHEN** the customer 1 query requests
  `consecutive_periods(lag(sum(amount)) > 100)`
- **THEN** the values are 0, 1, 2, 1, 2, 3, 4, 1, 2 — the lagged predicate is
  row-based, the run it drives is calendar-based

#### Scenario: Downstream stage time dimension

- **WHEN** an inner stage aggregates customer 1's orders to month totals and an
  outer stage declares a month time dimension over the inner stage's month
  column and requests `consecutive_periods(<total>:sum > 100)`
- **THEN** the values are 1, 2, 3, 1, 2, 3, 4, 1, 2 — identical to the
  model-backed query

#### Scenario: Every granularity uses its own calendar step

- **WHEN** the active time bucket is `second`, `minute`, `hour`, `day`,
  `week`, `week_sunday`, `month`, `quarter` or `year` and the series holds
  buckets 1, 2, 4 and 5 of that granularity (bucket 3 empty), each with a true
  predicate
- **THEN** the values are 1, 2, 1, 2 by executed values on SQLite and DuckDB

#### Scenario: NULL time bucket is adjacent to nothing

- **WHEN** customer 1's month series also holds an order with a NULL
  `order_date` and amount 300
- **THEN** the NULL bucket's streak is 1 and every dated month keeps the values
  1, 2, 3, 1, 2, 3, 4, 1, 2

#### Scenario: Server dialects execute the calendar streak

- **WHEN** the gapped-series query runs on PostgreSQL, MySQL, ClickHouse and
  SQL Server over a DATE-typed and a TIMESTAMP-typed time column, at `month`,
  `quarter` and `week_sunday`
- **THEN** each returns the same streaks as SQLite and DuckDB

#### Scenario: Generated SQL is pinned across dialects

- **WHEN** a gap-aware `consecutive_periods` query is rendered for the golden
  dialect set (postgres, sqlite, duckdb, tsql, bigquery)
- **THEN** the generated SQL matches recorded golden baselines, and every
  boolean-shaped predicate still appears only in condition positions

### Requirement: consecutive_periods rejects the period keyword

`consecutive_periods` SHALL reject a `period=` keyword argument with the
unsupported-keyword `ValueError` every other transform raises for a keyword it
does not accept; the counted period is always the query's active time bucket.

#### Scenario: period keyword rejected

- **WHEN** a query requests `consecutive_periods(sum(amount) > 100, period='year')`
- **THEN** the query fails with a `ValueError` naming the unsupported `period`
  keyword, instead of silently ignoring it

### Requirement: Sub-day time offsets on SQLite

On SQLite, a time offset at `hour`, `minute` or `second` granularity SHALL keep
the time of day, so `time_shift`, `change` and `change_pct` at sub-day buckets
read the calendar-shifted bucket exactly as on the other dialects. Offsets at
`day` and coarser, and `week_sunday` truncation, SHALL render as before.

#### Scenario: Hourly time_shift on SQLite

- **WHEN** a SQLite query over hourly buckets 01:00, 02:00, 03:00 and 05:00 of
  one day (sums 1, 2, 3, 5) requests `time_shift(sum(amount), -1)` and
  `change(sum(amount))`
- **THEN** `time_shift` reads NULL, 1, 2, NULL and `change` reads NULL, 1, 1,
  NULL — matching DuckDB

#### Scenario: Minute and second granularity on SQLite

- **WHEN** the same shape runs at `minute` and at `second` granularity on SQLite
- **THEN** each shifted value is the prior bucket's sum, NULL after a gap

#### Scenario: Day-and-coarser SQLite offsets are unchanged

- **WHEN** SQLite renders a time offset at `day`, `week`, `month`, `quarter` or
  `year`, or a `week_sunday` truncation
- **THEN** the rendered SQL is unchanged from before this change
