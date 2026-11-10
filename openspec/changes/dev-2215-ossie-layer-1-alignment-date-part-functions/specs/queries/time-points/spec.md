## MODIFIED Requirements

### Requirement: Temporal operands and time-literal typing

A temporal operand SHALL be an operand that `queries/date-functions` ("Date operands must be temporal") types DATE or TIMESTAMP — including `min`, `max`, `first` or `last` with `partition_by=` or a window, and with an ISO string literal that is a value argument of a conditional in the operand typed as a date value when the whole operand then types temporal — or a bucket `date_trunc(unit, col)` on a temporal column as the comparison's direct operand. Time-point semantics SHALL apply only to comparisons with a temporal operand. A relative token, or a single-string `in` / `not in`, against an operand that is not temporal SHALL fail with the date-operand type error date functions raise, naming the operand and the remedy (declare the column's type), except against an extraction, which fails per `queries/date-functions` › Extraction compared with a date-shaped literal is refused. A plain date or date-time string compared with a non-temporal operand SHALL keep its existing plain-literal meaning. A sub-day point (an instant with a non-midnight time part, or a relative token at `hour`, `minute` or `second`) against a DATE operand SHALL fail with a typed error.

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

### Requirement: A granularity call is a row-level expression in every position

A bucket `date_trunc(unit, expr)` — `unit` a granularity per `queries/date-functions` › date_trunc is the granularity call, and `expr` a row-level expression typed DATE or TIMESTAMP (a bare or dotted column reference being the one-leaf case) — SHALL denote `expr` truncated to that granularity, with the same per-dialect bucketing as a time dimension, wherever a row-level expression is legal: filters, aggregation sources and parameters, computed-dimension sub-expressions and arithmetic. A whole `dimensions` or `time_dimensions` entry of that shape SHALL keep its time-dimension rewrite, and an order key of that shape SHALL keep its projected-time-dimension rule. A comparison between `date_trunc(unit, expr)` and a time point SHALL return exactly the rows of the equivalent bound on `expr`. A `date_trunc` call of any other shape (no or extra arguments, a non-literal unit, `*`, keyword arguments) SHALL fail with the typed error naming the required `date_trunc(unit, expr)` shape and the valid granularities; an argument that does not type DATE or TIMESTAMP SHALL fail with the date-operand type error naming the operand.

#### Scenario: Granularity call in a filter

- **WHEN** a filter is `date_trunc('month', created_at) >= '2024-03-15'`
- **THEN** it matches exactly the rows with `created_at >= 2024-04-01`, by executed values on SQLite and DuckDB

#### Scenario: Granularity calls compared with each other

- **WHEN** a filter is `date_trunc('month', shipped_at) = date_trunc('month', ordered_at)`
- **THEN** it matches the rows shipped in the month they were ordered

#### Scenario: Granularity call inside a measure and a computed dimension

- **WHEN** a query has the measure `count_distinct(date_trunc('month', created_at))` and the computed dimension `{"expression": "date_trunc('year', created_at)", "name": "y"}`
- **THEN** the measure counts distinct months and the dimension groups by year bucket, by executed values on SQLite and DuckDB

#### Scenario: Granularity call over an expression

- **WHEN** a filter is `date_trunc('month', coalesce(shipped_at, created_at)) >= '2024-03-15'` and a computed dimension is `{"expression": "date_trunc('year', CAST(created_txt AS DATE))", "name": "y"}`
- **THEN** the filter matches exactly the rows whose coalesced timestamp is at or after 2024-04-01 and the dimension groups by the year of the cast value, by executed values on SQLite and DuckDB

#### Scenario: Wrong-shape granularity call in a filter

- **WHEN** a filter contains `date_trunc('month')`, `date_trunc('month', a, b)`, `date_trunc(unit_col, created_at)` or `date_trunc('month', *)`
- **THEN** the query fails with the typed error naming the `date_trunc(unit, expr)` shape and the valid granularities

#### Scenario: Non-temporal granularity argument

- **WHEN** a filter contains `date_trunc('month', upper(x)) >= '2024-01-01'`
- **THEN** the query fails with the date-operand type error naming `upper(x)`

#### Scenario: Granularity call against an instant

- **WHEN** filters are `date_trunc('month', created_at) = '2025-03-15 10:00:00'` and `date_trunc('month', created_at) = '2025-03-01 00:00:00'`
- **THEN** the first matches no rows and the second matches the March rows

### Requirement: Time bounds from any spelling are frame bounds

A time bound on a time dimension's column SHALL narrow the visible buckets without clipping the rows a trailing window or `time_shift` reads, whether written as a `date_range`, a comparison against a time point, a period `=` / single-string `in`, or a comparison of `date_trunc(unit, col)` with a time point. A comparison against an instant with `=`, and any `!=` / `not in` against a period, SHALL remain an ordinary row filter.

#### Scenario: Relative bound does not clip a trailing window

- **WHEN** a monthly query with `sum(revenue, window='90d')` has the filter `created_at in 'last 3 months'`
- **THEN** the earliest visible month's value includes rows from before its start, and equals the value produced with `date_range: "last 3 months"` and with explicit `>=` / `<` bounds

#### Scenario: Period bound does not clip time_shift

- **WHEN** a monthly query with `time_shift(sum(revenue), -1)` has the filter `date_trunc('month', created_at) >= '2025-01'`
- **THEN** the January row's shifted value is December's total

#### Scenario: Period equality is a frame bound, instant equality is not

- **WHEN** a windowed query filters `created_at = '2024-06-01'`, and another filters `created_at = '2024-06-01 00:00:00'`
- **THEN** the first reaches back before June 1 for its window like a range bound, and the second restricts the window's input rows to that instant

### Requirement: whole_periods_only snaps frame bounds to earlier bucket boundaries

With `whole_periods_only`, for each column carrying time dimensions (model or stage), every frame bound on that column SHALL snap to the latest bucket boundary at or before it, where the effective upper bound is the earlier of the stated exclusive upper bound and now (now when no upper bound is stated). The boundary SHALL be the earliest among the floors at each granularity of that column's time dimensions, built-in or custom (`queries/custom-granularities`). Bounds under `or` / `not`, and bounds on other columns, SHALL be untouched. When two time dimensions on one column have granularities of which neither nests into the other (`week` with `month`; nesting per `queries/custom-granularities`), a structured warning SHALL name the pair. A query with `whole_periods_only` and no time dimension but a dimension that is an extraction of a temporal value SHALL fail with a typed error naming the `date_trunc('<part>', <expr>)` remedy built from that extraction.

#### Scenario: Snapping at month granularity

- **WHEN** the clock reads `2026-09-29 12:00:00`, `whole_periods_only` is set, and a monthly time dimension has `date_range` `["2025-01-15", "2025-03-10"]`, `["2025-01-01", "2025-03-31"]`, `"this year"`, or no range
- **THEN** the returned months are Jan–Feb 2025, Jan–Mar 2025, Jan–Aug 2026, and every month before September 2026 respectively, each computed over its whole month

#### Scenario: Current incomplete bucket excluded at sub-day granularity

- **WHEN** the clock reads `2026-09-29 12:30:00` and an hourly query with `whole_periods_only` has rows at `2026-09-28 15:00`, `2026-09-29 11:10` and `2026-09-29 12:10`
- **THEN** the `15:00` and `11:00` buckets are returned and the `12:00` bucket is not

#### Scenario: Snapped filter bounds

- **WHEN** a monthly query with `whole_periods_only` has the filter `created_at >= '2025-01-15' AND status = 'paid'`
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

#### Scenario: Snapping to custom boundaries

- **WHEN** the clock reads `2026-09-29 12:00:00`, `whole_periods_only` is set, and a `fiscal_year` (April origin) time dimension on `order_date` over rows 2024-03-15 (10), 2024-04-02 (20), 2025-03-31 (30), 2025-04-01 (40) has `date_range: ["2024-05-01", "2025-12-31"]`
- **THEN** the bounds snap to 2024-04-01 and 2025-04-01 and the only row is 2024-04-01: 50

#### Scenario: Month with a mid-month custom granularity warns

- **WHEN** a query with `whole_periods_only` has `month` and `billing_month` (`{base: month, origin: 2000-01-15}`) time dimensions on one column
- **THEN** it executes and returns a structured warning naming the two granularities

#### Scenario: Extraction-only dimensions name the bucket remedy

- **WHEN** a query with `whole_periods_only` has the dimension `month(created_at)` and no time dimension
- **THEN** it fails with the typed error suggesting `date_trunc('month', created_at)`

### Requirement: Time bounds render on every Tier-1 dialect

The SQL emitted for time-point comparisons, `date_range` bounds and `date_trunc` buckets in expressions SHALL render on every Tier-1 dialect (SQLite, DuckDB, Postgres, MySQL, ClickHouse, BigQuery, Snowflake, T-SQL) as valid SQL that compares against the resolved bounds.

#### Scenario: Emission per dialect

- **WHEN** a query with `date_range: "2025-Q1"`, the filter `ts > 'last 6 hours'` and the filter `date_trunc('month', ts) = date_trunc('month', shipped_at)` is rendered for each Tier-1 dialect
- **THEN** each rendering is valid for that dialect and compares against `2025-01-01`, `2025-04-01` and the resolved sub-day bound
