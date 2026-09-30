## MODIFIED Requirements

### Requirement: Relative tokens resolve against one clock reading

A relative token SHALL be matched case-insensitively with whitespace collapsed, and SHALL denote a period computed from "now", read once per query execution from the engine's clock (the SLayer host's local wall-clock time; arithmetic is naive wall-clock). The grammar is closed: `today`, `yesterday`, `tomorrow`; `this <unit>`, `last <unit>`, `next <unit>` (the current, previous and next unit); `last N <units>` and `next N <units>` (the N whole units immediately before or after the current unit, excluding the current unit); `N <units> ago` and `N <units> from now` (the single unit N steps before or after the current one); `week to date`, `month to date`, `quarter to date`, `year to date` (`[start of the current unit, start of tomorrow)`). `<unit>` is any time granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`) or a custom granularity defined on the query's datasource (`queries/custom-granularities`), with an optional plural `s`; `week` is Monday-anchored and `week_sunday` Sunday-anchored. A custom unit's current unit is the custom bucket containing now, its steps are the adjacent custom buckets, and it is a sub-day unit when its base is `hour`, `minute` or `second`; a custom unit resolves against the datasource the query runs against, and a name that datasource does not define is not a unit. N is a positive integer. The clock SHALL be the engine's own; SQL-side `now()` / `current_date()` (`queries/date-functions`) read the database clock.

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

#### Scenario: Custom-granularity units

- **WHEN** the clock reads `2026-09-29 12:00:00`, the datasource defines `fiscal_year = {base: year, origin: 2000-04-01}` and `quarter_hour = {base: minute, multiple: 15}`, and filters on a TIMESTAMP column use `this fiscal_year`, `last fiscal_year`, `last 2 fiscal_years`, `1 fiscal_year ago` and `last 2 quarter_hours`
- **THEN** they resolve to `[2026-04-01, 2027-04-01)`, `[2025-04-01, 2026-04-01)`, `[2024-04-01, 2026-04-01)`, `[2025-04-01, 2026-04-01)` and `[2026-09-29 11:30:00, 2026-09-29 12:00:00)`, by executed values on SQLite and DuckDB

#### Scenario: Custom unit outside its datasource

- **WHEN** a query against a datasource that defines no granularities filters `ordered_at >= 'last fiscal_year'`
- **THEN** the query fails with the typed time-literal error listing the accepted forms

#### Scenario: Sub-day custom unit against a DATE column

- **WHEN** a filter compares a DATE column with `'last 2 quarter_hours'`
- **THEN** the query fails with the typed error stating that the column has day resolution

### Requirement: whole_periods_only snaps frame bounds to earlier bucket boundaries

With `whole_periods_only`, for each column carrying time dimensions (model or stage), every frame bound on that column SHALL snap to the latest bucket boundary at or before it, where the effective upper bound is the earlier of the stated exclusive upper bound and now (now when no upper bound is stated). The boundary SHALL be the earliest among the floors at each granularity of that column's time dimensions, built-in or custom (`queries/custom-granularities`). Bounds under `or` / `not`, and bounds on other columns, SHALL be untouched. When two time dimensions on one column have granularities of which neither nests into the other (`week` with `month`; nesting per `queries/custom-granularities`), a structured warning SHALL name the pair.

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

#### Scenario: Snapping to custom boundaries

- **WHEN** the clock reads `2026-09-29 12:00:00`, `whole_periods_only` is set, and a `fiscal_year` (April origin) time dimension on `order_date` over rows 2024-03-15 (10), 2024-04-02 (20), 2025-03-31 (30), 2025-04-01 (40) has `date_range: ["2024-05-01", "2025-12-31"]`
- **THEN** the bounds snap to 2024-04-01 and 2025-04-01 and the only row is 2024-04-01: 50

#### Scenario: Month with a mid-month custom granularity warns

- **WHEN** a query with `whole_periods_only` has `month` and `billing_month` (`{base: month, origin: 2000-01-15}`) time dimensions on one column
- **THEN** it executes and returns a structured warning naming the two granularities
