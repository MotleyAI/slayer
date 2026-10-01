## Purpose

Named custom granularities defined on a datasource — offset years and quarters, N-minute buckets, N-week sprints — and their use as full time granularities wherever a built-in granularity is accepted.

## ADDED Requirements

Scenarios use a datasource defining `fiscal_year = {base: year, origin: 2000-04-01}`, `billing_month = {base: month, origin: 2000-01-15}`, `quarter_hour = {base: minute, multiple: 15}` and `sprint = {base: week, multiple: 2, origin: 2025-01-06}`, and `orders` rows dated 2024-03-15 (10), 2024-04-02 (20), 2025-03-31 (30), 2025-04-01 (40). Every executed scenario SHALL hold on SQLite and DuckDB.

### Requirement: Datasources define custom granularities

A datasource configuration SHALL accept an optional `granularities` list whose entries are `{name, base, multiple, origin}`: `name` an identifier; `base` a built-in granularity (`second`, `minute`, `hour`, `day`, `week`, `week_sunday`, `month`, `quarter`, `year`); `multiple` an integer ≥ 1, default 1; `origin` a naive date or date-time (the time-point instant grammar, no zone, second precision), defaulting to the base's natural alignment (midnight on a Monday for `week`, on a Sunday for `week_sunday`, midnight on the 1st of January otherwise). Saving SHALL fail with a typed error naming the entry and the rule when: the name is not an identifier or collides (case-insensitively) with a built-in granularity, an aggregation name, a Mode-B function or transform name, or another entry; the base is not built-in; the multiple is below 1; the origin carries a zone or sub-second part; a `day`-or-coarser base has an origin with a time of day; a `month` / `quarter` / `year` base has an origin day of month above 28; a sub-day base has an origin not aligned to its base unit. The list SHALL persist unchanged and round-trip through the datasource create and edit surfaces (Python, REST, MCP, CLI); datasource inspection SHALL show it. A datasource without the field behaves as before.

#### Scenario: Definitions round-trip

- **WHEN** a datasource is saved with the four scenario granularities via `edit_datasource` and read back through storage, REST and `inspect`
- **THEN** each surface shows the same four definitions, with `quarter_hour`'s origin defaulted

#### Scenario: Invalid definitions rejected

- **WHEN** a datasource is saved with a granularity named `month`, `sum`, `date_add` or `fiscal year`; one with base `fortnight`; one with multiple 0; one with base `month` and origin 2000-01-31; one with base `year` and origin 2000-04-01 06:00; or one with base `minute` and origin 2000-01-01 10:07:30
- **THEN** each save fails with a typed error naming the entry and the violated rule, and nothing is stored

### Requirement: Custom buckets are origin-anchored

A custom granularity's bucket boundaries SHALL be `origin + k × multiple × base` for every integer k (calendar arithmetic for month-family bases), and the bucket of an instant SHALL be the last boundary at or before it, including instants before the origin. A DATE value SHALL be bucketed as the instant at its midnight. The bucket SHALL be identical on every Tier-1 dialect.

#### Scenario: Fiscal year

- **WHEN** a query groups `orders` by `order_date` at `fiscal_year` with `sum(amount)`
- **THEN** the rows are 2023-04-01: 10, 2024-04-01: 50, 2025-04-01: 40

#### Scenario: Mid-month origin

- **WHEN** rows dated 2025-03-10 and 2025-03-15 are grouped at `billing_month`
- **THEN** their buckets are 2025-02-15 and 2025-03-15

#### Scenario: Before the origin and multi-week steps

- **WHEN** rows dated 2024-12-30, 2025-01-19 and 2025-01-20 are grouped at `sprint`
- **THEN** their buckets are 2024-12-23, 2025-01-06 and 2025-01-20

#### Scenario: Sub-hour buckets

- **WHEN** TIMESTAMP rows at 10:07 (1), 10:14 (2), 10:15 (4) and 10:44 (8) of one day are grouped at `quarter_hour` with `sum`
- **THEN** the rows are 10:00: 3, 10:15: 4, 10:30: 8

#### Scenario: Server dialects agree

- **WHEN** the four scenarios above run on PostgreSQL, MySQL, ClickHouse and SQL Server over DATE- and TIMESTAMP-typed columns
- **THEN** each returns the same buckets as SQLite and DuckDB

### Requirement: A custom granularity is accepted wherever a built-in one is

A custom granularity name SHALL be accepted, and SHALL behave exactly like a built-in granularity with the same buckets, in every position that accepts a granularity: a time dimension's `granularity` (on a model column, a joined column, a stage column or `time_spine.timestamp`), the functional `name(col)` form in `dimensions`, `time_dimensions` string entries and `order`, a granularity call inside an expression or filter, `Column.granularity`, and a time-ordered transform's unit and calendar step. Names SHALL resolve against the datasource the query runs against, at binding; a name that is neither built-in nor defined there SHALL fail with a typed error listing the built-in and the datasource's granularities. Result keys SHALL follow the built-in rules (the granularity is appended only to disambiguate same-column time dimensions).

#### Scenario: Functional form and order key

- **WHEN** a query has `dimensions=["fiscal_year(order_date)"]`, `measures=["sum(amount)"]` and `order=[{"column": "fiscal_year(order_date)", "direction": "desc"}]`
- **THEN** it returns the fiscal-year rows above in descending order, with result key `orders.order_date`, identically to the explicit time-dimension form

#### Scenario: Custom granularity in a filter

- **WHEN** a query filters `fiscal_year(order_date) = '2024-04-01'` and selects `sum(amount)`
- **THEN** the value is 50

#### Scenario: On the spine

- **WHEN** a spine query groups by `time_spine.timestamp` at `fiscal_year` with `date_range: ["2023-04-01", "2026-03-31"]` and selects `sum(orders.amount)`
- **THEN** the rows are 2023-04-01: 10, 2024-04-01: 50, 2025-04-01: 40

#### Scenario: Unknown name

- **WHEN** a query uses granularity `fiscal_yr`, or the dimension entry `mnth(order_date)`
- **THEN** planning fails with a typed error listing the built-in granularities and `fiscal_year`, `billing_month`, `quarter_hour`, `sprint`

#### Scenario: Name scoped to its datasource

- **WHEN** a query against a second datasource that defines no granularities uses `fiscal_year`
- **THEN** it fails with the unknown-granularity error

### Requirement: Nesting is boundary containment

A granularity g1 SHALL nest into g2 iff every g2 boundary is a g1 boundary, decided arithmetically from the definitions: between fixed-length bases (`second` … `week`, `week_sunday`) iff g2's length is a multiple of g1's and their origins differ by a multiple of g1's length; between month-family bases (`month`, `quarter`, `year`) iff g2's length in months is a multiple of g1's and their origins share day and time and differ by a multiple of g1's length in months; a fixed-length g1 of one day or finer into a month-family g2 iff g2's origin is aligned to g1's boundaries; never a `week`-based g1 into a month-family g2 nor a month-family g1 into a fixed-length g2. Built-in granularities nest by the same rule. Re-bucketing (stage columns and `Column.granularity`) and `whole_periods_only` SHALL use this relation.

#### Scenario: Month nests into the fiscal year, week does not

- **WHEN** a query-backed model's column carries granularity `month` and a query groups it at `fiscal_year`, then another carries `week` and is grouped at `fiscal_year`
- **THEN** the first executes and the second fails with the typed re-bucketing error

#### Scenario: The fiscal year does not nest into the calendar year

- **WHEN** a stage column bucketed at `fiscal_year` is re-bucketed downstream at `year`
- **THEN** planning fails with the typed re-bucketing error

#### Scenario: Minute multiples

- **WHEN** a column carrying `quarter_hour` is grouped at `hour`, and a column carrying `minute` is grouped at `quarter_hour`
- **THEN** both execute; a column carrying `hour` grouped at `quarter_hour` fails with the re-bucketing error
