## ADDED Requirements

### Requirement: Time-ordered transforms step by custom granularities

When the query's active time bucket is a custom granularity (`queries/custom-granularities`), one calendar step of `time_shift`, `change`, `change_pct` and `consecutive_periods` SHALL be `multiple` units of its base, so a bucket's neighbour is the adjacent custom bucket and an absent one is a gap. `time_shift(x, n, g)` SHALL accept a custom granularity `g` as its unit, moving the bucket start by `n × multiple` base units with the `date_add` calendar rules and reading the bucket containing the moved instant.

#### Scenario: Shift by the fiscal year

- **WHEN** a query over `orders` rows dated 2024-03-15 (10), 2024-04-02 (20), 2025-03-31 (30), 2025-04-01 (40) groups by `order_date` at `fiscal_year` (April origin) and selects `time_shift(sum(amount), -1)` and `change(sum(amount))`
- **THEN** the shifted values are FY2023 NULL, FY2024 10, FY2025 50 and the changes NULL, 40, -10, on SQLite and DuckDB

#### Scenario: Custom unit on a finer axis

- **WHEN** the same rows are grouped by month and the query selects `time_shift(sum(amount), -1, 'fiscal_year')`
- **THEN** 2024-03 and 2024-04 read NULL, 2025-03 reads 10 and 2025-04 reads 20 — one fiscal-year step is twelve months, so the result equals `time_shift(sum(amount), -1, 'year')`

#### Scenario: Streaks count custom buckets

- **WHEN** rows fall in sprints starting 2025-01-06, 2025-01-20 and 2025-02-17 (the 2025-02-03 sprint empty) and the query groups at `sprint` with `consecutive_periods(count(*) > 0)`
- **THEN** the streaks are 1, 2, 1
