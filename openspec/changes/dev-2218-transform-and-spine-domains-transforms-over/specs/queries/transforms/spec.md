## ADDED Requirements

### Requirement: Transforms evaluate over their operand's domain
Every transform — the rank family (`rank`, `dense_rank`, `percent_rank`, `ntile`), `cumsum`, `lag`, `lead`, `first`, `last`, `time_shift`, `change`, `change_pct` and `consecutive_periods` — SHALL evaluate over its operand's domain and attach its result to the query's cells on its result grain, in every position (dimension, measure, filter, order) and whether or not the query names `population`. The domain SHALL be the cells of the operand's constituents at the operand grain: an aggregate constituent's cells are the grain tuples its filtered home rows reach (rows hidden by a model filter or a row-level-security policy do not exist); a nested transform constituent's cells are its own result cells; constituents at the full operand grain supply their cells, and a coarser constituent's cells extend to the agreeing cells of the finer constituents, else of the query's dimension anchor; an operand with no aggregate constituent takes the dimension anchor's cells. In a spine query the domain SHALL be the operand's non-time tuples that have a cell in a visible bucket, each crossed with every bucket in range. A query cell outside the domain SHALL read NULL for the transform. The query's rows, its other measures, its measure-typed filters and its order SHALL NOT change a transform's value in any cell, and a transform used only in a filter or the order SHALL supply no rows. Ranking, row offsets (`lag`, `lead`), running totals and collapsing picks SHALL step over the domain's cells only; calendar-step transforms keep stepping by calendar buckets.

Scenarios marked "the Appendix B seed" use customers C1 EAST retail, C2 EAST corporate, C3 WEST retail, C4 NORTH retail; orders C1 100 (2025-01-10), C1 200 (2025-02-10), C2 50 (2025-01-20), C3 300 (2025-02-20); returns C1 25, C4 10. "Daily sales" uses `sales` 10 on 2025-01-05 and 30 on 2025-03-05, and `refunds` 5 on 2025-02-05, both joining a `days` dataset.

#### Scenario: A sibling that adds rows leaves rank-family values unchanged
- **WHEN** a query without `population` on the Appendix B seed groups by `customers.region` and selects `percent_rank(count(orders.*))` and `ntile(count(orders.*), n=2)`, once alone and once with `returns.total_returns` added
- **THEN** both runs read EAST (1, 2) and WEST (0, 1), and the NORTH row the sibling adds reads NULL for both transforms

#### Scenario: lag skips gaps without the spine
- **WHEN** a daily-sales query by month of `days.day` selects `lag(sum(sales.amount))`, once alone and once with `sum(refunds.amount)` added
- **THEN** January reads NULL and March reads 10 in both runs, and the February row the sibling adds reads NULL

#### Scenario: A nested transform is a constituent at its result grain
- **WHEN** the daily-sales query selects `lag(cumsum(sum(sales.amount)))`, once alone and once with `sum(refunds.amount)` added
- **THEN** January reads NULL and March reads 10 in both runs: `lag` steps over the running total's own cells

#### Scenario: A collapsing pick reads the operand's cells
- **WHEN** the daily-sales query selects `last(sum(sales.amount))`, once alone and once beside `sum(refunds.amount)` with a refund dated in April
- **THEN** every row of both runs reads 30, the latest month with sales, never the April row the sibling adds

#### Scenario: A coarser operand ranks its own cells
- **WHEN** a query without `population` on the Appendix B seed groups by `customers.region` and `customers.segment` and selects only `rank(sum(orders.amount, partition_by=customers.region), direction='desc')`
- **THEN** the rows are (EAST, retail) 1, (EAST, corporate) 1 and (WEST, retail) 2: the two regions with orders are ranked and broadcast, and NORTH supplies no row

#### Scenario: A mixed-grain operand ranks the union-grain cells
- **WHEN** a query on the Appendix B seed groups by `customers.region` and `customers.segment` and selects `rank(sum(orders.amount, partition_by=customers.region) - sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc')`
- **THEN** exactly the cells (EAST, retail), (EAST, corporate) and (WEST, retail) are ranked, the region totals broadcast onto them, and the values are correct by executed values

#### Scenario: A named population does not widen the domain
- **WHEN** a query on the Appendix B seed names `population: customers`, groups by `customers.region` and selects `percent_rank(count(orders.*))`
- **THEN** EAST reads 1, WEST 0 and NORTH NULL

#### Scenario: A hand-made calendar population is not a dense series
- **WHEN** a query on the spine scenario datasource names `population: calendar` (one row per day, Jan–Jun 2025, joined from `return_date`), groups by month of `calendar.date` and selects `lag(sum(returns.amount))`
- **THEN** the months read NULL, NULL, 20, NULL, 10, NULL — `lag` steps over the months with returns only — while the same measure over the spine reads NULL, NULL, 20, 10, NULL, 5

#### Scenario: Filter and order positions read the same values
- **WHEN** the Appendix B query by `customers.region` with `orders.total_revenue` and `returns.total_returns` filters `percent_rank(count(orders.*)) = 1`, and another orders by `percent_rank(count(orders.*))` descending
- **THEN** the filter keeps exactly the EAST row, the order puts EAST before WEST, and neither changes any surviving value

#### Scenario: A filter-only transform supplies no rows
- **WHEN** a query without `population` on the Appendix B seed groups by `customers.region`, selects only `returns.total_returns` and filters `percent_rank(count(orders.*)) = 1`
- **THEN** the only row is EAST 25: WEST, which only the transform's domain holds, never becomes a row

## MODIFIED Requirements

### Requirement: consecutive_periods counts calendar periods

`consecutive_periods(p)` SHALL count consecutive calendar buckets of the
query's active time bucket in which `p` holds. A bucket with no row in its
series is a gap and SHALL break the run exactly as a false or NULL predicate
does; the next bucket where `p` holds after a gap SHALL return 1. Runs SHALL be
counted per combination of the query's non-time dimensions. The series SHALL be
its operand's domain (`queries/transforms` › Transforms evaluate over their
operand's domain): the query's date range bounds it (no bucket before the
range contributes), a row-level filter that removes every row of a bucket makes
that bucket a gap, and a measure-typed filter SHALL NOT change any surviving
row's streak. A NULL time bucket SHALL be adjacent to no bucket: it returns 1
where `p` holds and 0 otherwise, and it never extends or starts another
bucket's run. Row-based `lag` / `lead` SHALL keep stepping over their
operand's present cells when nested with `consecutive_periods` in either direction.

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
  column and requests `consecutive_periods(sum(<total>) > 100)`
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
