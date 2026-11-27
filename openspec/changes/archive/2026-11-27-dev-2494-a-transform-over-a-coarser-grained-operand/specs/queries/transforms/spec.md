## MODIFIED Requirements

### Requirement: Transforms evaluate over their operand's domain
Every transform — the rank family (`rank`, `dense_rank`, `percent_rank`, `ntile`), `cumsum`, `lag`, `lead`, `time_shift`, `change`, `change_pct` and `consecutive_periods` — SHALL evaluate over its operand's domain in every position — dimension, measure, filter, order key, aggregation source and aggregation parameter, at any depth — and whether or not the query names `population`. In the first four its result attaches to the query's cells on its result grain; inside an aggregation, the aggregation reads the transform's cells at its result grain, never the rows of the aggregation's own operand, so the transform reads the same value in every position. The domain SHALL be the cells of the operand's constituents at the operand grain: an aggregate constituent's cells are the grain tuples its filtered home rows reach (rows hidden by a model filter or a row-level-security policy do not exist); a windowed aggregate constituent's cells are those of the same aggregate without `window=`; a nested transform constituent's cells are its own result cells; constituents at the full operand grain supply their cells, and a coarser constituent's cells extend to the agreeing cells of the finer constituents, else of the query's dimension anchor; an operand with no aggregate constituent takes the dimension anchor's cells. In a spine query the domain SHALL be the operand's non-time tuples that have a cell in a visible bucket, each crossed with every bucket in range. A query cell outside the domain SHALL read NULL for the transform. The query's rows, its other measures, its measure-typed filters, its order, and the dimensions, sibling constituents and parameters of an aggregation enclosing the transform SHALL NOT change a transform's value in any cell, and a transform used only in a filter or the order SHALL supply no rows. Ranking, row offsets (`lag`, `lead`) and running totals SHALL step over the domain's cells only; calendar-step transforms keep stepping by calendar buckets. A NULL time bucket SHALL have no position on the time axis on every dialect: it is adjacent to no bucket, so a preserving time-ordered transform evaluates it as a series of its own (`cumsum` reads its own value; `lag`, `lead`, `time_shift`, `change` and `change_pct` read NULL there), and it never contributes to or reads from another bucket's value; the rank family treats it as an ordinary cell. A `first` / `last` over an aggregate is an aggregation, not a transform, and ranks exactly this domain's cells (`queries/semantics` › first and last over an aggregate rank their operand's cells).

Scenarios marked "the Appendix B seed" use customers C1 EAST retail, C2 EAST corporate, C3 WEST retail, C4 NORTH retail; orders C1 100 (2025-01-10), C1 200 (2025-02-10), C2 50 (2025-01-20), C3 300 (2025-02-20); returns C1 25, C4 10. "Daily sales" uses `sales` 10 on 2025-01-05 and 30 on 2025-03-05, and `refunds` 5 on 2025-02-05, both joining a `days` dataset. "The city seed" uses one dataset `orders` with North/A 10 (2025-01-10) and 25 (2025-02-10), North/B 30 (2025-01-11) and 40 (2025-02-11), South/C 50 (2025-01-12), South/D 20 (2025-01-13) and 40 (2025-02-13): city totals A 35, B 70, C 50, D 60; region totals North 105, South 110; region-month totals North 40 and 65, South 70 and 40.

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
- **THEN** every row of both runs reads 30, the latest month with sales — the second-order `last` ranks this domain's cells — never the April row the sibling adds

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

#### Scenario: A NULL time bucket is adjacent to no bucket
- **WHEN** a query rooted at `customers` selects `cumsum(sum(customers.spend))` and `lag(sum(customers.spend))` by month of `orders.ordered_at`, a customer without orders (spend 55) falling in the NULL bucket beside monthly sums 100, 150, 100, 110 for 2024-01 to 2024-04
- **THEN** on SQLite and DuckDB alike the running total reads 100, 250, 350, 460 and 55 in the NULL bucket, and `lag` reads NULL, 100, 150, 100 and NULL in the NULL bucket

#### Scenario: A transform over a windowed aggregate reads the aggregate's own cells
- **WHEN** over the association seed of `queries/population` a query without `population` groups by `orders.status` and the month of `customers.signup_at` and selects `count(customers.*, window='60d')` as `w` with, in turn, `lag(w)`, `cumsum(w)`, `time_shift(w, -1)` and `change(w)` spelled over the same aggregate
- **THEN** on SQLite and DuckDB the cells (open, January), (open, March), (paid, January), (paid, February), (NULL, April) and (NULL, May) read `w` 1, 1, 1, 2, 1, 1; `lag` NULL, 1, NULL, 1, NULL, 1; `cumsum` 1, 2, 1, 3, 1, 2; `time_shift` NULL, NULL, NULL, 1, NULL, 1 — (open, February) is no cell of the aggregate, so (open, March) reads NULL —; and `change` NULL, NULL, NULL, 1, NULL, 0

#### Scenario: A sibling or a named population leaves a transform over a windowed aggregate unchanged
- **WHEN** each query of the previous scenario runs again beside `sum(orders.amount)`, and again naming `population: customers`; and a query by `products.category` and the month of `orders.ordered_at` selects `cumsum(count(customers.*, window='60d'))` alone, beside `sum(orders.amount)` and naming `population: customers`
- **THEN** every transform reads, cell by cell, its value in the query where it is the only metric; the second query's running total reads (food, March) 1, (toys, January) 1, (toys, February) 3, (NULL, April) 1 and (NULL, NULL) 0 in all three runs

#### Scenario: Over the spine, a transform over a windowed aggregate steps over its home's tuples
- **WHEN** over the association seed a query without `population` groups by `orders.status` and the month of `time_spine.timestamp` with `date_range` 2024-01-01 to 2024-06-30 and selects `time_shift(count(customers.*, window='60d'), -1)`, once alone, once beside `sum(orders.amount)` and once naming `population: customers`
- **THEN** on SQLite and DuckDB all three runs read, January to June, `open` 0, 1, 1, 1, 0, 0; `paid` 0, 1, 2, 1, 0, 0; and NULL 0, 0, 0, 0, 1, 1: the NULL tuple, which only `customers` reaches, is in the series, and every bucket the frame reaches carries its value

#### Scenario: Inside an aggregation, a coarser operand ranks its own cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `rank(sum(orders.amount, partition_by=orders.region), direction='desc')` as `r` and `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` as `m`
- **THEN** on SQLite and DuckDB every row reads `r` = `m`, North 2 and South 1, and `m` carries the `associated` warning naming `city`

#### Scenario: Inside an aggregation, a re-aggregated operand ranks its own cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `max(rank(max(sum(orders.amount, partition_by=[orders.region, orders.city]), partition_by=orders.region), direction='desc'))`
- **THEN** every North row reads 1 and every South row 2

#### Scenario: Inside an aggregation, a windowed operand collapsed by last ranks its own cells
- **WHEN** a query on the city seed groups by `orders.region` and the month of `orders.ordered_at` and selects `max(rank(last(sum(orders.amount, window='2m', partition_by=[orders.region, orders.ordered_at])), direction='desc'))`
- **THEN** both North months read 2 and both South months 1

#### Scenario: A finer sibling constituent does not widen a transform's domain
- **WHEN** a query on the city seed groups by `orders.region` and selects `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc') + sum(orders.amount, partition_by=[orders.region, orders.city]))`
- **THEN** North reads 72 and South 61

#### Scenario: Every aggregation and rank-family member reads the transform's cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `sum(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` and `max(percent_rank(sum(orders.amount, partition_by=orders.region)))`
- **THEN** the North rows read 2 and 0 and the South rows 1 and 1

#### Scenario: As an aggregation parameter a transform reads its own cells
- **WHEN** a query on the city seed with no dimensions selects `weighted_avg(sum(orders.amount, partition_by=[orders.region, orders.city]), weight=T)` for `T` in turn `rank(sum(orders.amount, partition_by=orders.region), direction='desc')`, `rank(max(sum(orders.amount, partition_by=[orders.region, orders.city]), partition_by=orders.region), direction='desc')` and `percent_rank(sum(orders.amount, partition_by=orders.region))`; and a query grouped by the month of `orders.ordered_at` selects `weighted_avg(sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at]), weight=rank(last(sum(orders.amount, window='2m', partition_by=[orders.region, orders.ordered_at])), direction='desc'))`
- **THEN** the first reads 320/6, the second 325/6 and the third 55; the monthly query reads January 25 and February 34

#### Scenario: Filter and order read a transform inside an aggregation at its metric-position value
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city`, selects `sum(orders.amount)` and filters `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc')) = 2`; and the same query without the filter orders by `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` ascending
- **THEN** the filter keeps exactly the rows North/A and North/B, and the order puts both South rows before both North rows

#### Scenario: Transforms already evaluated over their own cells keep their values
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `sum(orders.amount * rank(sum(orders.amount, partition_by=orders.region), direction='desc'))`; a query grouped by `orders.region` and the month of `orders.ordered_at` selects `max(cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at])) + 0 * sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at]))`; a query grouped by `orders.region`, `orders.city` and the month of `orders.ordered_at` selects `max(time_shift(cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at])), -1))` and `max(time_shift(rank(sum(orders.amount, partition_by=[orders.region, orders.ordered_at]), direction='desc'), -1))`; and a query without `population` on the Appendix B seed groups by `customers.region` and selects `sum(rank(sum(returns.amount), direction='desc'))`
- **THEN** the first reads A 70, B 140, C 50, D 60; the second North 40 and 105, South 70 and 110; the third reads NULL in January and, in February, 40 and 3 on the North rows and 70 and 1 on the South row; and the fourth reads EAST 1 and NORTH 2, the NORTH cell, which only `returns` reaches, present
