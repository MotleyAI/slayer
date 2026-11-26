## MODIFIED Requirements

### Requirement: Time axis on stage datasets

A downstream stage's own time dimension SHALL be the time axis for every time-ordered transform in that stage (`time_shift`, `change`, `change_pct`, `cumsum`, `lag`, `lead`, `consecutive_periods`), for its windowed aggregates and for the default ranking key of a `first` / `last` over an aggregate in that stage, with values identical to the same shapes evaluated over a model-backed dataset holding the same rows. With two stage time dimensions and no `main_time_dimension`, a time-ordered transform SHALL fail with the existing unambiguous-time-dimension remedy; `main_time_dimension` SHALL select the axis. A stage has no model-level default time dimension, so no default is applied.

#### Scenario: time_shift over a stage time dimension

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` over an inner monthly stage and selects `time_shift(sum(rev), -1, 'month')`
- **THEN** each row carries the previous month's inner sum, NULL for the first month, by executed values on SQLite and DuckDB

#### Scenario: change and cumsum over a stage time dimension

- **WHEN** the same outer stage selects `change(sum(rev))` and `cumsum(sum(rev))`
- **THEN** each row carries the month-over-month delta (NULL first) and the running total, by executed values

#### Scenario: last over a stage time dimension

- **WHEN** the same outer stage selects `last(sum(rev))`
- **THEN** every row carries the value of the latest month — the second-order `last`
  ranked by the stage's month bucket — by executed values

#### Scenario: Windowed aggregate over a stage time dimension

- **WHEN** the outer stage selects `sum(rev, window='60d')` over the stage time dimension
- **THEN** each row carries the trailing-window sum keyed on the stage bucket, by executed values

#### Scenario: time_shift over a multi-hop flat time dimension

- **WHEN** an inner stage projects `customers.regions.last_activity_at` at `month` with `count(*)` named `n`, and the outer stage declares a time dimension on `customers__regions__last_activity_at` at `month` with `time_shift(sum(n), -1, 'month')`
- **THEN** the shifted relation references the inner stage's flat alias and the query executes with correct values

#### Scenario: Two stage time dimensions need main_time_dimension

- **WHEN** the outer stage declares time dimensions on two distinct temporal columns, `created_at` and `shipped_at`, each at `month`, and selects `change(sum(rev))` without `main_time_dimension`
- **THEN** planning fails with the existing error naming the `main_time_dimension` remedy, and setting `main_time_dimension` to `created_at` makes the query execute with that column's bucket as the axis

### Requirement: Transforms reject grain-refining row-level leaves

A transform, used in
measure, filter, or order position, or as a constituent of an aggregation
source, SHALL reject with a typed plan-time error — before any SQL is
generated — any row-level (non-aggregate) leaf in its input that refines the
consumer grain, that is, a leaf that is not itself a projected query
dimension. The error SHALL name the transform, the offending leaf, and the
remedies (aggregate the leaf, e.g. `cumsum(sum(weight))`; project it as a query
dimension; or compute it in an earlier `source_queries` stage), and cite no
tracking issue. The rule applies uniformly to every such transform op — the
rank family and the shift family (`time_shift`, `change`, `change_pct`)
included. Leaves that are projected grain keys — plain or computed dimensions
— remain legal, evaluated at the query grain. The raw source column of a
bucketed time dimension is not a projected grain key (it refines the bucket).
`first` / `last` are aggregations, never transforms, and the stricter
dimension-position rules are unchanged.

#### Scenario: Bare grain-refining leaf rejected
- **WHEN** a query with a month time dimension and no `weight` dimension selects
  the measure `cumsum(weight)`
- **THEN** it fails at plan time with the typed error naming the remedy — never
  SQL whose base grain is inflated to one row per (bucket, weight-value)

#### Scenario: Rank family is covered
- **WHEN** a query over `[store]` with a month time dimension selects the
  measure `rank(qty, direction='desc')`
- **THEN** it fails with the same typed error, never a result carrying one row
  per (store, month, qty-value)

#### Scenario: Shift family is covered
- **WHEN** a query with a month time dimension and no `weight` dimension
  selects `time_shift(weight, -1)`, `change(weight)` or `change_pct(weight)`,
  or `time_shift(hi_rev, -1)` over the unprojected derived column `hi_rev`
- **THEN** each fails at plan time with the same typed error naming the
  transform, the leaf and the remedies — never a result whose row count differs
  from the same query without the measure

#### Scenario: Predicate over an unprojected row column rejected
- **WHEN** a query selects the measure `consecutive_periods(weight > 0)` with
  `weight` not a query dimension
- **THEN** it fails with the same typed error naming the aggregate-the-leaf
  remedy

#### Scenario: A projected grain key stays legal
- **WHEN** a query projects `weight` as a dimension and selects the measure
  `rank(weight, direction='desc')`
- **THEN** it compiles at the query grain and executes with correct values —
  no error, no extra result rows

#### Scenario: A projected grain key stays legal under the shift family
- **WHEN** a query over `[store]` with a month time dimension selects
  `time_shift(store, -1)`
- **THEN** it compiles at the query grain and executes with the operand's own
  value per row — no error, no extra result rows

#### Scenario: Attached values do not launder a row leaf
- **WHEN** a query selects the measure
  `cumsum(weight * avg(unit_price, partition_by=product))` with `weight` not
  projected
- **THEN** it fails with the same typed error — a transform does not collapse
  row grain, unlike an aggregation

#### Scenario: Row leaf under a transform inside an aggregation source rejected
- **WHEN** a query selects the measure `sum(cumsum(weight) - 1)` with `weight` not a
  query dimension
- **THEN** it fails at plan time with the same typed error naming the transform and
  the aggregate-the-leaf remedy — an enclosing aggregation does not launder the
  transform's row leaf

#### Scenario: Projected grain key under a transform inside a source stays legal
- **WHEN** a query over `[region]` selects the measure `sum(rank(region, direction='desc'))`
- **THEN** it compiles: the transform types at the query grain and the aggregation is
  the degenerate identity with the degenerate-re-aggregation warning, never an error

### Requirement: Rank-family partition keys are operand-grain members
A rank-family transform (`rank`, `dense_rank`, `percent_rank`, `ntile`) partitions its
operand's cells, so every key in its own `partition_by=` SHALL be a member of the
transform's operand grain, in every position. The operand grain is the union over the
transform's input: an aggregate contributes its explicit `partition_by=` keys, else the
query grain (its dimensions and time buckets), plus the query's active time bucket when
it is windowed, a `first` / `last` contributing its own grain (`queries/semantics` › A ranked aggregation's grain drops its ranking key);
a nested transform contributes its own operand grain; a composite contributes the union of its operands,
with a projected row-level leaf contributing itself; an input with no aggregate (only
row-level leaves or literals) is the query grain, since it is evaluated once per
query-grain cell. A non-member key SHALL fail at plan time with an error naming
the transform, the key, the operand grain and the remedy (add the key to the inner
aggregate's `partition_by=`, or partition by a member). A key that is no query dimension
at all keeps the existing "not a query dimension" error, and an ungrained inner aggregate
in dimension position keeps the existing grain-self-containment error; both take
precedence over the membership error.

#### Scenario: Non-member query dimension as a measure
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=product, direction='desc')`
- **THEN** planning fails with an error naming `rank`, `product`, the operand grain `city, region` and the `partition_by=` remedy — it never executes by widening the grain

#### Scenario: Non-member query dimension in a filter
- **WHEN** the same query filters `rank(sum(amount, partition_by=[city, region]), partition_by=product, direction='desc') <= 2`
- **THEN** planning fails with the same error

#### Scenario: Non-member key in dimension position, plain column
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount, partition_by=[city, product]), partition_by=region, direction='desc')`
- **THEN** planning fails with the same error naming `region` and the grain `city, product`, never with an internal producer-slot error

#### Scenario: Non-member key in dimension position, computed-dimension name
- **WHEN** a query declares `ureg` = `upper(region)` and the dimension `rank(sum(amount, partition_by=[city, region]), partition_by=ureg, direction='desc')`
- **THEN** planning fails with the same error naming the key and the grain `city, region`, never with an internal error

#### Scenario: Member key executes
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=region, direction='desc')`
- **THEN** each row carries its (city, region) total's rank within the region: East Zeta 1, Delta 2, Epsilon 2; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap the NULL city 1, Kappa 2; Void Xi NULL (its total is NULL)

#### Scenario: Ungrained inner keeps the query-dimension rule
- **WHEN** a query over the banded dimension alone selects `rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** planning fails with the existing "partition_by column 'region' is not a query dimension" error listing the available dimensions; over `[region, band]` the same measure executes because the ungrained inner is grained at the query grain and `region` is a member

#### Scenario: Windowed inner admits the active bucket
- **WHEN** a monthly query selects `rank(sum(amount, window='1y', partition_by=customers.regions.name), partition_by=ordered_at, direction='desc')`
- **THEN** the partition key passes the operand-grain rule as the query's month bucket, a member contributed by the windowed inner

#### Scenario: Nested collapsing transform drops its axis
- **WHEN** a monthly query selects `rank(last(sum(amount, partition_by=[customers.regions.name, ordered_at])), partition_by=ordered_at, direction='desc')`
- **THEN** planning fails with the operand-grain error naming `ordered_at` and the grain `customers.regions.name` — the `last` drops its ranking key, the month bucket; with `partition_by=customers.regions.name` the key passes the rule

#### Scenario: Aggregate-free input takes the query grain
- **WHEN** a query over `[city, region, product]` selects `rank(city, partition_by=region, direction='desc')`
- **THEN** it executes, ranking each row's city value descending within its region: East Zeta 1, Epsilon 2, Delta 3; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap Kappa 1, the NULL city NULL; Void Xi 1

#### Scenario: Residue error precedes the membership rule
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** planning fails with the existing grain-self-containment error ("must declare partition_by= explicitly"), not the operand-grain error

### Requirement: Transforms over a constant input

A window transform — `cumsum`, `lag`, `lead`, `change`, `change_pct`, `time_shift`,
`consecutive_periods`, `rank`, `dense_rank`, `percent_rank` and `ntile` — whose input is a
bare literal SHALL execute at the query grain, treating the literal as the same value in
every result row, and SHALL NOT fail with an internal error. `first` and `last` are
aggregations, not transforms, and are outside this requirement.

#### Scenario: Running and offset transforms over a literal
- **WHEN** a query rooted at `orders` with a month time dimension selects `sum(amount)`
  and, in turn, `cumsum(1)`, `lag(1)`, `lead(1)`, `change(1)`, `change_pct(1)` and
  `time_shift(1, -1)`
- **THEN** each executes on SQLite and DuckDB with hand-computed values: `cumsum(1)` is
  the running count of months within each partition, `lag(1)` is NULL in the first month
  and 1 after, `lead(1)` is 1 except NULL in the last month, `change(1)` is NULL in the
  first month and 0 after, `change_pct(1)` is NULL in the first month and 0 after, and
  `time_shift(1, -1)` is 1 wherever the previous month exists

#### Scenario: consecutive_periods over a literal predicate
- **WHEN** the same query selects `consecutive_periods(1 > 0)`
- **THEN** it executes on SQLite and DuckDB and each month carries its calendar-period
  run length, by hand-computed values

#### Scenario: Rank family over a literal
- **WHEN** a query grouped by a dimension selects `rank(1, direction='desc')`,
  `dense_rank(1, direction='desc')` and `percent_rank(1)`, with and without
  `partition_by=`
- **THEN** every row carries 1, 1 and 0 respectively on SQLite and DuckDB

#### Scenario: ntile over a literal
- **WHEN** a query grouped by a dimension selects `ntile(1, n=2)`
- **THEN** it executes on SQLite and DuckDB and the multiset of bucket numbers is the
  balanced split of the rows into two buckets; which row lands in which bucket is not
  specified, since every input value ties

### Requirement: A time-ordered transform over extraction dimensions names the date_trunc remedy

When a time-ordered transform (`time_shift`, `change`, `change_pct`, `cumsum`,
`lag`, `lead`, `consecutive_periods`), a `first` / `last` over an aggregate
defaulting its ranking key to the active time bucket, or a windowed aggregate fails with the existing typed no-time-dimension error because
the query projects no time dimension, and a dimension of the query is a
date-part extraction whose part is a built-in granularity (`year(x)`,
`quarter(x)`, `month(x)`, `week(x)`, `day(x)`, `hour(x)`, `minute(x)`,
`second(x)`, or `date_part('<part>', x)` with such a part, any case) of a DATE-
or TIMESTAMP-typed value, the error SHALL additionally name the remedy
`date_trunc('<part>', <expr>)` built from that extraction's part and argument,
one per such dimension. The failure SHALL otherwise be unchanged — same error
type, raised before any SQL is generated — and a query with no such dimension
SHALL keep the existing message. An extraction dimension SHALL never serve as a
transform's or window's time axis.

#### Scenario: Running total over a month-of-year dimension

- **WHEN** a query on `orders` has `dimensions=["month(created_at)"]`, no time
  dimension, and selects `cumsum(sum(amount))`
- **THEN** planning fails with the typed no-time-dimension error whose message
  names `date_trunc('month', created_at)`, and no SQL is generated

#### Scenario: Shift family and windowed aggregate name the remedy

- **WHEN** a query has `dimensions=["year(created_at)"]`, no time dimension, and
  selects `change(sum(amount))`, `last(sum(amount))`, or `sum(amount, window='3m')`
- **THEN** each fails with the typed no-time-dimension error naming
  `date_trunc('year', created_at)`

#### Scenario: The remedy executes

- **WHEN** the running-total query instead has
  `dimensions=["date_trunc('month', created_at)"]`
- **THEN** it executes, accumulating `sum(amount)` along the month buckets, by
  executed values

#### Scenario: No extraction keeps the existing message

- **WHEN** a query has `dimensions=["status"]`, no time dimension, and selects
  `cumsum(sum(amount))`
- **THEN** it fails with the existing no-time-dimension error, whose message
  names no `date_trunc` remedy

### Requirement: Transforms evaluate over their operand's domain
Every transform — the rank family (`rank`, `dense_rank`, `percent_rank`, `ntile`), `cumsum`, `lag`, `lead`, `time_shift`, `change`, `change_pct` and `consecutive_periods` — SHALL evaluate over its operand's domain and attach its result to the query's cells on its result grain, in every position (dimension, measure, filter, order) and whether or not the query names `population`. The domain SHALL be the cells of the operand's constituents at the operand grain: an aggregate constituent's cells are the grain tuples its filtered home rows reach (rows hidden by a model filter or a row-level-security policy do not exist); a nested transform constituent's cells are its own result cells; constituents at the full operand grain supply their cells, and a coarser constituent's cells extend to the agreeing cells of the finer constituents, else of the query's dimension anchor; an operand with no aggregate constituent takes the dimension anchor's cells. In a spine query the domain SHALL be the operand's non-time tuples that have a cell in a visible bucket, each crossed with every bucket in range. A query cell outside the domain SHALL read NULL for the transform. The query's rows, its other measures, its measure-typed filters and its order SHALL NOT change a transform's value in any cell, and a transform used only in a filter or the order SHALL supply no rows. Ranking, row offsets (`lag`, `lead`) and running totals SHALL step over the domain's cells only; calendar-step transforms keep stepping by calendar buckets. A NULL time bucket SHALL have no position on the time axis on every dialect: it is adjacent to no bucket, so a preserving time-ordered transform evaluates it as a series of its own (`cumsum` reads its own value; `lag`, `lead`, `time_shift`, `change` and `change_pct` read NULL there), and it never contributes to or reads from another bucket's value; the rank family treats it as an ordinary cell. A `first` / `last` over an aggregate is an aggregation, not a transform, and ranks exactly this domain's cells (`queries/semantics` › first and last over an aggregate rank their operand's cells).

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
