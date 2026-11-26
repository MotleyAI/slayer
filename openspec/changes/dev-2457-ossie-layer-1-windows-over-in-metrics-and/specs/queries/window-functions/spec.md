## Purpose

Defines SQL window functions (`function(args) OVER (…)`) in Mode B: where they may be written, which rows or cells each runs over, how NULLs are placed in a window's ordering, and what each database runs or refuses.

## ADDED Requirements

### Requirement: A window function is read in every Mode B position

`function(args) OVER ([PARTITION BY expr, …] [ORDER BY expr [ASC|DESC] [NULLS FIRST|LAST], …] [frame])` SHALL be read wherever Mode B is read: a field's portable `sql` and `OSSIE_SQL_2026` entry, a metric's `expression`, and a query's dimensions, time dimensions, metrics, filters and order keys. The frame SHALL be `ROWS` or `RANGE`, with `BETWEEN <bound> AND <bound>` or a single start bound, each bound `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING` or `UNBOUNDED FOLLOWING` with `n` a whole number. The function under `OVER` SHALL be one of: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `PERCENT_RANK()`, `CUME_DIST()`, `NTILE(n)`, `LAG(expr[, offset[, default]])`, `LEAD(expr[, offset[, default]])`, `FIRST_VALUE(expr)`, `LAST_VALUE(expr)`, `NTH_VALUE(expr, n)`, or an Ossie aggregation function SLayer evaluates as a plain aggregate, with or without `FILTER (WHERE …)`. Any other function under `OVER` — a SLayer-only aggregation such as `first`, `last` or `collect`, a custom aggregation, a scalar function — SHALL fail with a typed error naming the function. A window function at an argument count other than those above, `OVER` after anything but a function call, a frame whose bounds are out of order, a single `FOLLOWING` bound, an `NTILE` bucket count or `NTH_VALUE` position that is not a whole-number literal of at least 1, a `LAG` / `LEAD` offset that is not a whole-number literal of at least 0, and a `RANGE` frame with an offset over other than exactly one order key SHALL each fail with a typed error naming the construct. The arguments, partition keys and order keys of a window SHALL accept every well-typed expression, another window function included.

Scenarios of this capability use `orders(id, region, amount, ordered_at, customer_id)` with rows `(1, EU, 10, 2024-01-01, 1)`, `(2, EU, 20, 2024-01-02, 1)`, `(3, EU, 30, 2024-01-03, 2)`, `(4, US, 40, 2024-01-01, 2)`, `(5, US, 50, 2024-01-02, 3)`, `(6, US, 60, 2024-01-03, 3)`, `(7, AS, 70, 2024-01-01, 1)`; `customers(id, tier)` with rows `(1, gold)`, `(2, silver)`, `(3, gold)`, `(4, silver)`, dataset `orders` joining `customers` many-to-one on `customer_id`; and `scores(id, score, at)` with rows `(1, 10, 2024-01-01)`, `(2, NULL, 2024-01-02)`, `(3, 5, NULL)`. Unless a scenario names a database, it holds on SQLite and on DuckDB.

#### Scenario: Each ranking function
- **WHEN** a raw-row query lists `orders.id` and, in turn, `ROW_NUMBER() OVER (ORDER BY orders.amount)`, `RANK() OVER (ORDER BY orders.ordered_at)`, `DENSE_RANK() OVER (ORDER BY orders.ordered_at)`, `NTILE(2) OVER (ORDER BY orders.amount)`, `PERCENT_RANK() OVER (ORDER BY orders.amount)` and `CUME_DIST() OVER (ORDER BY orders.amount)`
- **THEN** orders `1` to `7` carry `1` to `7`; `1, 4, 6, 1, 4, 6, 1`; `1, 2, 3, 1, 2, 3, 1`; `1, 1, 1, 1, 2, 2, 2`; `(id - 1) / 6`; and `id / 7`

#### Scenario: Each offset function
- **WHEN** a raw-row query lists `orders.id` and, in turn, `LAG(orders.amount, 1, 0) OVER (ORDER BY orders.amount)`, `LEAD(orders.amount, 1, 0) OVER (ORDER BY orders.amount)`, `LAG(orders.amount) OVER (ORDER BY orders.amount)`, `FIRST_VALUE(orders.amount) OVER (ORDER BY orders.amount)`, `LAST_VALUE(orders.amount) OVER (ORDER BY orders.amount)` and `NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.amount)`
- **THEN** orders `1` to `7` carry `0, 10, 20, 30, 40, 50, 60`; `20, 30, 40, 50, 60, 70, 0`; `NULL, 10, 20, 30, 40, 50, 60`; `10` on every row; each row's own `amount`; and `NULL` for order `1` and `20` for the others

#### Scenario: Window aggregations
- **WHEN** a raw-row query lists `orders.id` and, in turn, `SUM(orders.amount) OVER (PARTITION BY orders.region)`, `COUNT(*) OVER (PARTITION BY orders.region)`, `orders.amount / SUM(orders.amount) OVER () * 100`, `AVG(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` and `SUM(orders.amount) FILTER (WHERE orders.amount > 10) OVER (PARTITION BY orders.region)`
- **THEN** orders `1` to `7` carry `60, 60, 60, 150, 150, 150, 70`; `3, 3, 3, 3, 3, 3, 1`; `25` for order `7` and `amount / 2.8` for each; `10, 15, 20, 25, 30, 35, 40`; and `50, 50, 50, 150, 150, 150, 70`

#### Scenario: The three listed frames
- **WHEN** a raw-row query lists `orders.id` and, in turn, `SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`, `SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)` and `SUM(orders.amount) OVER (ORDER BY orders.ordered_at RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
- **THEN** orders `1` to `7` carry `10, 30, 60, 100, 150, 210, 280`; `30, 60, 90, 120, 150, 180, 130`; and `120, 190, 280, 120, 190, 280, 120`

#### Scenario: A frame on a ranking function changes nothing
- **WHEN** a raw-row query lists `orders.id` and `RANK() OVER (ORDER BY orders.amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)`, on every Tier-1 database
- **THEN** orders `1` to `7` carry `1` to `7`

#### Scenario: A function Ossie does not window is refused
- **WHEN** a query's dimension is `last(orders.amount, orders.ordered_at) OVER ()`, `collect(orders.region) OVER ()` or `UPPER(orders.region) OVER ()`
- **THEN** each fails with a typed error naming the function

#### Scenario: Malformed windows are refused
- **WHEN** a query's dimension is `ROW_NUMBER(orders.amount) OVER ()`, `orders.amount OVER ()`, `NTILE(0) OVER (ORDER BY orders.amount)`, `NTILE(orders.id) OVER (ORDER BY orders.amount)`, `LAG(orders.amount, -1) OVER (ORDER BY orders.amount)`, `NTH_VALUE(orders.amount, 0) OVER (ORDER BY orders.amount)`, `SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)` or `SUM(orders.amount) OVER (ORDER BY orders.id, orders.amount RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)`
- **THEN** each fails with a typed error naming the construct, before any SQL is run

#### Scenario: A window inside a window
- **WHEN** a raw-row query lists `orders.id` and `MAX(ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)) OVER ()`
- **THEN** every order carries `3`

### Requirement: A window with no aggregate-valued operand is a windowed column of one dataset

A window function whose arguments, partition keys and order keys hold no aggregate, transform or attached value SHALL be row-level: a windowed column of one dataset (`models/field-expressions` › A windowed field is a column of its dataset), computed over that dataset's rows passing its `filters` and the session policy, before any join, query filter or aggregation reads it, and then read as a column. Written in a field's `expression` or `filter`, its dataset SHALL be the declaring dataset. Written in a query, its dataset SHALL be the dataset its references are anchored at; references anchored at different datasets SHALL meet at the smallest dataset determining each anchor over provably to-one hops, tied candidates failing as ambiguous; a window anchored at a query stage SHALL run over that stage's rows. A row-level window in a query with no reference, or whose only references are to the time spine, SHALL fail with a typed error. A row-level window in a query SHALL be accepted in every position a windowed field is: dimension, time dimension, aggregation source, inside `FILTER (WHERE …)` and `CASE`, partition key, ranking key, order key, query filter and raw rows; in metric position it SHALL follow the rules of an aggregate-free measure. Its value SHALL NOT depend on the query's filters, on which dataset roots the query, or on the joins that reach it, and SHALL equal the value of a field of its dataset declared with the same text. Two occurrences of one window over one dataset in a query SHALL be computed once. Under a query zone, a timestamp read inside a row-level window SHALL be the query zone's wall clock, while the dataset's `filters`, field filters and session policy beneath the window SHALL see stored values.

#### Scenario: Inline as a dimension
- **WHEN** a query has the dimension `ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)` and the metric `sum(orders.amount)`
- **THEN** it returns `120` for `1`, `70` for `2` and `90` for `3`

#### Scenario: A query filter does not change the window
- **WHEN** a raw-row query lists `orders.id` and `ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)` with the filter `orders.id > 1`
- **THEN** orders `2` to `7` carry `2, 3, 1, 2, 3, 1`, the values computed over every order

#### Scenario: Inline as a query filter
- **WHEN** a query has the dimension `orders.region`, the metric `sum(orders.amount)` and the filter `ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at) = 1`
- **THEN** it returns `10` for EU, `40` for US and `70` for AS

#### Scenario: Inline as an aggregation source and inside FILTER
- **WHEN** a query has the metrics `max(ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at))` and `sum(orders.amount) FILTER (WHERE ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at) = 1)` and no dimension
- **THEN** it returns `3` and `120`

#### Scenario: Inline as a partition key, a ranking key and an order key
- **WHEN** queries use `ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)` where `models/field-expressions` › A windowed field is a column of its dataset uses `orders.rn` as a partition key, as a ranking key and as an order key of a grouped query
- **THEN** each returns the values that requirement's scenario gives

#### Scenario: The anchor decides whose rows are numbered
- **WHEN** a raw-row query rooted at `orders` lists `orders.id` with `RANK() OVER (ORDER BY orders.customers.id DESC)`, and another with `RANK() OVER (ORDER BY customers.id DESC)`
- **THEN** in the first, orders `1` to `7` carry `5, 5, 3, 3, 1, 1, 5`, ranks over the seven orders; in the second they carry `4, 4, 3, 3, 2, 2, 4`, ranks over the four customers

#### Scenario: A window reading a to-one joined field
- **WHEN** a raw-row query lists `orders.id` and `ROW_NUMBER() OVER (PARTITION BY orders.customers.tier ORDER BY orders.id)`
- **THEN** orders `1` to `7` carry `1, 2, 1, 2, 3, 4, 5`

#### Scenario: Inline equals the field
- **WHEN** field `orders.rn` is declared with the portable expression `ROW_NUMBER() OVER (PARTITION BY region ORDER BY ordered_at)`, and a raw-row query lists `orders.id`, `orders.rn` and `ROW_NUMBER() OVER (PARTITION BY orders.region ORDER BY orders.ordered_at)`
- **THEN** the two columns are equal on every row, and the emitted statement computes the window once

#### Scenario: A window with no reference is refused
- **WHEN** a query's dimension is `ROW_NUMBER() OVER ()`, and another's is a window whose only reference is a time-spine field
- **THEN** each fails with a typed error naming the window and stating that it names no dataset

#### Scenario: A window over a query stage
- **WHEN** a query's first stage `by_region` has the dimension `orders.region` and the metric `sum(orders.amount)` named `total`, and its final stage lists raw rows `by_region.region` and `RANK() OVER (ORDER BY by_region.total DESC)`
- **THEN** it returns `1` for US, `2` for AS and `3` for EU

#### Scenario: A row-level window as a metric
- **WHEN** a query has the dimension `orders.region` and the metric `SUM(orders.amount) OVER (PARTITION BY orders.region)`
- **THEN** it fails with the position typing error for an aggregate-free measure over a non-dimension column, as the metric `round(orders.amount, 2)` does

#### Scenario: The query zone applies inside the window
- **WHEN** dataset `events(id, at)` holds the UTC instants `(1, 2024-01-01 22:30)` and `(2, 2024-01-01 23:30)`, and a raw-row query in zone `Europe/Berlin` lists `events.id` and `ROW_NUMBER() OVER (PARTITION BY CAST(events.at AS DATE) ORDER BY events.id)`
- **THEN** both events carry `1`, the two instants falling on different Berlin days; with no query zone they carry `1` and `2`

#### Scenario: The session policy restricts the rows before the window
- **WHEN** a session policy restricts `orders` to `region = 'US'` and a raw-row query lists `orders.id` and `ROW_NUMBER() OVER (ORDER BY orders.id)`
- **THEN** it returns orders `4`, `5` and `6` with `1`, `2` and `3`

### Requirement: A window in a field's filter masks on its computed value

A field's `filter` SHALL accept a window function written in the datasource's SQL. The window SHALL be a windowed column of the declaring dataset, and the filter SHALL mask the field's value on it. NULL placement inside it SHALL be the database's own, as in a dialect entry.

#### Scenario: A mask on a window
- **WHEN** field `orders.first_amount` has the expression `amount` and the filter `row_number() over (partition by region order by ordered_at) = 1`, and a query has the metric `sum(orders.first_amount)` and no dimension
- **THEN** it returns `120`, and the dataset saves through the engine and through the storage backend

### Requirement: A window with an aggregate-valued operand is a transform over its cells

A window function with an aggregate, a transform or an attached value in its arguments, partition keys or order keys SHALL be measure-level: a transform whose result grain is its operand grain, evaluated with SQL's meaning over the cells of its operand's domain (`queries/transforms` › Transforms evaluate over their operand's domain). Each partition key and order key SHALL be aggregate-valued or a member of the operand grain; any other key, and any other row-level reference outside an aggregate, SHALL fail with a typed error naming it and the remedy of adding it as a dimension. Query dimensions the window does not name SHALL NOT partition it. A step SHALL be a cell of the domain, never a calendar step. A cell outside the domain SHALL read NULL, so a sibling measure, a named population or a measure filter SHALL never change the window's value in a surviving cell. A measure-level window SHALL be accepted wherever a transform is: metric, order key, measure filter, second-order aggregation source, aggregation parameter, and dimension position under the explicit-grain rule. A dataset-scoped and a model-scoped metric whose expression is a measure-level window SHALL be saved and evaluated.

#### Scenario: A running total over the query's cells
- **WHEN** a query has the dimension `orders.ordered_at` and the metric `SUM(SUM(orders.amount)) OVER (ORDER BY orders.ordered_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
- **THEN** it returns `120`, `190` and `280` for the three days in order

#### Scenario: A period-over-period change
- **WHEN** a query has the dimension `orders.ordered_at` and the metric `SUM(orders.amount) - LAG(SUM(orders.amount), 1) OVER (ORDER BY orders.ordered_at)`
- **THEN** it returns NULL, `-50` and `20` for the three days in order

#### Scenario: A rank within a partition
- **WHEN** a query has the dimensions `orders.region` and `orders.ordered_at` and the metric `RANK() OVER (PARTITION BY orders.region ORDER BY SUM(orders.amount) DESC)`
- **THEN** within EU and within US the three days rank `3, 2, 1` in date order, and the AS row ranks `1`

#### Scenario: A key that is not a dimension is refused
- **WHEN** a query has the dimension `orders.region` and the metric `SUM(SUM(orders.amount)) OVER (ORDER BY orders.ordered_at)`
- **THEN** it fails with a typed error naming `orders.ordered_at` and the remedy of adding it as a dimension

#### Scenario: Extra dimensions do not partition the window
- **WHEN** a query has the dimensions `orders.region` and `orders.ordered_at` and the metric `SUM(SUM(orders.amount)) OVER ()`
- **THEN** every row carries `280`

#### Scenario: The window runs over its own cells
- **WHEN** a query with the population `customers` has the dimension `customers.id` and the metric `RANK() OVER (ORDER BY SUM(orders.amount) DESC)`
- **THEN** it returns `2` for customer `1`, `3` for customer `2`, `1` for customer `3` and NULL for customer `4`, who has no orders

#### Scenario: A measure filter does not change the window
- **WHEN** a query has the dimension `orders.region`, the metric `RANK() OVER (ORDER BY SUM(orders.amount) DESC)` and the filter `sum(orders.amount) < 100`
- **THEN** it returns `2` for AS and `3` for EU

#### Scenario: A stored windowed metric
- **WHEN** dataset `orders` declares the metric `revenue_rank` with expression `RANK() OVER (ORDER BY SUM(amount) DESC)`, saved through the engine and through the storage backend, and a query has the dimension `orders.region` and the metric `orders.revenue_rank`
- **THEN** both saves succeed and the query returns `1` for US, `2` for AS and `3` for EU

#### Scenario: A measure-level window as an aggregation source
- **WHEN** a query has no dimension and the metric `max(RANK() OVER (ORDER BY sum(orders.amount, partition_by=orders.region) DESC))`
- **THEN** it returns `3`

### Requirement: NULL sorts as the largest value in a window's ordering

In Mode B, an order item of a window that omits `NULLS FIRST` and `NULLS LAST` SHALL place NULL as the largest value — last for `ASC`, first for `DESC` — and an item that states a placement SHALL have it, on every database and whatever its session's default NULL ordering. This SHALL hold for a field's portable `sql`, an `OSSIE_SQL_2026` entry, a document's `ANSI_SQL` entry read as Mode B, a metric's `expression` and query text. Placing NULLs SHALL NOT change which rows are peers of one another. A window in a datasource-dialect entry or in a field's `filter` SHALL keep the database's own placement. Where a database cannot give an item its placement, the query SHALL fail with a typed error naming the item and the database rather than place NULLs differently.

#### Scenario: The default placement
- **WHEN** a raw-row query lists `scores.id` with `ROW_NUMBER() OVER (ORDER BY scores.score)` and with `ROW_NUMBER() OVER (ORDER BY scores.score DESC)`, on every Tier-1 database
- **THEN** scores `1`, `2`, `3` carry `2, 3, 1` and `2, 1, 3`

#### Scenario: A stated placement
- **WHEN** a raw-row query lists `scores.id` with `ROW_NUMBER() OVER (ORDER BY scores.score NULLS FIRST)` and with `ROW_NUMBER() OVER (ORDER BY scores.score DESC NULLS LAST)`, on every Tier-1 database
- **THEN** scores `1`, `2`, `3` carry `3, 1, 2` and `1, 3, 2`

#### Scenario: A session default does not move NULLs
- **WHEN** a DuckDB session sets `default_null_order` to `nulls_first` and a raw-row query lists `scores.id` and `ROW_NUMBER() OVER (ORDER BY scores.score)`
- **THEN** scores `1`, `2`, `3` carry `2, 3, 1`

#### Scenario: Peers survive the placement
- **WHEN** table `ties(id, a, b)` holds `(1, NULL, NULL)`, `(2, NULL, NULL)`, `(3, 1, NULL)`, `(4, 1, 2)`, and a raw-row query lists `ties.id` with `RANK() OVER (ORDER BY ties.a, ties.b DESC)`, `DENSE_RANK() OVER (ORDER BY ties.a, ties.b DESC)`, `CUME_DIST() OVER (ORDER BY ties.a)` and `COUNT(*) OVER (ORDER BY ties.a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`, on every Tier-1 database
- **THEN** rows `1` to `4` carry `3, 3, 1, 2`; `3, 3, 1, 2`; `1, 1, 0.5, 0.5`; and `4, 4, 2, 2`

#### Scenario: A placement the database cannot give is refused
- **WHEN** on MySQL a raw-row query lists `SUM(scores.score) OVER (ORDER BY scores.score RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)`, and another lists the same window with `ORDER BY scores.score NULLS FIRST`
- **THEN** the first fails with a typed error naming the order item and MySQL, and the second returns `10`, NULL and `5` for scores `1`, `2`, `3`; on DuckDB the first returns `10`, NULL and `5`

#### Scenario: A dialect entry keeps the database's placement
- **WHEN** on DuckDB field `scores.rn_native` is the dialect entry `ROW_NUMBER() OVER (ORDER BY score DESC)` and field `scores.rn_portable` the portable expression of the same text
- **THEN** `rn_native` is `1, 3, 2` for scores `1`, `2`, `3`, DuckDB placing NULL last, and `rn_portable` is `2, 1, 3`

### Requirement: Every database runs the required windows or refuses with a typed error

On each Tier-1 database every window function of "A window function is read in every Mode B position" over `SUM`, `COUNT`, `COUNT(*)`, `AVG`, `MIN`, `MAX`, `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP` and `VAR_SAMP`, the ranking functions and the offset functions, with no frame or with one of Ossie's three frame forms, SHALL return the value this capability defines, whether the database has the construct or not. `COUNT(DISTINCT x)`, `MEDIAN` and `PERCENTILE_CONT` under `OVER` SHALL return that value on a database that evaluates them as window functions and SHALL otherwise fail with a typed error naming the function and the database. A frame outside Ossie's three forms SHALL return its value on a database that has it and SHALL otherwise fail with a typed error naming the frame and the database. No window SHALL reach a database as SQL it rejects.

#### Scenario: The matrix
- **WHEN** the scenarios "Each ranking function", "Each offset function", "Window aggregations" and "The three listed frames" run on Postgres, MySQL, ClickHouse, SQL Server and Trino
- **THEN** each returns the values those scenarios give; on BigQuery and Snowflake each compiles to a statement holding the window

#### Scenario: A holistic aggregate where the database windows it
- **WHEN** on DuckDB a raw-row query lists `orders.id` and `MEDIAN(orders.amount) OVER ()`
- **THEN** every order carries `40`

#### Scenario: A holistic aggregate where the database does not
- **WHEN** on SQLite and on Postgres a raw-row query lists `MEDIAN(orders.amount) OVER ()`, and another `COUNT(DISTINCT orders.region) OVER ()`
- **THEN** each fails with a typed error naming the function and the database

### Requirement: first and last differ from FIRST_VALUE and LAST_VALUE on NULL keys

`first(x, t)` and `last(x, t)` SHALL keep their rule (`aggregations/functional-form` › first and last never pick a NULL ranking key). `FIRST_VALUE(x) OVER (… ORDER BY t)` and `LAST_VALUE(x) OVER (… ORDER BY t)` SHALL order by "NULL sorts as the largest value in a window's ordering" and so MAY return the value of a row whose key is NULL. Neither form SHALL be rewritten into the other: importing an Ossie document SHALL keep its window form, and exporting `first` / `last` SHALL never write `FIRST_VALUE` or `LAST_VALUE`.

#### Scenario: last and LAST_VALUE differ
- **WHEN** a query has the metric `last(scores.id, scores.at)` and no dimension, and a raw-row query lists `LAST_VALUE(scores.id) OVER (ORDER BY scores.at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`
- **THEN** the first returns `2` and the second `3` on every row

#### Scenario: first and FIRST_VALUE differ
- **WHEN** a query has the metric `first(scores.id, scores.at)` and no dimension, and a raw-row query lists `FIRST_VALUE(scores.id) OVER (ORDER BY scores.at NULLS FIRST)`
- **THEN** the first returns `1` and the second `3` on every row

#### Scenario: Export never writes a window for first or last
- **WHEN** a dataset with the measures `latest: last(amount, ordered_at)` and `earliest: first(amount, ordered_at)` is exported, with and without extensions
- **THEN** no exported expression holds `FIRST_VALUE` or `LAST_VALUE`
