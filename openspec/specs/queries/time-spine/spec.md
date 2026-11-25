# queries/time-spine Specification

## Purpose
The built-in time spine: a virtual per-datasource calendar model every fact maps onto through its own time column, giving queries a shared time axis across facts and a population with every bucket in range (gap filling), derived from the existing population, attribution and filter rules.

## Requirements

### Requirement: Every datasource has a virtual time spine model

Every datasource SHALL expose a model named `time_spine` with exactly one column, `timestamp`, of type `time`, which is its primary key and denotes every instant. The model SHALL NOT be stored; it resolves in the datasource a query runs against. It SHALL appear in model listings, `inspect` and search as a built-in model, and its description SHALL list the models wired to it with their axis columns. Saving a user model whose name equals `time_spine` case-insensitively SHALL be rejected with a typed error, unless a model of that exact name and datasource is already stored; if a stored model is named exactly `time_spine`, any query referencing `time_spine` SHALL fail with a typed error naming the conflict, never silently using either model. A stored model whose name differs from `time_spine` only in case SHALL keep loading, saving and answering queries that spell it exactly; its load, its save and every query response that reads it SHALL carry the structured name-collision warning naming it and `time_spine`.

#### Scenario: Spine is listed with its wired models

- **WHEN** the models of the scenario datasource are listed and `time_spine` is inspected
- **THEN** `time_spine` appears as a built-in model with the single column `timestamp`, and its inspection names `orders` via `order_date` and `returns` via `return_date`

#### Scenario: Reserved name rejected on save

- **WHEN** a user saves a model named `time_spine`, and another named `Time_Spine`
- **THEN** each save fails with a typed error naming the reserved model and nothing is stored

#### Scenario: Pre-existing stored clash fails closed

- **WHEN** a model named `time_spine` is already stored in a datasource and a query there groups by `time_spine.timestamp` month
- **THEN** the query fails with a typed error naming the clash and asking to rename the stored model

#### Scenario: A pre-existing case variant keeps working and warns

- **WHEN** a model named `Time_Spine` is already stored in a datasource, it is loaded, saved unchanged, and a query rooted at `Time_Spine` counts its rows
- **THEN** the load, the save and the query each succeed and each carries the name-collision warning naming `Time_Spine` and `time_spine`

### Requirement: A dataset's axis joins the spine to-one

A dataset's axis SHALL be its declared `default_time_dimension`, else its only column of type `time` or `date`; a dataset with neither has no axis. For a query-backed model or a query stage, the axis SHALL be the propagated `default_time_dimension` of its source when that column survives in its schema, else its only temporal column. A dataset with an axis SHALL behave as if it declared a many-to-one join from its axis to `time_spine.timestamp` by equality; a DATE axis value denotes the instant at midnight of that day. The same effective default SHALL serve as the model's default time dimension for the transform time-axis tie-break and the `first` / `last` fallback ranking column. Saving a model whose declared `default_time_dimension` names a column whose type is not `time` or `date` SHALL be rejected with a typed error naming the model and the column.

The axis join, and a spine population's product with the spine, SHALL hold whatever names the dataset's relationships carry, in both directions: a relationship a dataset declares, named like the spine in any case, SHALL save, load and query without refusal, rename or warning, and every spine query SHALL attribute and route the dataset exactly as if that relationship had any other name. An explicit hop spelt like the spine from such a dataset SHALL stay ambiguous per the hop rules. No response, generated SQL, warning, error, `inspect` or search output SHALL spell the spine other than `time_spine`, and no suggested path SHALL be offered that does not resolve back to the route it describes.

#### Scenario: Declared and sole-column axes are both wired

- **WHEN** a query groups by `time_spine.timestamp` month over Jan–Jun and selects `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** orders attribute by `order_date` and returns by `return_date` (their only temporal column) with no broadcast warning

#### Scenario: Two temporal columns and no declared default means no axis

- **WHEN** a model `shipments` has `created_at` and `delivered_at`, no declared default, and a query groups by the spine month with `sum(shipments.weight)`
- **THEN** `shipments` has no route to the spine, and the measure resolves per `to_many_handling` as an unattributable dimension: by association with the `associated` warning by default, broadcast with a `broadcast` warning under `broadcast`, the typed error in `error` mode

#### Scenario: Stage with one temporal column is wired

- **WHEN** a query list's stage `firsts` groups `orders` by `customer_id` with `min(order_date)` named `first_order`, and the main stage groups by months Jan–Jun with `count(firsts.customer_id)`
- **THEN** the stage's axis is `first_order` and the counts are Jan 2, then 0 for Feb–Jun

#### Scenario: Sole-column default feeds transforms and first/last

- **WHEN** `customers` gains a `signed_up_at` column, a query rooted at `returns` has time dimensions `returns.return_date` and `customers.signed_up_at` at month with no `main_time_dimension` and selects `cumsum(sum(returns.amount))`, and another query rooted at `returns` selects `last(returns.amount)` with no time dimension
- **THEN** the running total orders by `return_date` months, and `last` is 5 (the latest `return_date`), exactly as with a declared `default_time_dimension: return_date`

#### Scenario: Non-temporal declared default rejected on save

- **WHEN** a user saves `returns` declaring `default_time_dimension: amount`, a number column
- **THEN** the save fails with a typed error naming `returns` and `amount`, and nothing is stored; a text column (the default type) is rejected the same way, and a `date` column is accepted

#### Scenario: A relationship named like the spine keeps the axis join

- **WHEN** `orders` is saved with a relationship named `time_spine` to `customers`, and a query groups by `time_spine.timestamp` month over Jan–Jun with `sum(orders.amount)`, on SQLite and on DuckDB
- **THEN** the save succeeds without a warning, orders attribute by `order_date` (150 in Jan, 70 in Feb), and the response carries neither an `unrelated_dataset` nor an `associated` warning

#### Scenario: The same relationship in a stored Ossie document

- **WHEN** an Ossie semantic-model document declaring `orders`, `customers` and a relationship named `time_spine` from `orders` to `customers` is stored, and the same spine query runs
- **THEN** it returns the rows and warnings it returns when the relationship has another name

#### Scenario: Every spine route through the dataset ignores the name

- **WHEN** `orders` has a relationship named `time_spine`, and these run: a query with population `orders` grouping by the spine month; a query rooted at `time_spine` grouping by month and by a field `orders` reaches; a spine query counting `customers` rows, `customers` having no axis of its own and reaching the spine across `orders`; and a spine query over a query-backed dataset whose source query reads `orders`
- **THEN** each returns the rows and warnings it returns when the relationship has another name

#### Scenario: An explicit hop spelt like the spine stays ambiguous

- **WHEN** `orders` has a relationship named `time_spine` and a query selects `orders.time_spine.region`; and, separately, a relationship named `Time_Spine` and a query selects `orders.Time_Spine.region`
- **THEN** each fails with the typed ambiguous-hop error naming the reference

#### Scenario: The spine is always spelt `time_spine`

- **WHEN** `orders` has a relationship named `time_spine`, the spine query above runs, `orders` is inspected, and a spine query that cannot route a field of `orders` fails
- **THEN** the response, its SQL, its warnings, the inspection and the error spell the spine only as `time_spine`, and any path the error suggests resolves to the route it describes

### Requirement: Spine routes are nearest-axis and the spine is never crossed

A dataset SHALL reach the spine by its nearest axis along a chain of provably to-one, executable hops (spelled per the binding walker's rules, including named parallel edges); the shortest such route SHALL win, and two or more distinct shortest routes SHALL fail with a typed ambiguous-route error naming each canonical route. A dataset with no such route SHALL reach the spine by association across its nearest fanning axis: the shortest executable route that crosses at least one fanning or unproven hop and ends at a dataset with an axis, never passing through the spine. In each bucket its aggregations SHALL run over its distinct rows associated with an axis row in that bucket and inside the spine bounds — rows hidden by a model filter or a row-level-security policy associating nothing — and the response SHALL carry the `associated` warning; under `to_many_handling: "broadcast"` its measures SHALL broadcast with the `broadcast` warning, and under `"error"` the query SHALL fail, both as before. Two or more distinct nearest fanning axes SHALL fail with the typed ambiguous-route error naming each route spelled as that fact's time column (e.g. `customers.orders.order_date`), with the remedy of querying one of them as a time dimension. A dataset with no route to any axis SHALL be associated with every bucket, as before. Each error SHALL be raised only by a query that needs that dataset's spine route. A join path MAY begin at the spine (as the population) or end at it, but SHALL NEVER pass through it: no route between two other datasets uses a spine edge. Routing that involves no spine edge SHALL be unchanged.

#### Scenario: A dataset without its own axis reaches the spine through its parent

- **WHEN** `order_items` (no temporal column) joins `orders` many-to-one and a query groups by the spine month with `sum(order_items.qty)`
- **THEN** each item is attributed to its order's `order_date` month

#### Scenario: Own axis beats a parent's axis

- **WHEN** `customers` gains a sole temporal column `signed_up_at` and a spine-month query selects `sum(orders.amount)`
- **THEN** orders attribute by their own `order_date` (one hop), not by their customer's signup (two hops)

#### Scenario: Equal-length routes fail loudly

- **WHEN** `shipments` (no temporal column) joins both `orders` and `returns` many-to-one and a spine-month query selects `count(shipments.id)`
- **THEN** the query fails with the typed ambiguous-route error naming `shipments.orders` and `shipments.returns`

#### Scenario: Existing queries never route through the spine

- **WHEN** a query rooted at `orders` grouped by `orders.order_date` month selects `sum(returns.amount)`
- **THEN** it resolves through `customers` per `to_many_handling` (associated by default, broadcast with a warning under `broadcast`), never through `time_spine`

#### Scenario: Active customers per month through a fanning axis
- **WHEN** a datasource's `customers` (C1 EAST, C2 EAST, C3 WEST, C4 NORTH) has no temporal column and `orders` (C1 2025-01-10, C1 2025-02-10, C2 2025-01-20, C3 2025-02-20) is its only dated child, and a query groups by the spine month over Jan–Mar and selects `count(customers.*)`, then again also grouping by `customers.region`
- **THEN** the counts are Jan 2, Feb 2, Mar 0 with the `associated` warning, and by region EAST (2, 1, 0) and WEST (0, 1, 0), with no NORTH row

#### Scenario: A model filter on the axis dataset limits the association
- **WHEN** that `orders` dataset declares the filter `status = 'completed'` (keeping the January orders of C1 and C2)
- **THEN** the counts are Jan 2, Feb 0, Mar 0

#### Scenario: Equal-distance fanning axes fail loudly
- **WHEN** a spine-month query on the scenario datasource, whose `customers` reaches both `orders` and `returns` across one fanning hop, selects `count(customers.id)`, once with measures and once as a dimension-only query by `customers.region`
- **THEN** each fails with the typed ambiguous-route error naming `customers.orders.order_date` and `customers.returns.return_date` and the remedy of a time dimension on one of them; with `to_many_handling: "broadcast"` the measure query broadcasts 3 per month as before, and grouping by month of `customers.orders.order_date` instead reads Jan 2 and Feb 1

#### Scenario: A to-one axis beats a fanning axis
- **WHEN** `customers` gains a sole temporal column `signed_up_at` and a spine-month query over Jan–Jun selects `count(customers.id)`
- **THEN** customers attribute by their signup month exactly — Jan 1, Apr 1, else 0 — with no `associated` warning

### Requirement: A spine query's population is the spine times P

A query with one or more time dimensions on `time_spine.timestamp` SHALL have population `time_spine × P`, with one row per combination of a spine bucket in range and a tuple of P. When the query names `population` without `population_inferred: true`, P SHALL be that model's distinct value combinations of the other dimensions among its filtered rows, and adding or removing a measure SHALL never change the row set. Otherwise P SHALL be the row supply of the other dimensions and time dimensions (`queries/population`): the union of each top-level measure's non-time tuples that have a cell in a visible bucket — so a group whose facts all lie outside the spine bounds is no row, and a home row whose dimension path is broken lands in the NULL tuple — the dimension anchor's tuples restricted by the spine bounds when there are no measures, and the one-row unit when there are no other dimensions; adding a measure SHALL never remove a row or change a value in an existing row. `population: time_spine` denotes the unit. The population SHALL be reported as `time_spine × P` for a named P and `time_spine × A` otherwise, A the dimension anchor of the other dimensions, and `row_sources` SHALL list the supplying homes and every anchor component used to extend coarser cells. This SHALL hold for every query of a query list alike: the main query and each named stage.

#### Scenario: Every month in range, both facts on one axis

- **WHEN** a query groups by months Jan–Jun and selects `sum(orders.amount)`, `sum(returns.amount)` and `count(orders.id)`
- **THEN** the rows are Jan (150, NULL, 2), Feb (70, 20, 1), Mar (NULL, 10, 0), Apr (NULL, NULL, 0), May (NULL, 5, 0), Jun (NULL, NULL, 0), with no warnings

#### Scenario: Per-group grid

- **WHEN** the query groups by `customers.region` and the spine month with `date_range: ["2025-01-01", "2025-03-31"]` and selects `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** there are 6 rows (regions N, S × Jan–Mar): N Jan (100, NULL), N Feb (70, 20), N Mar (NULL, NULL), S Jan (50, NULL), S Feb (NULL, NULL), S Mar (NULL, 10); region E, with no fact in range, is no row, the population is reported `time_spine × customers` and `row_sources` is `["orders", "returns"]`

#### Scenario: Explicit P and explicit spine

- **WHEN** the per-group query names `population: customers`, and the two-fact query names `population: time_spine`
- **THEN** the first returns 9 rows (regions N, S, E × Jan–Mar): the inferred twin's rows plus E with NULL in every month; the second returns exactly the rows of its inferred twin

#### Scenario: Two spine time dimensions

- **WHEN** a query groups by the spine at `year` and at `month` over Jan–Jun
- **THEN** there are 6 rows, each month paired with year 2025

#### Scenario: Measures never change the rows

- **WHEN** `avg(orders.amount)` is added to any spine query above, `sum(returns.amount)` is removed from the per-group query naming `population: customers`, and `sum(returns.amount)` is added to a per-group query over Mar–Jun selecting only `sum(orders.amount)`
- **THEN** no row is removed and no existing value changes, the named-population query keeps its rows, and the Mar–Jun query grows from no rows to regions N and S × Mar–Jun, its `sum(orders.amount)` NULL throughout

#### Scenario: A spine stage keeps every bucket

- **WHEN** a query list's stage `monthly` groups by the spine month with `date_range: ["2025-01-01", "2025-03-31"]` and selects `sum(orders.amount)` named `o`, once without `population` and once with `population: time_spine`, and the main query over `monthly` selects `sum(o)` and `count(*)`
- **THEN** both variants read 220 and 3: the stage has one row per month Jan–Mar, March included

#### Scenario: A per-group spine stage

- **WHEN** the stage `monthly` also groups by `customers.region`, and the main query over it groups by `region` and selects `sum(o)` and `count(*)`
- **THEN** the rows are N (170, 3) and S (50, 3)

#### Scenario: A spine stage feeding a spine query

- **WHEN** the main query over the stage `monthly` groups by months Jan–Jun and selects `sum(monthly.o)`
- **THEN** the stage's axis is its month column and the rows are Jan 150, Feb 70, then NULL for Mar–Jun

#### Scenario: A filter on a finer dataset keeps P
- **WHEN** the per-group query omits `population` and adds the filter `orders.amount > 60`
- **THEN** the rows are N Jan (100, NULL), N Feb (70, 20) and N Mar (NULL, NULL) — the returns restricted by association to customers with such an order — and the population is reported `time_spine × customers`

#### Scenario: An orphan beside the spine is not yet a row
- **WHEN** the per-group query omits `population` and an order references no customer
- **THEN** a NULL-region row appears for each month, carrying that order's amount in its month, and the population is reported `time_spine × customers`

#### Scenario: Tuples come from rows inside the frame
- **WHEN** the per-group query uses `date_range: ["2025-02-01", "2025-03-31"]` and selects only `sum(orders.amount)`, once without `population` and once with `population: customers`
- **THEN** the first returns N Feb 70 and N Mar NULL — S's only order lies before the frame — and the second returns regions N, S and E × Feb–Mar

#### Scenario: A windowed measure supplies the groups its window reaches
- **WHEN** the per-group query over Feb–Mar selects only `sum(orders.amount, window='2m')`
- **THEN** the rows are N Feb 170, N Mar 70, S Feb 50 and S Mar NULL: S's January order lands in February's window

#### Scenario: A dimension-only spine query over an axis-bearing anchor
- **WHEN** a spine query over Feb–Mar groups by `orders.id` with no measures, once without `population` and once with `population: orders`
- **THEN** the first returns order 3 in February and March only, and the second returns orders 1, 2 and 3 in both months

#### Scenario: A pinned inferred spine stage keeps inferred rows
- **WHEN** the per-group spine stage is saved and loaded, carrying `population: customers` with `population_inferred: true`, and a legacy saved stage carries `population: customers` without the marker
- **THEN** the first returns the inferred per-group rows (N and S) and the second the 9-row grid

### Requirement: Spine bounds decide which buckets exist

A spine query SHALL bound `time_spine.timestamp` from below with a frame bound (a `date_range` or a conjunctive comparison of the spine column against a time point); without one it SHALL fail with a typed error whose remedy names adding a lower bound. Without a stated upper bound the query SHALL be bounded by the period containing now, at the finest granularity among its spine time dimensions — the `this <granularity>` time point, read from the engine clock. A bucket SHALL exist iff its interval overlaps the bounded interval. Spine bounds SHALL restrict the rows of every home and of the dimension anchor like a field filter: through an axis reached over to-one hops a row counts only if its axis instant satisfies them, through a fanning axis by association, and a dataset with no route to any axis is not restricted. They SHALL decide which non-time tuples a query omitting `population` has (`A spine query's population is the spine times P`), and SHALL remain frame bounds: trailing windows and `time_shift` read rows before the lower bound.

#### Scenario: No lower bound

- **WHEN** a spine-month query, or a spine-month stage of a query list, has no `date_range` and no bound filter on `time_spine.timestamp`
- **THEN** it fails with the typed missing-lower-bound error before any SQL runs

#### Scenario: Mid-bucket lower bound keeps the bucket, not the rows

- **WHEN** the two-fact query uses `date_range: ["2025-01-15", "2025-02-28"]`
- **THEN** the rows are Jan (50, NULL, 1) and Feb (70, 20, 1) — January exists, but only the order of 2025-01-20 counts

#### Scenario: Exclusive period upper bound

- **WHEN** the query uses `date_range: ["2025-01", "2025-02"]` (periods; upper bound exclusive at 2025-03-01)
- **THEN** exactly January and February are returned, never March

#### Scenario: Implied upper bound is the current bucket

- **WHEN** the engine clock reads 2025-03-02 12:00 and the two-fact query uses `date_range: ["2025-01-01", null]`
- **THEN** the rows are Jan, Feb and Mar only, and March's `sum(returns.amount)` is 10 — the return dated 2025-03-03 lies in the current month and counts

#### Scenario: Implied upper bound follows the finest spine granularity

- **WHEN** the clock reads 2025-03-02 12:00 and a spine query at `day` uses `date_range: ["2025-02-27", null]`
- **THEN** the last row is 2025-03-02

#### Scenario: Bounds stay frame bounds

- **WHEN** a spine-month query with `date_range: ["2025-02-01", "2025-03-31"]` selects `time_shift(sum(orders.amount), -1)` and `sum(orders.amount, window='2m')`
- **THEN** February reads 150 and 220 (January's orders are reached), and March reads 70 and 70

#### Scenario: whole_periods_only excludes the current bucket

- **WHEN** the clock reads 2025-03-15 12:00 and the two-fact query sets `whole_periods_only` with `date_range: ["2025-01-01", null]`
- **THEN** only January and February are returned

### Requirement: Filters on a spine query follow the existing dispositions

A field-typed filter that does not reference the spine column SHALL restrict each measure's home, the dimension anchor and a named P by the existing filter rules (to-one inline, association otherwise, per `to_many_handling`), and so the tuples they supply, never changing a surviving cell's values beyond that restriction; it SHALL NEVER remove a spine bucket. A filter on the spine column that is not a frame bound SHALL fail with a typed error naming the filter and suggesting the fact's own time column.

#### Scenario: A fact filter keeps every month

- **WHEN** the two-fact Jan–Jun query adds the filter `orders.amount > 60`
- **THEN** all six months are returned; `sum(orders.amount)` is Jan 100, Feb 70, else NULL; `sum(returns.amount)` counts only returns of customers with an order over 60: Feb 20, May 5, else NULL

#### Scenario: A filter on P keeps every month

- **WHEN** the per-group query adds the filter `customers.region = 'N'`
- **THEN** each contributing measure's rows are restricted to region N's customers: only region N rows are returned, one per month Jan–Mar, with the N values of the per-group scenario

#### Scenario: Non-bound spine filter

- **WHEN** a spine-month query filters `date_part('day_of_week', time_spine.timestamp) = 1`
- **THEN** it fails with the typed error naming the filter and suggesting a time dimension on the fact's own column

### Requirement: The spine has no countable rows

An aggregation whose home is `time_spine` (any aggregation over `time_spine.timestamp`, or `count(*)` over a spine-rooted population) SHALL fail with a typed error. `time_spine.timestamp` SHALL be usable only as a time dimension with a granularity or inside a frame-bound filter; as a plain dimension, a computed-dimension operand, an order key other than its projected bucket, or in a raw-rows (`distinct_dimension_values: false`) query it SHALL fail with a typed error. When the offending plain dimension is a date-part extraction of `time_spine.timestamp` whose part is a built-in granularity (e.g. `month(time_spine.timestamp)`), the error's remedy SHALL name `date_trunc('<part>', time_spine.timestamp)`.

#### Scenario: Aggregation over the spine

- **WHEN** a spine-month query, or a spine-month stage of a query list, selects `count(*)` with `population: time_spine`, or `min(time_spine.timestamp)`
- **THEN** it fails with the typed no-countable-rows error

#### Scenario: Plain spine dimension

- **WHEN** a query lists `time_spine.timestamp` under `dimensions`, or sets `distinct_dimension_values: false` with a spine time dimension
- **THEN** it fails with a typed error naming the time-dimension remedy

#### Scenario: Extraction of the spine column as a dimension

- **WHEN** a query lists `month(time_spine.timestamp)` under `dimensions`
- **THEN** it fails with the typed error, whose remedy names `date_trunc('month', time_spine.timestamp)`, and no SQL is generated

### Requirement: Transforms and windows see the dense series

Over the spine, a transform's domain SHALL hold every bucket in range for each of its operand's non-time tuples that has a cell in a visible bucket (`queries/transforms` › Transforms evaluate over their operand's domain), and a trailing window SHALL evaluate at every bucket in range, so time-ordered transforms and trailing windows evaluate at empty buckets like any other; a tuple only a sibling measure supplies SHALL NOT change a transform's values.

#### Scenario: Transforms over a filled series

- **WHEN** the Jan–Jun spine query selects `change(sum(orders.amount))`, `cumsum(sum(orders.amount))`, `consecutive_periods(count(returns.id) > 0)`, `lag(sum(returns.amount))` and `sum(orders.amount, window='2m')`
- **THEN** in month order they read change NULL, -80, NULL, NULL, NULL, NULL; cumsum 150, 220, 220, 220, 220, 220; consecutive_periods 0, 1, 2, 0, 1, 0; lag NULL, NULL, 20, 10, NULL, 5; window 150, 220, 70, NULL, NULL, NULL

#### Scenario: A fill value

- **WHEN** the query selects `coalesce(sum(orders.amount), 0)` and `cumsum(coalesce(sum(orders.amount), 0))`
- **THEN** the empty months read 0 and the running total is 150, 220, 220, 220, 220, 220

#### Scenario: Per-group running totals ignore a sibling's tuples
- **WHEN** the per-group query over Feb–Mar selects `cumsum(sum(orders.amount))`, once alone and once with `sum(returns.amount)` added
- **THEN** N reads 70, 70 in both runs, and the S rows the returns add read NULL

### Requirement: Re-bucketing through the spine is typed

A spine time dimension SHALL obey the re-bucketing rule of every axis column it attributes a measure through: when a measure's axis column carries a granularity, a finer or non-nesting spine granularity SHALL fail with the typed re-bucketing error naming the column.

#### Scenario: Monthly model on a daily spine

- **WHEN** a query-backed model `monthly` (its axis a `month`-bucketed column) is queried on the spine at `day` with `sum(monthly.rev)`
- **THEN** planning fails with the typed re-bucketing error, and the same query at `quarter` executes

### Requirement: The spine is tenant-free and cache-stable

A row-level-security policy SHALL neither restrict nor reject the spine itself; the facts and P SHALL be scoped exactly as in any query. A spine query whose upper bound is implied SHALL be served from the result cache until the clock leaves the current bucket.

#### Scenario: Policy scopes the facts, not the calendar

- **WHEN** a policy scopes `customers` to region N and the two-fact Jan–Jun query runs
- **THEN** all six months are returned with only customer 1's values: orders Jan 100, Feb 70; returns Feb 20, May 5

#### Scenario: Cache hit within the bucket, miss after rollover

- **WHEN** a spine-month query with an implied upper bound runs twice with the clock inside the same month, then again after the clock enters the next month
- **THEN** the second run is a cache hit and the third runs against the database

### Requirement: Spine SQL is portable

The spine SHALL render on every Tier-1 dialect as a bucket series generated in SQL (never a stored table), crossed with P's distinct value combinations, with each fact's producer joined on its complete grain.

#### Scenario: Server dialects execute the spine

- **WHEN** the two-fact Jan–Jun query, and a spine query over one day at a datasource granularity `{base: minute, multiple: 15}`, run on PostgreSQL, MySQL, ClickHouse and SQL Server over DATE- and TIMESTAMP-typed axes
- **THEN** each returns the same rows as SQLite and DuckDB

#### Scenario: Large ranges

- **WHEN** a spine query at that 15-minute granularity spans one year on SQLite, DuckDB and PostgreSQL
- **THEN** it returns 35,040 rows (35,136 in a leap year) without a recursion-limit error

#### Scenario: Generated SQL is pinned

- **WHEN** the two-fact and per-group queries are rendered for postgres, sqlite, duckdb, tsql and bigquery
- **THEN** the SQL matches recorded golden baselines

### Requirement: The hand-made calendar pattern keeps working

A user-authored calendar model that facts join explicitly SHALL behave exactly as before the spine existed.

#### Scenario: Calendar model parity

- **WHEN** the two-fact query is run rooted at a user `calendar` model (one row per day, joined from `order_date` and `return_date`) and on `time_spine` over the same range
- **THEN** both return the same values

### Requirement: An expression is never an axis

A dataset's axis SHALL be a column, never an expression: a time dimension whose expression reads `time_spine.timestamp` other than as the whole bare column SHALL fail before any SQL is generated with a typed error naming the time dimension and the remedy of bucketing `time_spine.timestamp` itself. A time dimension whose expression is a date-part extraction of `time_spine.timestamp` (e.g. `month(time_spine.timestamp)`) is such an expression, and its remedy SHALL name `date_trunc('<unit>', time_spine.timestamp)`, `<unit>` the time dimension's granularity. An expression time dimension that does not read the spine SHALL be an ordinary dimension of the spine query's P, exactly as a fact column's time dimension is.

#### Scenario: Spine dimension over an expression rejected

- **WHEN** a query buckets `time_spine.timestamp + INTERVAL '1 day'` at `month`, or `coalesce(time_spine.timestamp, orders.order_date)` at `month`
- **THEN** planning fails with the typed error naming the time dimension and the remedy, and no SQL is generated

#### Scenario: A fact expression time dimension is a dimension of P

- **WHEN** the two-fact Jan–Jun spine-month query also buckets `coalesce(orders.shipped_at, orders.order_date)` at `month`
- **THEN** it executes with population `spine × P`, P carrying that bucket, with the same values per (spine month, bucket) as the query with a column holding the coalesced value

#### Scenario: Extraction of the spine column as a time dimension

- **WHEN** a query has `time_dimensions=[{"field": "month(time_spine.timestamp)", "granularity": "month"}]`
- **THEN** planning fails with the typed expression-axis error, whose remedy names `date_trunc('month', time_spine.timestamp)`, and no SQL is generated

### Requirement: The spine is reached in any case
Every model-name position a query writes — `population`, an extension's model, the join targets of an extension, and the first segment of every dotted path in its dimensions, time dimensions, measures, filters and order — SHALL reach the built-in `time_spine` when its unquoted spelling equals `time_spine` case-insensitively, exactly as `time_spine` would, in every query of a query list. A stage of the list whose name the spelling matches SHALL take precedence; a spelling that names a stored model exactly SHALL reach that model. When the datasource the query runs against stores a model whose name differs from `time_spine` only in case, any other spelling that is not exactly `time_spine` SHALL fail with the typed ambiguity error naming `time_spine` and that model. A quoted spelling SHALL reach the spine only when it is exactly `"time_spine"`. String literals and the rest of every text SHALL be left unchanged.

#### Scenario: Upper-case spine root
- **WHEN** a query has `population: TIME_SPINE`, the time dimension `TIME_SPINE.timestamp` by month over Jan–Feb, and the measure `sum(orders.amount)`
- **THEN** it returns the same rows and result keys as the same query spelled `time_spine`

#### Scenario: Mixed-case spine reference without a root
- **WHEN** a query without `population` groups by `Time_Spine.timestamp` month over Jan–Feb and sums `orders.amount`, once as a dimension filter `Time_Spine.timestamp >= '2024-01-01'` and once in its order
- **THEN** each returns the same rows as the query spelled `time_spine`

#### Scenario: Quoted and literal spellings are untouched
- **WHEN** a query groups by `"TIME_SPINE".timestamp` month, and another filters `orders.note = 'TIME_SPINE.timestamp'` while grouping by `time_spine.timestamp` month
- **THEN** the first fails with the unresolved-reference error and the second compares against the literal text unchanged

#### Scenario: A stage named like the spine wins
- **WHEN** a query list has a stage `Time_Spine` and its main query is rooted at `TIME_SPINE`
- **THEN** the main query reads the stage

#### Scenario: A stored case variant makes other spellings ambiguous
- **WHEN** a datasource already stores a model `Time_Spine` and queries there are rooted at `TIME_SPINE`, at `time_spine` and at `Time_Spine`
- **THEN** the first fails with the ambiguity error naming `time_spine` and `Time_Spine`, the second reads the built-in spine, and the third reads the stored model with the name-collision warning

#### Scenario: A case variant in another datasource is no ambiguity
- **WHEN** datasource `a` stores a model `Time_Spine` and a query against datasource `b` is rooted at `TIME_SPINE`
- **THEN** it reads datasource `b`'s built-in spine

### Requirement: A stored join never targets the spine
Saving a model SHALL be refused with a typed error naming the model and the join when a join's `to` equals `time_spine` case-insensitively and no stored model of its datasource is named exactly as written; the error SHALL state that the spine joins each dataset through its time axis, so a declared join to it is never needed. On load, a stored model below schema version 25 SHALL have each such join removed, with a structured warning naming the model and the join, and SHALL be persisted at the current version only by a save or `slayer storage upgrade`.

#### Scenario: A join to the spine is refused in any spelling
- **WHEN** a model `orders` is saved with a join to `time_spine` on `created_at = timestamp`, and again with the join target `TIME_SPINE`
- **THEN** each save fails with the typed error naming `orders` and the join, and nothing is stored

#### Scenario: A stored join to the spine is dropped on load
- **WHEN** a store written before schema version 25 holds a model `ev` without a time axis whose join targets `time_spine`, and a query groups by `time_spine.timestamp` month over Jan–Mar and sums `ev.amount`
- **THEN** `ev` loads without that join, with the warning naming `ev` and the join, the stored document is unchanged, and the query returns the monthly sums
