# queries/time-spine Specification

## Purpose
The built-in time spine: a virtual per-datasource calendar model every fact maps onto through its own time column, giving queries a shared time axis across facts and a population with every bucket in range (gap filling), derived from the existing population, attribution and filter rules.

## Requirements

### Requirement: Every datasource has a virtual time spine model

Every datasource SHALL expose a model named `time_spine` with exactly one column, `timestamp`, of type `time`, which is its primary key and denotes every instant. The model SHALL NOT be stored; it resolves in the datasource a query runs against. It SHALL appear in model listings, `inspect`, `inspect_model` and search as a built-in model, and its description SHALL list the models wired to it with their axis columns. Saving a user model named `time_spine` SHALL be rejected with a typed error; if a stored model already has that name, any query referencing `time_spine` SHALL fail with a typed error naming the conflict, never silently using either model.

#### Scenario: Spine is listed with its wired models

- **WHEN** the models of the scenario datasource are listed and `time_spine` is inspected
- **THEN** `time_spine` appears as a built-in model with the single column `timestamp`, and its inspection names `orders` via `order_date` and `returns` via `return_date`

#### Scenario: Reserved name rejected on save

- **WHEN** a user saves a model named `time_spine`
- **THEN** the save fails with a typed error naming the reserved model and nothing is stored

#### Scenario: Pre-existing stored clash fails closed

- **WHEN** a model named `time_spine` is already stored in a datasource and a query there groups by `time_spine.timestamp` month
- **THEN** the query fails with a typed error naming the clash and asking to rename the stored model

### Requirement: A dataset's axis joins the spine to-one

A dataset's axis SHALL be its declared `default_time_dimension`, else its only column of type `time` or `date`; a dataset with neither has no axis. For a query-backed model or a query stage, the axis SHALL be the propagated `default_time_dimension` of its source when that column survives in its schema, else its only temporal column. A dataset with an axis SHALL behave as if it declared a many-to-one join from its axis to `time_spine.timestamp` by equality; a DATE axis value denotes the instant at midnight of that day. The same effective default SHALL serve as the model's default time dimension for the transform time-axis tie-break and the `first` / `last` fallback ranking column. Saving a model whose declared `default_time_dimension` names a column whose type is not `time` or `date` SHALL be rejected with a typed error naming the model and the column.

#### Scenario: Declared and sole-column axes are both wired

- **WHEN** a query groups by `time_spine.timestamp` month over Jan–Jun and selects `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** orders attribute by `order_date` and returns by `return_date` (their only temporal column) with no broadcast warning

#### Scenario: Two temporal columns and no declared default means no axis

- **WHEN** a model `shipments` has `created_at` and `delivered_at`, no declared default, and a query groups by the spine month with `sum(shipments.weight)`
- **THEN** `shipments` has no route to the spine, and the measure resolves per `to_many_handling` as an unattributable dimension: broadcast with a `broadcast` warning by default, the typed error in `error` mode

#### Scenario: Stage with one temporal column is wired

- **WHEN** a query list's stage `firsts` groups `orders` by `customer_id` with `min(order_date)` named `first_order`, and the main stage groups by months Jan–Jun with `count(firsts.customer_id)`
- **THEN** the stage's axis is `first_order` and the counts are Jan 2, then 0 for Feb–Jun

#### Scenario: Sole-column default feeds transforms and first/last

- **WHEN** `customers` gains a `signed_up_at` column, a query rooted at `returns` has time dimensions `returns.return_date` and `customers.signed_up_at` at month with no `main_time_dimension` and selects `cumsum(sum(returns.amount))`, and another query rooted at `returns` selects `last(returns.amount)` with no time dimension
- **THEN** the running total orders by `return_date` months, and `last` is 5 (the latest `return_date`), exactly as with a declared `default_time_dimension: return_date`

#### Scenario: Non-temporal declared default rejected on save

- **WHEN** a user saves `returns` declaring `default_time_dimension: amount`, a number column
- **THEN** the save fails with a typed error naming `returns` and `amount`, and nothing is stored; a text column (the default type) is rejected the same way, and a `date` column is accepted

### Requirement: Spine routes are nearest-axis and the spine is never crossed

A dataset SHALL reach the spine by its nearest axis along a chain of provably to-one, executable hops (spelled per the binding walker's rules, including named parallel edges); the shortest such route SHALL win, and two or more distinct shortest routes SHALL fail with a typed ambiguous-route error naming each canonical route. The error SHALL be raised only by a query that needs that dataset's spine route. A join path MAY begin at the spine (as the population) or end at it, but SHALL NEVER pass through it: no route between two other datasets uses a spine edge. Routing that involves no spine edge SHALL be unchanged.

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
- **THEN** it resolves exactly as before this change (through `customers`, broadcast with a warning), never through `time_spine`

### Requirement: A spine query's population is the spine times P

A query with one or more time dimensions on `time_spine.timestamp` SHALL have population `time_spine × P`, where P is the population of its remaining determination items — its other dimensions and time dimensions and the field-typed filters not on the spine column — inferred by the dimension-determined default, or the model named by `source_model`, or the one-row unit when there are no remaining items (`source_model: time_spine` also denotes the unit). The result SHALL have one row per combination of a spine bucket in range and a distinct value combination of P's dimensions among P's filtered rows; adding or removing a measure SHALL never change this row set. This SHALL hold for every query of a query list alike: the main query and each named stage.

#### Scenario: Every month in range, both facts on one axis

- **WHEN** a query groups by months Jan–Jun and selects `sum(orders.amount)`, `sum(returns.amount)` and `count(orders.id)`
- **THEN** the rows are Jan (150, NULL, 2), Feb (70, 20, 1), Mar (NULL, 10, 0), Apr (NULL, NULL, 0), May (NULL, 5, 0), Jun (NULL, NULL, 0), with no warnings

#### Scenario: Per-group grid

- **WHEN** the query groups by `customers.region` and the spine month with `date_range: ["2025-01-01", "2025-03-31"]` and selects `sum(orders.amount)` and `sum(returns.amount)`
- **THEN** there are 9 rows (regions N, S, E × Jan–Mar): N Jan (100, NULL), N Feb (70, 20), N Mar (NULL, NULL), S Jan (50, NULL), S Feb (NULL, NULL), S Mar (NULL, 10), and E NULL in every month

#### Scenario: Explicit P and explicit spine

- **WHEN** the per-group query names `source_model: customers`, and the two-fact query names `source_model: time_spine`
- **THEN** each returns exactly the rows of its inferred twin

#### Scenario: Two spine time dimensions

- **WHEN** a query groups by the spine at `year` and at `month` over Jan–Jun
- **THEN** there are 6 rows, each month paired with year 2025

#### Scenario: Measures never change the rows

- **WHEN** `sum(returns.amount)` is removed from, or `avg(orders.amount)` added to, any spine query above
- **THEN** the row set is unchanged

#### Scenario: A spine stage keeps every bucket

- **WHEN** a query list's stage `monthly` groups by the spine month with `date_range: ["2025-01-01", "2025-03-31"]` and selects `sum(orders.amount)` named `o`, once without `source_model` and once with `source_model: time_spine`, and the main query over `monthly` selects `sum(o)` and `count(*)`
- **THEN** both variants read 220 and 3: the stage has one row per month Jan–Mar, March included

#### Scenario: A per-group spine stage

- **WHEN** the stage `monthly` also groups by `customers.region`, and the main query over it groups by `region` and selects `sum(o)` and `count(*)`
- **THEN** the rows are N (170, 3), S (50, 3), E (NULL, 3)

#### Scenario: A spine stage feeding a spine query

- **WHEN** the main query over the stage `monthly` groups by months Jan–Jun and selects `sum(monthly.o)`
- **THEN** the stage's axis is its month column and the rows are Jan 150, Feb 70, then NULL for Mar–Jun

### Requirement: Spine bounds decide which buckets exist

A spine query SHALL bound `time_spine.timestamp` from below with a frame bound (a `date_range` or a conjunctive comparison of the spine column against a time point); without one it SHALL fail with a typed error whose remedy names adding a lower bound. Without a stated upper bound the query SHALL be bounded by the period containing now, at the finest granularity among its spine time dimensions — the `this <granularity>` time point, read from the engine clock. A bucket SHALL exist iff its interval overlaps the bounded interval. Spine bounds SHALL restrict each fact's rows through its axis (a fact row counts only if its axis instant satisfies them) and SHALL remain frame bounds: trailing windows and `time_shift` read rows before the lower bound.

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

A field-typed filter that does not reference the spine column SHALL restrict P's rows (and so P's value combinations) and SHALL reach each measure's home by the existing filter rules (to-one inline, association otherwise, per `to_many_handling`); it SHALL NEVER remove a spine bucket. A filter on the spine column that is not a frame bound SHALL fail with a typed error naming the filter and suggesting the fact's own time column.

#### Scenario: A fact filter keeps every month

- **WHEN** the two-fact Jan–Jun query adds the filter `orders.amount > 60`
- **THEN** all six months are returned; `sum(orders.amount)` is Jan 100, Feb 70, else NULL; `sum(returns.amount)` counts only returns of customers with an order over 60: Feb 20, May 5, else NULL

#### Scenario: A filter on P keeps every month

- **WHEN** the per-group query adds the filter `customers.region = 'N'`
- **THEN** only region N rows are returned, one per month Jan–Mar, with the N values of the per-group scenario

#### Scenario: Non-bound spine filter

- **WHEN** a spine-month query filters `date_part('day_of_week', time_spine.timestamp) = 1`
- **THEN** it fails with the typed error naming the filter and suggesting a time dimension on the fact's own column

### Requirement: The spine has no countable rows

An aggregation whose home is `time_spine` (any aggregation over `time_spine.timestamp`, or `count(*)` over a spine-rooted population) SHALL fail with a typed error. `time_spine.timestamp` SHALL be usable only as a time dimension with a granularity or inside a frame-bound filter; as a plain dimension, a computed-dimension operand, an order key other than its projected bucket, or in a raw-rows (`distinct_dimension_values: false`) query it SHALL fail with a typed error.

#### Scenario: Aggregation over the spine

- **WHEN** a spine-month query, or a spine-month stage of a query list, selects `count(*)` with `source_model: time_spine`, or `min(time_spine.timestamp)`
- **THEN** it fails with the typed no-countable-rows error

#### Scenario: Plain spine dimension

- **WHEN** a query lists `time_spine.timestamp` under `dimensions`, or sets `distinct_dimension_values: false` with a spine time dimension
- **THEN** it fails with a typed error naming the time-dimension remedy

### Requirement: Transforms and windows see the dense series

Over a spine population every bucket in range SHALL be a row of each partition, so time-ordered transforms and trailing windows evaluate at empty buckets like any other.

#### Scenario: Transforms over a filled series

- **WHEN** the Jan–Jun spine query selects `change(sum(orders.amount))`, `cumsum(sum(orders.amount))`, `consecutive_periods(count(returns.id) > 0)`, `lag(sum(returns.amount))` and `sum(orders.amount, window='2m')`
- **THEN** in month order they read change NULL, -80, NULL, NULL, NULL, NULL; cumsum 150, 220, 220, 220, 220, 220; consecutive_periods 0, 1, 2, 0, 1, 0; lag NULL, NULL, 20, 10, NULL, 5; window 150, 220, 70, NULL, NULL, NULL

#### Scenario: A fill value

- **WHEN** the query selects `coalesce(sum(orders.amount), 0)` and `cumsum(coalesce(sum(orders.amount), 0))`
- **THEN** the empty months read 0 and the running total is 150, 220, 220, 220, 220, 220

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
