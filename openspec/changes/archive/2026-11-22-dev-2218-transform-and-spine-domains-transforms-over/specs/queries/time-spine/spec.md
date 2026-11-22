## MODIFIED Requirements

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
