## MODIFIED Requirements

### Requirement: Rows of an inferred query are the row supply

A query omitting `population` SHALL list its **row supply**, before measure-typed filters,
`order` and `limit`. An aggregate grouped by the query's dimensions whose home determines or
associates each of them over a join path is **full-grain**; it supplies its cells — the dimension
tuples its filtered home rows reach along the home's own null-extending paths, so a home row with
no related row lands in the NULL tuple. Any other aggregate (an explicit `partition_by=`, an
aggregate homed on an unrelated dataset, whose cell is the one empty tuple, or one that falls back
to broadcast) has cells on a coarser grain; they SHALL extend to the cells of the dimension anchor
agreeing with them there. A metric SHALL supply the union of its full-grain aggregate operands'
cells, through combinations and transforms, and with none, the union of its operands' extended
cells. The rows SHALL be the NULL-safe union of every metric's supply; with no metrics, the
dimension anchor's cells (the rows of the query anchored at that dataset); with no dimensions,
exactly one row. The query's metrics SHALL define the row supply: an aggregate in a filter, an
order entry or a computed dimension SHALL read the rows it defines — the dimension anchor's cells
when there are no metrics — and SHALL add none. A windowed aggregate SHALL supply the same cells
as the same aggregate without `window=` over the same home and dimensions; its value in every row
SHALL depend only on that row's cell, never on the dataset hosting the query or on the sibling
metrics. Each term SHALL attach to the rows on its own grain, a cell it lacks taking its
empty value. Field filters SHALL restrict every supply before its tuples are taken and SHALL NEVER
choose the dimension anchor or a supply. Rows a row-level security policy or a model filter
removes SHALL NOT exist for any of this: a home row reaching only such a row lands in the NULL
tuple. Spine queries are governed by `queries/time-spine` and keep `time_spine × P`.

Scenario seed data (Ossie Layer 3 Appendix B), executed on SQLite and DuckDB: `customers(customer_id
PK, region, segment)` C1 EAST retail, C2 EAST corporate, C3 WEST retail, C4 NORTH retail;
`orders(order_id PK, customer_id, amount, status)` O1 C1 100 completed, O2 C1 200 open, O3 C2 50
completed, O4 C3 300 open; `returns(return_id PK, customer_id, amount)` R1 C1 25, R2 C4 10;
`visits(visit_id PK, customer_id)` V1 C4, V2 C4, V3 C3; `orders`, `returns` and `visits` each
joined many-to-one to `customers`; `tickets(ticket_id PK)` T1, T2, T3 with no join path to any of
them. The **orphan variant** adds order O5 C9 70 open. Saved metrics `customers.customer_count =
count(*)`, `orders.total_revenue = sum(amount)`, `returns.total_returns = sum(amount)`,
`visits.visit_count = count(*)`, `tickets.ticket_count = count(*)`. No query below names
`population`.

The **association seed**, executed on SQLite and DuckDB: `customers(id, name, credit, signup_at)`
Ann 50 2024-03-01, Bob 70 2024-02-01, Cy 60 2024-01-15, Eve NULL 2024-04-01, Fay 25 2024-05-01;
`orders(id, customer_id, status, amount, ordered_at)` 1 Ann open 30 2024-03-01, 2 Bob paid 5
2024-02-01, 3 Cy open 60 2024-04-01, 4 Cy paid 8 2024-01-10, joined many-to-one to `customers`;
`order_items(id, order_id, product_id, qty)` items of orders 1 (P1), 2 (P2) and 4 (P2), joined
many-to-one to `orders` and to `products(id, category)` P1 food, P2 toys, P5 books. Eve and Fay
have no orders; order 3 has no items. A `window='60d'` frame of a month bucket holds the rows whose
time lies in `[bucket_end − 60 days, bucket_end)`.

#### Scenario: A dimension value no metric reaches is not a row

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue`, and another
  measures `returns.total_returns`
- **THEN** the first returns EAST 350 and WEST 300; the second EAST 25 and NORTH 10

#### Scenario: An orphan home row lands in the NULL tuple

- **WHEN** over the orphan variant a query groups by `customers.region` and measures
  `orders.total_revenue`, once with no filter and once with the filter `orders.amount > 60`
- **THEN** the first returns EAST 350, WEST 300 and NULL 70; the second EAST 300, WEST 300 and
  NULL 70, keyed `customers.region` and `orders.total_revenue` in both

#### Scenario: Two metrics union their rows

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` and
  `returns.total_returns`
- **THEN** the rows are EAST (350, 25), WEST (300, NULL) and NORTH (NULL, 10)

#### Scenario: A count reads 0 on a row another metric supplies

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` and
  `visits.visit_count`
- **THEN** the rows are EAST (350, 0), WEST (300, 1) and NORTH (NULL, 2)

#### Scenario: A metrics-only query returns one row

- **WHEN** a query measures `sum(orders.amount) / count_distinct(customers.customer_id)`; another
  measures `orders.total_revenue` and `tickets.ticket_count`; another measures
  `orders.total_revenue` with the filter `1 = 0`
- **THEN** the first returns one row 162.5 (180 over the orphan variant); the second one row
  (650, 3); the third one row NULL

#### Scenario: Dimension-only rows come from the anchor

- **WHEN** a query groups by `customers.region` with no metrics, once with no filter and once
  with the filter `orders.amount > 100`
- **THEN** the first returns EAST, WEST and NORTH; the second EAST and WEST, keyed
  `customers.region`, the customers restricted by association

#### Scenario: Only metrics supply rows

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue`, in turn with
  the measure-typed filter `sum(returns.amount) >= 0 or orders.total_revenue > 0`, with an order
  entry on `sum(returns.amount)`, and with the computed dimension `case when sum(returns.amount,
  partition_by=[customers.region]) > 0 then 'y' else 'n' end`
- **THEN** every result's regions are EAST and WEST: NORTH, which only `returns` reaches, is not a
  row

#### Scenario: A filter aggregate with no metrics reads the anchor's cells

- **WHEN** a query groups by `customers.region` with no metrics and the filter
  `sum(orders.amount) > 0`
- **THEN** the rows are EAST and WEST: the filter reads the dimension anchor's cells EAST, WEST
  and NORTH and removes NORTH, whose sum is NULL

#### Scenario: A filter never brings rows back

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` with the
  filter `orders.amount > 100`, and again with `orders.order_id` as a second dimension
- **THEN** the first returns EAST 200 and WEST 300; the second (O2, EAST) and (O4, WEST); and with
  the filter `1 = 0` the grouped query returns no rows

#### Scenario: A filter restricts every home

- **WHEN** a query groups by `customers.region`, measures `orders.total_revenue` and
  `returns.total_returns`, and filters `orders.amount > 100`
- **THEN** the rows are EAST (200, 25) and WEST (300, NULL), the returns restricted to customers
  with such an order, with a `semi_join_pushed` warning naming `returns.total_returns`

#### Scenario: A cross-dataset filter

- **WHEN** a query groups by `customers.region`, measures `orders.total_revenue` and filters
  `orders.status = 'completed' and customers.region = 'EAST'`
- **THEN** the only row is EAST 150

#### Scenario: An unrelated metric supplies the anchor's cells

- **WHEN** a query groups by `customers.region` and measures `orders.total_revenue` and
  `tickets.ticket_count`
- **THEN** the rows are EAST (350, 3), WEST (300, 3) and NORTH (NULL, 3), with `associated` and
  `unrelated_dataset` warnings naming `tickets.ticket_count`

#### Scenario: An associated metric supplies the tuples its home rows reach

- **WHEN** a query groups by `orders.status` and measures `customers.customer_count`
- **THEN** the rows are completed 2, open 2 and NULL 1 (C4 has no orders), with an `associated`
  warning

#### Scenario: A partitioned metric supplies its own cells extended by the anchor

- **WHEN** a query groups by `customers.region` and `customers.segment` and measures
  `sum(orders.amount, partition_by=[customers.region])`
- **THEN** the rows are (EAST, retail) 350, (EAST, corporate) 350 and (WEST, retail) 300; NORTH,
  which no order reaches, is not a row

#### Scenario: A filtered-out dimension row does not exist

- **WHEN** `customers` carries the model filter `region <> 'WEST'`, and separately a row-level
  security policy hides C3, and a query groups by `customers.region` and measures
  `orders.total_revenue`
- **THEN** in both cases the rows are EAST 350 and NULL 300: O4, whose customer does not exist for
  the query, lands in the NULL tuple

#### Scenario: Ossie's home routing answers a query the population could not route

- **WHEN** `orders → customers → regions`, `regions → managers` and `orders → managers` are
  many-to-one, and a query groups by `regions.name` and measures `orders.total_revenue`
- **THEN** it executes, each order reaching its region through `customers`, never refused as
  ambiguous

#### Scenario: Producer forms supply rows alike

- **WHEN** a query groups by `customers.region` and measures `returns.total_returns` beside, in
  turn, a ranked aggregate `last(orders.amount, orders.order_id)`, a windowed aggregate, a
  time-shifted transform, a second-order aggregate, an aggregate over a list child, and an
  aggregate homed on a keyless dataset, each over the orphan variant
- **THEN** every result's rows are the union of both metrics' supplies, the orphan landing in the
  NULL tuple where its metric is full-grain, and each value equals the value of that metric alone
  in its own rows

#### Scenario: An absent cell and an all-NULL cell are alike

- **WHEN** a query groups by `customers.region`, measures `orders.total_revenue` and
  `sum(returns.amount)`, and filters `sum(returns.amount) is null`, with one customer's only
  return carrying a NULL amount
- **THEN** a region whose returns are all NULL and a region with no returns both survive the
  filter, with the same NULL value

#### Scenario: Key types agree across row sources

- **WHEN** a query whose rows union several homes groups by a nullable decimal, a timestamp and a
  text dimension, one home supplying only NULL tuples
- **THEN** it executes on every dialect with each key column of one type

#### Scenario: A windowed metric supplies the cells of its unwindowed twin

- **WHEN** over the association seed a query groups by `products.category` and the month of
  `orders.ordered_at` and measures `sum(customers.credit, window='60d')`, another measures
  `count(customers.*, window='60d')`, and another `sum(customers.credit)`
- **THEN** the first returns (food, March) 50, (toys, January) 60, (toys, February) 130, (NULL,
  April) 60 and (NULL, NULL) NULL; the second reads 0 in (NULL, NULL), whose frame is empty; and
  the third returns the same five rows with 50, 60, 70, 60 and 25

#### Scenario: A windowed value is the same whatever hosts the query

- **WHEN** over the association seed a query groups by `orders.status` and the month of
  `customers.signup_at` and measures `count(customers.*)` and `count(customers.*,
  window='60d')`, alone and beside `sum(orders.amount)`, and again naming `population:
  customers`
- **THEN** in every case both metrics read 1 in the (NULL, April) and (NULL, May) cells (Eve and
  Fay, who have no orders), and each windowed value equals, cell by cell, its value in the query
  naming `population: customers`

#### Scenario: A windowed spine metric supplies its home's tuples

- **WHEN** over the association seed a query groups by `orders.status` and the month of
  `time_spine.timestamp` over January to June 2024 and measures only `count(customers.*,
  window='60d')`, and again naming `population: customers`
- **THEN** both return open, paid and NULL × January–June; NULL reads 1 in April and May and 0
  elsewhere, open 1 in January to March, paid 1, 2, 1 in January to March, and every other cell 0
