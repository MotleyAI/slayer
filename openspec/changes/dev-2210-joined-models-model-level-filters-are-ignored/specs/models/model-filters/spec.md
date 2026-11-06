## Purpose

Defines what a model's `filters` mean: the filtered dataset every read of the model sees, how a filter's joined reads restrict it, and when a filter is refused.

## ADDED Requirements

### Requirement: Model filters define the model's dataset
A model's dataset SHALL be its rows passing every entry of its `filters`. Every read of the model SHALL see
only that dataset, restricted before any join: as a query's or stage's source, as the target of a join hop
of any join type, as a hop of a filter pushed across a fanning or unproven hop (in every pushdown shape),
as the root of a cross-model or isolated aggregate, a windowed or time-shifted evaluation, as the parent of
a list child, as either side of a product with an unrelated dataset, and as a dataset read by another
model's filter or derived column. A read reaching no passing row SHALL behave exactly as a read reaching no
row: a LEFT-joined target null-extends, an INNER-joined target drops the row, a pushed-down filter finds no
match. Model filters SHALL apply whether the model is table-backed, SQL-backed or query-backed. A model
without filters SHALL read its rows unchanged.

The examples below use `orders(id PK, status, amount)` = (1,'done',100), (2,'done',200), (3,'done',300),
(4,'open',400) and `returns(id PK, order_id, reason)` = (10,1,'damaged'), (11,1,'late'), (12,2,'damaged'),
(13,4,'damaged'); models `orders`; `done_orders` on table `orders` with filter `status = 'done'`;
`returns` joining `order_id → done_orders.id`; `late_returns` on table `returns` with filter
`reason = 'late'` joining `order_id → orders.id`.

#### Scenario: A pushed-down filter reads the target's filtered rows
- **WHEN** a query rooted at `orders` selects dimension `id` with filter `late_returns.id is not null`
- **THEN** the rows are order `1` only

#### Scenario: A pushed-down anti-join reads the target's filtered rows
- **WHEN** a query rooted at `orders` selects dimension `id` with filter `late_returns.id is null`
- **THEN** the rows are orders `2`, `3` and `4`

#### Scenario: A to-one dimension null-extends a filtered-out target row
- **WHEN** a query rooted at `returns` selects dimensions `id` and `done_orders.status`
- **THEN** returns `10`, `11`, `12` read `'done'` and return `13` reads NULL

#### Scenario: A null test on a to-one target keeps rows whose target is filtered out
- **WHEN** a query rooted at `returns` selects dimension `id` with filter `done_orders.id is null`
- **THEN** the rows are return `13` only, and the join stays outer for the other rows

#### Scenario: A null-extended pushdown reads the target's filtered rows
- **WHEN** a query rooted at `orders` selects dimension `id` with a filter across `late_returns` that a
  NULL-extended row satisfies, such as `late_returns.id is null or amount > 350`
- **THEN** the rows are orders `2`, `3` and `4`

#### Scenario: A cross-model aggregate counts the target's filtered rows
- **WHEN** a query rooted at `orders` selects dimension `id` and measure `count(late_returns.*)`
- **THEN** the counts are `1`, `0`, `0`, `0` for orders `1` to `4`

#### Scenario: An inner-typed join to a filtered target drops the row
- **WHEN** `returns` declares its join to `done_orders` with join type `inner` and a query rooted at
  `returns` selects dimensions `id` and `done_orders.status`
- **THEN** the rows are returns `10`, `11` and `12`

#### Scenario: SQL-backed and query-backed targets are restricted
- **WHEN** the join target `done_orders` is defined by `sql` over `orders`, or as a query-backed model over
  `orders`, in either case with filter `status = 'done'`, and a query rooted at `returns` selects dimensions
  `id` and `done_orders.status`
- **THEN** return `13` reads NULL in both cases

#### Scenario: A list child reads its filtered parent's rows
- **WHEN** a model with a list column has a filter excluding some of its rows, and a query reads the list's
  elements
- **THEN** only elements of rows passing the parent's filter appear

#### Scenario: A filter's joined read sees the joined model's filtered rows
- **WHEN** a model `returns_of_done` on table `returns` joins `order_id → done_orders.id` and has filter
  `done_orders.id is not null`, and a query rooted at it selects dimension `id`
- **THEN** the rows are returns `10`, `11` and `12`

#### Scenario: A filtered root is unchanged
- **WHEN** a query rooted at `done_orders` selects dimension `id`, or measure `sum(amount)`, or a filter
  of the root reads a to-one joined model
- **THEN** the results are those of the root's passing rows, as before this change

#### Scenario: Isolated, windowed and shifted evaluations read the filtered root
- **WHEN** a query rooted at `done_orders` asks only for an isolated aggregate, a windowed aggregate, a
  time-shifted aggregate, an aggregate under a `date_range`, or uses spine time dimensions
- **THEN** every value is computed over orders `1`, `2` and `3` only, and no cell exists for a row the
  filter excludes

#### Scenario: Row-level security still scopes tables inside a filtered dataset
- **WHEN** a session policy with a column ruleset or a join ruleset is active, and a query reads a filtered
  model whose filter also reads a joined model
- **THEN** both the model's table and the table its filter reads are scoped by the policy, and the query
  reports both models as read

#### Scenario: Every supported dialect renders the filtered dataset
- **WHEN** the queries above are rendered for each supported SQL dialect
- **THEN** each renders valid SQL for that dialect

### Requirement: A model filter's joined reads restrict by association
A model filter reading a joined model SHALL restrict the model's rows by association: a row passes when
the filter holds for some combination of its associated rows, a missing associated row reading as NULL.
A filtered dataset SHALL never contain a row more than once because of a hop its filter crosses, whatever
the hop's arity. Inlining the joined reads as joins SHALL be a pure optimization used only when every hop
they cross is proven to-one, and SHALL return the same rows as the association form.

#### Scenario: A filter across an unproven hop never multiplies rows
- **WHEN** `orders` joins `shipments` on `id → order_ref` where `shipments.order_ref` is not declared
  unique, order `1` has two shipments with carrier `'ups'`, and a model `ups_orders` on table `orders`
  has filter `shipments.carrier = 'ups'`
- **THEN** a query rooted at `ups_orders` with measure `count(*)` counts order `1` once, and a query
  rooted at `returns` joining `ups_orders` returns one row per return

#### Scenario: A filter mixing own and joined columns reads a missing joined row as NULL
- **WHEN** a filter is `shipments.carrier = 'ups' or amount > 350` and order `4` has no shipment
- **THEN** order `4` passes

#### Scenario: Inlined and association forms agree on proven to-one hops
- **WHEN** a model's filter reads only through proven to-one hops and the query is rendered both with
  joins inlined and in the association form
- **THEN** both return the same rows

### Requirement: Filters whose dataset reaches itself are refused
A model's dataset SHALL depend on the dataset of every model its filters read — every model on every join
path in the filters' dependency closure, through the derived columns they name. A model whose dataset
depends on itself SHALL be refused with a filter-cycle error naming the cycle of models in order. Saving a
model that closes such a cycle through itself SHALL fail at both save doors (the engine and the storage
backend), leaving the store unchanged. A query reading a model on such a cycle, stored without validation,
SHALL fail with the same error. A join edge alone, or a filter reading a model whose own filters do not
lead back, SHALL NOT form a cycle.

#### Scenario: Two filters reading each other are refused at save time
- **WHEN** `a` joins `b` and `b` joins `a`, `a` has filter `b.flag = 1`, and `b` is saved with filter
  `a.flag = 1`
- **THEN** the save fails with the filter-cycle error naming `b → a → b`, through the engine and through
  the storage backend alike

#### Scenario: A cycle through a derived column is refused
- **WHEN** `b`'s filter reads a derived column of `b` whose `sql` reads `a.flag`, and `a`'s filter reads `b`
- **THEN** the save closing the cycle fails with the filter-cycle error

#### Scenario: A stored cycle fails the query
- **WHEN** models forming a filter cycle were stored without validation and a query reads one of them
- **THEN** the query fails with the filter-cycle error

#### Scenario: Reverse edges without filter recursion are accepted
- **WHEN** `a` and `b` join each other, `a` has filter `b.flag = 1`, and `b` has no filter or a filter
  reading only its own columns
- **THEN** both saves succeed and queries reading either model run

### Requirement: Model filters are validated wherever read and at save time
A model filter referencing a saved measure, or a column whose SQL contains a window function, SHALL be
refused with the model-filter error — judged for each reference against the model that owns it, including
qualified references to joined models and references reached through derived columns. Saving such a model
SHALL fail at both save doors. A query SHALL fail with the same error when it reads, by the query's read
set, a model whose stored filter is invalid — whether as its source or through any join, filter or
definition.

#### Scenario: Saving a filter on a measure is refused
- **WHEN** a model is saved with a filter `total > 100` where `total` is one of its saved measures
- **THEN** the save fails with the model-filter error naming the filter

#### Scenario: Saving a filter on a joined windowed column is refused
- **WHEN** a model is saved with a filter reading a joined model's column whose SQL contains a window
  function
- **THEN** the save fails with the model-filter error

#### Scenario: A join target's invalid stored filter fails the query
- **WHEN** a join target's filter references one of its measures, stored without validation, and a query
  rooted elsewhere reads that target
- **THEN** the query fails with the model-filter error naming the target's filter
