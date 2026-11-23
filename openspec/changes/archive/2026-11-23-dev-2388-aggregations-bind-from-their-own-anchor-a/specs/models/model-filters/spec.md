## MODIFIED Requirements

### Requirement: Model filters define the model's dataset
A model's dataset SHALL be its rows passing every entry of its `filters`. Every read of the model SHALL see
only that dataset, restricted before any join: as a query's or stage's source, as the target of a join hop
of any join type, as a hop of a filter pushed across a fanning or unproven hop (in every pushdown shape),
as the home of any aggregate — cross-model, isolated, windowed or time-shifted —, as the parent of
a list child, as either side of a product with an unrelated dataset, and as a dataset read by another
model's filter or derived column. A read reaching no passing row SHALL behave exactly as a read reaching no
row: a LEFT-joined target null-extends, an INNER-joined target drops the row, a pushed-down filter finds no
match. Model filters SHALL apply whether the model is table-backed, SQL-backed or query-backed. A model
without filters SHALL read its rows unchanged. A model's filters SHALL restrict only reads of that model: as
a query's population they decide which rows exist, and an aggregate whose home reads the model on none of its
own paths SHALL count every row of its home (`queries/population` › Population may be omitted).

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
- **WHEN** a query rooted at `done_orders` asks only for its own windowed aggregate, time-shifted
  aggregate or aggregate under a `date_range`, or uses spine time dimensions; and separately asks only
  for the isolated aggregate `count(returns.*)`, once with no dimension and once grouped by
  `done_orders.id`
- **THEN** every value of the root's own aggregates is computed over orders `1`, `2` and `3` only, and no
  cell exists for a row the filter excludes; `count(returns.*)` counts all `4` returns, its home reading no
  `done_orders` row; grouped by `done_orders.id` the counts are `2`, `1`, `0` for orders `1` to `3`, the
  home reading `done_orders` on its own path and return `13` landing in the NULL tuple, which is no row

#### Scenario: Row-level security still scopes tables inside a filtered dataset
- **WHEN** a session policy with a column ruleset or a join ruleset is active, and a query reads a filtered
  model whose filter also reads a joined model
- **THEN** both the model's table and the table its filter reads are scoped by the policy, and the query
  reports both models as read

#### Scenario: Every supported dialect renders the filtered dataset
- **WHEN** the queries above are rendered for each supported SQL dialect that supports the features they use
- **THEN** each renders valid SQL for that dialect
