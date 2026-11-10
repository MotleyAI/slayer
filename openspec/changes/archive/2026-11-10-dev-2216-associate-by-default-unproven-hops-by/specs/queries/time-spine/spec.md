## MODIFIED Requirements

### Requirement: A dataset's axis joins the spine to-one

A dataset's axis SHALL be its declared `default_time_dimension`, else its only column of type `time` or `date`; a dataset with neither has no axis. For a query-backed model or a query stage, the axis SHALL be the propagated `default_time_dimension` of its source when that column survives in its schema, else its only temporal column. A dataset with an axis SHALL behave as if it declared a many-to-one join from its axis to `time_spine.timestamp` by equality; a DATE axis value denotes the instant at midnight of that day. The same effective default SHALL serve as the model's default time dimension for the transform time-axis tie-break and the `first` / `last` fallback ranking column. Saving a model whose declared `default_time_dimension` names a column whose type is not `time` or `date` SHALL be rejected with a typed error naming the model and the column.

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
- **THEN** it resolves through `customers` per `to_many_handling` (associated by default, broadcast with a warning under `broadcast`), never through `time_spine`
