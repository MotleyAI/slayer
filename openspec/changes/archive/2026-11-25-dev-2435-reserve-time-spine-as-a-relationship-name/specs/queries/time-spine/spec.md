## MODIFIED Requirements

### Requirement: A dataset's axis joins the spine to-one

A dataset's axis SHALL be its declared `default_time_dimension`, else its only column of type `time` or `date`; a dataset with neither has no axis. For a query-backed model or a query stage, the axis SHALL be the propagated `default_time_dimension` of its source when that column survives in its schema, else its only temporal column. A dataset with an axis SHALL behave as if it declared a many-to-one join from its axis to `time_spine.timestamp` by equality; a DATE axis value denotes the instant at midnight of that day. The same effective default SHALL serve as the model's default time dimension for the transform time-axis tie-break and the `first` / `last` fallback ranking column. Saving a model whose declared `default_time_dimension` names a column whose type is not `time` or `date` SHALL be rejected with a typed error naming the model and the column.

The axis join, and a spine population's product with the spine, SHALL hold whatever names the dataset's relationships carry, in both directions: a relationship named like the spine in any case SHALL save, load and query without refusal, rename or warning, and every spine query SHALL attribute and route the dataset exactly as if that relationship had any other name. An explicit hop spelt like the spine from such a dataset SHALL stay ambiguous per the hop rules. No response, generated SQL, warning, error, `inspect` or search output SHALL spell the spine other than `time_spine`, and no suggested path SHALL be offered that does not resolve back to the route it describes.

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
