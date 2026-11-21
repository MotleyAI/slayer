## MODIFIED Requirements

### Requirement: Explicit grain and window on cross-model aggregates
Cross-model aggregates SHALL accept `partition_by=`, `window=`, and `first`/`last`.
The explicit-key rule below SHALL hold alike for cross-model aggregates and for local
aggregates rooted at the query population. Under `"broadcast"` and `"error"` modes,
every explicit partition key MUST be attributable from the aggregate's home — an
unattributable key is a hard error naming the key and both remedies (omitting the mode,
and declaring cardinality or a covering unique key). When the mode is omitted or
`"associate"`, an explicit partition key not attributable from the home is legal: the
aggregate attributes at the declared grain by distinct-entity association (per
`queries/attribution-modes` › Unattributable explicit partition keys resolve per the
mode), without warning (requested grain) — a `first`/`last` or windowed aggregate
included. An unattributable time partition key associates per bucket: a home row
counts once in each bucket one of its associated rows falls in, so a home row whose
associated rows span several buckets counts in each of them. A windowed aggregate — cross-model or
rooted at the query population — whose query's active time dimension is not attributable
from its home dataset associates by its frame when the mode is omitted or `"associate"`
(per `queries/attribution-modes` › Ranked and windowed association), never a fanning join
multiplying the windowed rows; under `"broadcast"` and `"error"` it fails at plan time,
whether or not the query filters, with a typed error naming the time dimension and the
remedies, association among them — broadcasting cannot drop the window's own axis.

#### Scenario: Cross-model partitioned aggregate computes at the declared grain
- **WHEN** a query selects `sum(customers.spend, partition_by=<customer-level dimension>)`
- **THEN** the value is computed at exactly the declared grain and broadcast to the
  query rows, by executed values

#### Scenario: Unattributable explicit partition key errors
- **WHEN** a cross-model aggregate declares `partition_by=` naming a dimension not
  attributable from its root, under `"broadcast"` or `"error"` mode
- **THEN** the query fails with a clear error naming the key and the remedy

#### Scenario: Unattributable explicit partition key attributes under associate
- **WHEN** the same aggregate runs omitting the mode, and again under
  `to_many_handling: "associate"`
- **THEN** in both runs the value is computed at the declared grain over distinct associated
  entities and attached to the query rows, by executed values, with no warning

#### Scenario: Cross-model first/last and windowed aggregates work
- **WHEN** a query selects a `first`/`last` or `window=` aggregate over a joined
  model's column with an attributable grain
- **THEN** the value is correct by executed values and result cardinality is unchanged

#### Scenario: Population-rooted window over a fanning time axis fails closed
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` over
  `time_dimensions: [{"field": "orders.ordered_at", "granularity": "month"}]`, with and
  without `filter: ["orders.status = 'ok'"]`, omitting the mode and again under
  `"associate"`
- **THEN** it no longer fails closed: each month counts every customer with an order in its
  trailing year once — Jan
  100, Feb 250, Mar 350, Apr 460 unfiltered and Jan 100, Feb 250, Mar 310, Apr 420 filtered
  on the reference data — never a value counting a customer once per order

#### Scenario: Population-rooted window over a fanning time axis fails outside association
- **WHEN** the same query runs under `"broadcast"` or `"error"`, with or without the filter
- **THEN** the query fails at plan time with a typed error naming `orders.ordered_at` and the
  remedies, association among them, containing no issue reference


#### Scenario: Local aggregate with a fanning partition key associates
- **WHEN** a query rooted at `customers` by `orders.status` selects
  `sum(spend, partition_by=orders.status)`, omitting the mode and again under `"associate"`
- **THEN** both runs return ok 420, new 290 and NULL 55 (the customer with no orders), with
  no warning, by executed values on SQLite and DuckDB

#### Scenario: Local first/last with a fanning partition key associates
- **WHEN** a query rooted at `customers` by `orders.status` selects
  `last(spend, signup_at, partition_by=orders.status)`, omitting the mode and again under
  `"associate"`
- **THEN** both runs return ok 30, new 40 and NULL 55, with no warning, by executed values

#### Scenario: Local windowed aggregate with a fanning partition key associates
- **WHEN** a query rooted at `customers` by `orders.status` and signup month selects
  `sum(spend, window='1y', partition_by=orders.status)`, omitting the mode and again under
  `"associate"`
- **THEN** both runs return ok Jan 100, Feb 250, Mar 310, Apr 420; new Jan 100, Feb 250,
  Mar 290; and NULL Apr 55, with no warning, by executed values

#### Scenario: A fanning partition key and a fanning window axis read one related row
- **WHEN** a query rooted at `customers` by `orders.status` and order month selects
  `sum(spend, window='1y', partition_by=orders.status)` on data where one customer (spend
  1000) has a `new` order in March 2024 and an `ok` order in January 2023, omitting the mode
- **THEN** that customer counts in (new, 2024-03) and not in (ok, 2024-03), whose value is
  the spend of the customers with an `ok` order in the cell's frame, so the status and the
  time of one order decide membership together, by executed values

#### Scenario: Local fanning time partition key associates per bucket
- **WHEN** a query rooted at `customers` by order month selects
  `sum(spend, partition_by=orders.ordered_at)`, omitting the mode and again under
  `"associate"`
- **THEN** both runs return Jan 100, Feb 150, Mar 100, Apr 110 and NULL 55, with no warning,
  by executed values

#### Scenario: A home row spanning buckets counts in each
- **WHEN** one customer (spend 1000, gold) has orders in January 2023 and March 2024, and a
  query rooted at `customers` by order month selects `sum(spend, partition_by=orders.ordered_at)`,
  and a query rooted at `orders` by order month selects
  `sum(cumsum(sum(customers.spend, partition_by=[customers.tier, orders.ordered_at])))`
- **THEN** the first returns 2023-01 1000 and 2024-03 1100 (60 + 40 + 1000), and the second
  returns 2023-01 1000, 2024-01 1100, 2024-02 150, 2024-03 2200 and 2024-04 2420, by executed
  values on SQLite and DuckDB

#### Scenario: Local fanning partition keys refuse outside association
- **WHEN** any of the local queries above runs under `"broadcast"` or `"error"`
- **THEN** it fails at plan time with a typed partition-key error naming the key and both
  remedies, containing no issue reference and never describing the aggregate as
  cross-dataset
