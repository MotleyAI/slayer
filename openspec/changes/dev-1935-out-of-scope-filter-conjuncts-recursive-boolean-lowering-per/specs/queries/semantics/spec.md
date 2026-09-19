## MODIFIED Requirements

### Requirement: Grain guarantee
For a query with at least one measure, and for a measure-less query with distinct
dimension values enabled (the default), the result SHALL have exactly one row per
combination of dimension values present among the row-filtered population's rows.
Raw-row mode (`distinct_dimension_values=false`) is the documented exception and
returns one row per population row. A row-level filter that reaches the population root
only across a non-determining path SHALL NOT multiply the population's rows, whatever the
conjunct's boolean shape: raw-row mode returns each population row passing the restriction
exactly once.

#### Scenario: One row per dimension combination
- **WHEN** an aggregating query groups by dimensions whose value combinations repeat
  across many population rows
- **THEN** the result contains each present combination exactly once

#### Scenario: Raw rows are never multiplied by a population filter's join
- **WHEN** a query rooted at `customers` selects `dimensions: ["tier"]` with
  `distinct_dimension_values: false` and `filters: ["orders.status = 'ok'"]`, and one
  customer has two `ok` orders
- **THEN** by executed values the result has one row per customer with at least one `ok`
  order (five rows on the reference dataset), never one row per matching order


### Requirement: Filters restrict by association or fail loudly
A row-level filter conjunct that reaches a dataset's root only across non-determining
paths SHALL either restrict that dataset's population by association — the dataset
computes over exactly the root rows related to at least one surviving row combination,
each counted once — or be loudly excluded (dropped-filter warning, an error under
`to_many_handling: "error"`) per the pushdown-scope rules in
`queries/cross-model-aggregates`. This applies to the query population itself exactly as
to an aggregate's root: a conjunct reaching the population root only across a
non-determining path restricts the population by association, every aggregate evaluated
over the population rows — inline or in a producer rooted at the population — counts each
population row once, the filtered model is not joined into the base query for that
conjunct, and the restriction is reported through the semi-join informational entry with
no aggregate named — never an error, in any `to_many_handling` mode, whatever the query
selects. A conjunct whose non-determining paths a consumer's grain already materialises (a
projected dimension or time dimension on the same join branch) applies to that consumer on
the same related row instead, so filtering and grouping on one branch bind to one row; when
the grain materialises only some of a conjunct's branches, the conjunct's references on a
materialised branch bind to the grouped row and only the remaining branches are quantified.
A stated restriction SHALL never be silently ignored and SHALL never fan out an
aggregation's inputs. Whether a conjunct crosses a non-determining path is judged on its
dependency closure — a reference to a derived column whose definition crosses such a path
crosses it too; a conjunct whose closure cannot be analysed SHALL fail at plan time with a
typed error naming the filter and the column, in every mode, whether or not the query
aggregates. Restriction by association is defined on the root row's join product over the
branches the conjunct references, built as the inline path would join them (each hop with
its declared join type, LEFT by default, so a hop with no related row contributes NULL
columns): the root row survives iff the conjunct holds on at least one row of that product,
root-local references taking the root row's values, conjuncts sharing a branch judged on
one product row, SQL three-valued logic applying inside and the restriction itself never
unknown. This rule is total over the conjunct's boolean shape — a root-local and a cross-path
reference mixed under `OR`/`NOT`, cross-path references spanning several join branches, and
an atom comparing columns of two branches all restrict by association, never dropped and
never an error in any mode; negation keeps the existential reading (`NOT B` holds when some
related row fails `B`), and a null-test on a related column holds for a root row with no
related row.

#### Scenario: Cross-path filter restricts the population by association
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `customers.spend:sum`
- **THEN** by executed values the metric counts exactly the customers with at least one
  order passing the predicate, each once

#### Scenario: A restriction is never silently ignored
- **WHEN** a filter conjunct cannot be applied to an aggregate's population
- **THEN** the response carries the dropped-filter warning (or the query errors under
  `to_many_handling: "error"`) — never an unrestricted value presented as restricted

#### Scenario: Population filter across a fanning hop restricts the population
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects the
  local `spend:sum`, in any `to_many_handling` mode, and one customer has two `ok` orders
- **THEN** by executed values the metric equals the spend of the distinct customers with at
  least one `ok` order, each once (420 on the reference dataset, never 520); the base query
  restricts `customers` by a correlated semi-join and does not join `orders`; the response
  carries a `semi_join_pushed` entry naming the filter with no aggregate; no Python-level
  warning is emitted

#### Scenario: Derived population filter across a fanning hop restricts the population
- **WHEN** a query rooted at `orders` filters on `customers.regions.bad_pop > 0` (a derived
  column whose definition crosses the one-to-many `regions → region_events` hop) and
  selects the local `amount:sum`, in any `to_many_handling` mode
- **THEN** by executed values the metric equals the amount of the orders whose region
  passes the predicate, each order once (120 on the reference dataset, never 220)

#### Scenario: Population filter over a provably to-one path stays inline
- **WHEN** a query rooted at `orders` filters on `customers.tier = 'gold'` and selects the
  local `amount:sum`
- **THEN** the query executes with the filter applied as a plain row restriction, by
  executed values and generated SQL unchanged from today

#### Scenario: Filter and dimension on one branch bind to the same row
- **WHEN** a query rooted at `customers` selects `dimensions: ["orders.status"]`, the
  local `spend:sum`, and `filters: ["orders.amount = 20"]`, where the only order of
  amount 20 has status `new` and its customer also has `ok` orders
- **THEN** the result has exactly one cell, `new`, by executed values — the filter and
  the dimension are evaluated on the same joined row, never on the customer's other orders

#### Scenario: Two branches restrict the population independently
- **WHEN** a query rooted at `customers` selects `spend:sum` with
  `filters: ["orders.status = 'ok'", "regions.region_events.value >= 50"]`, where
  `customers → orders` and `regions → region_events` are one-to-many
- **THEN** by executed values the metric counts exactly the customers with at least one
  `ok` order whose region has at least one event of value 50 or more, each once (280 on the
  reference dataset), and the response carries one `semi_join_pushed` entry per filter

#### Scenario: Unanalyzable filter dependency fails closed
- **WHEN** a query filters on `customers.regions.unparseable > 0`, a derived column no
  supported dialect can parse, with or without a measure, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming the filter and the column,
  containing no issue reference — never SQL routed as if the column crossed nothing

#### Scenario: Population filter across a fanning hop with an inline aggregate fails closed
- **WHEN** a query rooted at `customers` filters on
  `tier = 'bronze' or orders.status = 'ok'` (a root-local and a cross-path reference under
  `OR`, the cross-path leg reaching only across the fanning `orders` hop) and selects the
  local `spend:sum`, in any `to_many_handling` mode
- **THEN** the query no longer fails closed: by executed values the metric equals the spend of
  the distinct bronze customers and customers with at least one `ok` order, each once (460 on
  the reference dataset, never the join-multiplied 560); the base query does not join
  `orders`; the response carries a `semi_join_pushed` entry naming the filter with no
  aggregate and no dropped-filter warning; no Python-level warning is emitted

#### Scenario: A root row with no related row is judged with NULL related columns
- **WHEN** a query rooted at `customers` filters on `tier = 'gold' or orders.status = 'ok'`
  and selects `spend:sum`, where one gold customer has no orders at all
- **THEN** by executed values that customer is counted — the disjunction holds on the
  customer's null-extended row — so the metric is 475 on the reference dataset, never 420
  (the customer wrongly dropped) and never the join-multiplied 675

#### Scenario: Negation keeps the existential reading
- **WHEN** a query rooted at `customers` filters on
  `not (tier = 'gold' and orders.status = 'ok')` and selects `spend:sum`
- **THEN** by executed values the metric counts the non-gold customers together with the
  gold customers having at least one order that is not `ok`, each once (370 on the reference
  dataset); a gold customer with no orders is not counted, and the value is never the
  join-multiplied 520

#### Scenario: A null-test on a related column reads as absence
- **WHEN** a query rooted at `customers` filters on `orders.id is null` and selects
  `spend:sum`
- **THEN** by executed values the metric counts exactly the customers with no orders (55 on
  the reference dataset), and the response carries the `semi_join_pushed` entry

#### Scenario: Branches under a disjunction restrict independently
- **WHEN** a query rooted at `customers` filters on
  `orders.status = 'new' or regions.region_events.value >= 50` and selects `spend:sum`
- **THEN** by executed values the metric counts the customers with at least one `new` order
  together with those whose region has an event of value 50 or more, each once (320 on the
  reference dataset), with one `semi_join_pushed` entry for the filter

#### Scenario: An atom spanning two branches is judged on their product
- **WHEN** a query rooted at `customers` filters on
  `orders.amount < regions.region_events.value` and selects `spend:sum`
- **THEN** by executed values the metric counts the customers having some order and some
  region event with the order's amount below the event's value, each once (420 on the
  reference dataset), never a dropped-filter warning

#### Scenario: A materialised branch binds to the grouped row inside a multi-branch conjunct
- **WHEN** a query rooted at `customers` selects `dimensions: ["orders.id"]` and no measures
  with `filters: ["orders.status = 'ok' or regions.name = 'South'"]`
- **THEN** by executed values the result has one cell per order that is itself `ok` or whose
  customer's region is `South` (orders 1, 3, 5, 7, 9 and 10 on the reference dataset) — never
  a cell for a customer's other order admitted because a sibling order is `ok`

#### Scenario: Out-of-scope conjunct without an inline aggregate keeps applying
- **WHEN** a query rooted at `customers` selects `sum(spend, partition_by=tier)` by `tier`
  with `filters: ["tier = 'bronze' or orders.status = 'ok'"]`, in any `to_many_handling`
  mode
- **THEN** the conjunct keeps applying, now by association in the producer too: by executed
  values each tier cell equals the spend of that tier's distinct customers passing the
  disjunction, each once (gold 190, silver 230, bronze 40 on the reference dataset), the
  response carries the producer's and the population's
  `semi_join_pushed` entries and no dropped-filter warning, and `to_many_handling: "error"`
  does not error

#### Scenario: Out-of-scope conjunct in raw-row mode fails closed
- **WHEN** a query rooted at `customers` selects `dimensions: ["tier"]` with
  `distinct_dimension_values: false` and `filters: ["tier = 'bronze' or orders.status = 'ok'"]`,
  and one customer has two `ok` orders
- **THEN** the query no longer fails closed: by executed values the result has one row per
  bronze customer or customer with at least one `ok` order (six rows on the reference
  dataset), never one row per matching join row (seven) — the raw-row grain guarantee holds by
  association

#### Scenario: Dimension-only and producer-only queries are unaffected by the guard
- **WHEN** a query rooted at `customers` filters on `orders.status = 'ok'` and selects no
  measures, or selects only aggregates that compute in their own producers (cross-model,
  partitioned, or windowed)
- **THEN** the retired guard blocks nothing: the query executes with values unchanged from
  today, the filter applied to the population and to each producer by association
