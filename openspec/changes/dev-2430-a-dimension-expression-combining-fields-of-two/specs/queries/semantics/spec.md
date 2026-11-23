## ADDED Requirements

### Requirement: Row-level expressions are homed or refused
A row-level expression in a value position — a dimension (plain or computed), a time dimension, or a
row-level order key — SHALL be homed on the deepest dataset that determines every operand over
provably to-one hops (Axiom 2.2). Each row-level leaf is an operand at the dataset it is declared on,
its dependency closure only validating that home; an attached aggregate or transform operand stands
for its grain members (Axiom 2.3); a literal constrains nothing, so an expression with one operand
is always homed. When no dataset determines every operand, the query SHALL fail before any SQL is
generated with the typed input-safety error, naming the position, two operands that no dataset
determines together, and the fanning or unproven hop each crosses from their common dataset, with
the remedy of aggregating one operand to a grain the other's dataset determines or of grouping by
each field as its own dimension. The refusal SHALL hold in every `to_many_handling` mode, in every
stage of a query list, under a named or an inferred population, in raw-row mode, and with or without
metrics. A row-level order key SHALL be judged before any host-grain wrap is built from it. A field
filter is not a value position: a conjunct whose operands no dataset determines keeps restricting
by association (*Filters restrict by association or fail loudly*). An operand on a dataset no join
path reaches from the population SHALL keep the product-hop type error of `queries/unrelated-datasets`,
reported ahead of this refusal. An expression whose operands a dataset determines SHALL keep its
established values; a population that does not determine its home resolves it per the
`to_many_handling` mode, as for a column of that home.

Scenarios below run on the seed of `queries/population` › Rows of an inferred query are the row
supply: `orders` and `returns` are sibling many-to-one children of `customers`; `tickets` is
reached by no relationship.

#### Scenario: Sibling operands in a dimension are refused
- **WHEN** a query with `population: "customers"` groups by `{"field": "orders.amount * returns.amount", "alias": "x"}` and measures `count(customers.*)`, with `to_many_handling` omitted, `"associate"`, `"error"` and `"broadcast"`
- **THEN** each run fails with the input-safety error naming the dimension `x`, `orders.amount` and `returns.amount`, and the hops to `orders` and to `returns` from `customers`, and its remedy

#### Scenario: Refused whatever the query's other parts
- **WHEN** the same dimension is queried without `population`, with no metrics, with `distinct_dimension_values: false`, or in the second stage of a two-stage query list
- **THEN** each query fails with the same error

#### Scenario: Sibling operands in an order key are refused before the wrap
- **WHEN** a query with `population: "customers"` groups by `customers.region`, measures `count(customers.*)` and orders by `orders.amount * returns.amount`
- **THEN** it fails with the input-safety error naming the order key and both operands — never a `min` or `max` wrap over the pairs

#### Scenario: A constituent grained on a sibling is refused
- **WHEN** a query with `population: "customers"` groups by `orders.amount * sum(returns.amount, partition_by=[returns.return_id])`, or by `orders.amount * rank(sum(returns.amount, partition_by=[returns.return_id]), direction='desc')`
- **THEN** each query fails with the input-safety error naming `orders.amount` and the constituent's grain member `returns.return_id`

#### Scenario: The remedy is accepted
- **WHEN** the query instead groups by `{"field": "orders.amount * sum(returns.amount, partition_by=[customers.customer_id])", "alias": "x"}` and measures `count(customers.*)`
- **THEN** by executed values the cells are `x` = 2500 → 1, 5000 → 1, NULL → 3, with the `associated` warning

#### Scenario: Parent and child operands are homed on the child
- **WHEN** a query with `population: "customers"` measuring `count(customers.*)` groups by `{"field": "orders.amount * (customers.region = 'EAST')", "alias": "x"}`; or buckets `case when customers.region = 'EAST' then orders.ordered_at end` at `month`; or groups by `customers.region` and orders by `orders.amount * (customers.region = 'EAST')` descending, NULLs last
- **THEN** by executed values the dimension cells are 0 → 1, 50 → 1, 100 → 1, 200 → 1, NULL → 1; the time-dimension cells are 2025-01 → 2, 2025-02 → 1, NULL → 2, each with the `associated` warning; and the regions are ordered EAST, WEST, NORTH by the per-region `max` of the key

#### Scenario: A derived operand is judged by its closure
- **WHEN** `orders` declares `amount_x2` as `amount * 2` and a query with `population: "customers"` groups by `orders.amount_x2 * returns.amount`
- **THEN** it fails with the input-safety error naming `amount_x2` and `returns.amount`

#### Scenario: A derived operand reading across an unproven hop is refused
- **WHEN** `customers` declares a derived field reading `orders.amount` across an `orders → customers` relationship of unknown cardinality, and a query with `population: "customers"` groups by that field alone, or by `customers.region || <that field>`
- **THEN** each query fails with the input-safety error naming the field and the hop to `orders`

#### Scenario: An unproven hop determines nothing
- **WHEN** the `orders → customers` relationship declares no cardinality and `customers` declares no key, and a query with `population: "customers"` groups by `orders.amount * (customers.region = 'EAST')`
- **THEN** it fails with the input-safety error naming `orders.amount`, `customers.region` and the hop to `orders`

#### Scenario: A field filter with sibling operands still restricts by association
- **WHEN** a query with `population: "customers"` groups by `customers.region`, measures `count(customers.*)` and filters `orders.amount * returns.amount > 100`
- **THEN** by executed values the only row is EAST 1, with the semi-join informational entry

#### Scenario: An unrelated operand keeps the product-hop error
- **WHEN** a query with `population: "customers"` groups by `orders.amount * length(tickets.priority)`, buckets an expression reading `tickets` at `month`, orders by an expression reading `tickets`, or groups by an `orders` derived field whose definition reads `tickets`
- **THEN** each query fails with the product-hop type error of `queries/unrelated-datasets` naming `tickets` and `customers`, never the input-safety error

## MODIFIED Requirements

### Requirement: No double counting
Every aggregation — however consumed: directly selected, a composite leaf, filter-only,
order-only, inside a computed dimension, or nested in a producer, and local and
cross-model alike — SHALL run over the rows of its own root model, each root row
counted exactly once per result cell — never over the row product of a join; no join
fan-out may multiply an aggregation's inputs.

#### Scenario: Join fan-out never multiplies aggregation inputs
- **WHEN** a query rooted at `orders` selects `sum(customers.spend)` and each customer
  has several orders
- **THEN** by executed values every customer's spend is counted exactly once, however
  many orders fan the join

#### Scenario: Local aggregate over a fanning dimension never multiplies
- **WHEN** a query rooted at `customers` selects the local measure `sum(spend)` by
  `orders.status`, and one customer has two orders with the same status
- **THEN** by executed values that customer's spend is never counted twice in a cell —
  by default and under `to_many_handling: "associate"` the cell carries the
  distinct-entity value; under `"broadcast"` the metric broadcasts with a warning

#### Scenario: Fanned local aggregates are caught in every consumer context
- **WHEN** a local aggregate over a fanning dimension is consumed inside an arithmetic
  composite, an ORDER BY entry, an aggregate-phase filter, or a computed dimension
- **THEN** each context resolves it per the mode exactly as a directly selected
  measure — never through the silently multiplied join product
