# queries/semantics delta

## MODIFIED Requirements

### Requirement: Attribution by determination
A query dimension SHALL be attributable to an aggregate iff a chain of provably
many-to-one join hops (per `models/join-cardinality` evidence) leads from the
aggregate's root model to the dimension. Attributable dimensions retain exact per-cell
values — the cells partition the root rows and sum to the total. Every other dimension
resolves per the query's `to_many_handling` mode: under `"broadcast"` (the default) the
aggregate broadcasts across it (its value repeats across that dimension's cells);
under `"associate"` each cell aggregates over the distinct root entities associated
with it (per `queries/attribution-modes`); under `"error"` the query refuses. In no
mode does an aggregate join through an unproven path.

#### Scenario: Attributable dimension partitions, unattributable broadcasts
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by one
  customer-determined dimension and one orders-level dimension
- **THEN** by executed values, cells vary along the determined dimension, sum to the
  total across it, and repeat unchanged across the orders-level dimension

#### Scenario: Associate mode keeps attributable dimensions exact
- **WHEN** the same query runs with `to_many_handling: "associate"`
- **THEN** by executed values, cells along the customer-determined dimension are
  unchanged from the broadcast run, while cells along the orders-level dimension carry
  per-cell distinct-entity values

### Requirement: No double counting
Every aggregation — however consumed: directly selected, a composite leaf, filter-only,
order-only, inside a computed dimension, or nested in a producer, and local and
cross-model alike — SHALL run over the rows of its own root model, each root row
counted exactly once per result cell — never over the row product of a join; no join
fan-out may multiply an aggregation's inputs. The sole exception is the host-grain wrap
(an aggregate deliberately evaluated over the join result at host grain), as specified
in `queries/cross-model-aggregates`.

#### Scenario: Join fan-out never multiplies aggregation inputs
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` and each customer
  has several orders
- **THEN** by executed values every customer's spend is counted exactly once, however
  many orders fan the join

#### Scenario: Local aggregate over a fanning dimension never multiplies
- **WHEN** a query rooted at `customers` selects the local measure `spend:sum` by
  `orders.status`, and one customer has two orders with the same status
- **THEN** by executed values that customer's spend is never counted twice in a cell —
  under the default the metric broadcasts with a warning; under
  `to_many_handling: "associate"` the cell carries the distinct-entity value

#### Scenario: Fanned local aggregates are caught in every consumer context
- **WHEN** a local aggregate over a fanning dimension is consumed inside an arithmetic
  composite, an ORDER BY entry, an aggregate-phase filter, or a computed dimension
- **THEN** each context resolves it per the mode exactly as a directly selected
  measure — never through the silently multiplied join product

### Requirement: Filters restrict by association or fail loudly
A row-level filter conjunct that reaches an aggregate's root only across
non-determining paths SHALL either restrict the aggregate's population by association
— the aggregate computes over exactly the root rows related to at least one surviving
row combination, each counted once — or be loudly excluded (dropped-filter warning,
an error under `to_many_handling: "error"`) per the pushdown-scope rules in
`queries/cross-model-aggregates`. A stated restriction SHALL never be silently
ignored and SHALL never fan out an aggregation's inputs.

#### Scenario: Cross-path filter restricts the population by association
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `customers.spend:sum`
- **THEN** by executed values the metric counts exactly the customers with at least
  one order passing the predicate, each once

#### Scenario: A restriction is never silently ignored
- **WHEN** a filter conjunct cannot be applied to an aggregate's population
- **THEN** the response carries the dropped-filter warning (or the query errors under
  `to_many_handling: "error"`) — never an unrestricted value presented as restricted

### Requirement: Loud degradation
Whenever partition semantics is unavailable — an implicit attribution-loss broadcast,
or a filter excluded from a producer — the response SHALL carry a machine-readable
warning naming the affected aggregate and the cause, and `to_many_handling: "error"`
SHALL turn it into an error (per the metadata and mode requirements in
`queries/cross-model-aggregates` and `queries/attribution-modes`). Every broadcast
warning SHALL carry the dice–slice hint: filtering on the dimension restricts the
metric by association while slicing broadcasts, and `to_many_handling: "associate"`
reconciles the two. Associate-mode resolution SHALL warn that the cells' entity
populations may overlap across the named dimensions and are not additive. Explicit
`partition_by=` is requested grain and SHALL NOT warn — broadcasting and associating
alike.

#### Scenario: Implicit broadcast warns, explicit grain does not
- **WHEN** one query broadcasts a metric over an unattributable dimension and another
  declares the same coarser grain via `partition_by=`
- **THEN** the first response carries the broadcast warning and the second carries none

#### Scenario: Broadcast warning carries the dice–slice hint
- **WHEN** any query broadcasts a metric over an unattributable dimension
- **THEN** the broadcast warning entry names the associate mode as the reconciliation,
  whether or not the query contains a filter

#### Scenario: Associate warns about overlapping populations
- **WHEN** an associate-mode query resolves a metric over an unattributable dimension
  implicitly (no explicit `partition_by=`)
- **THEN** the response carries a warning naming the metric and the dimensions whose
  cells are not additive, and the same event surfaces as a Python-level warning

## ADDED Requirements

### Requirement: Dice–slice correspondence under associate
Under `to_many_handling: "associate"`, filtering the population to `d = v` SHALL yield
the same value for an aggregate as slicing the `v` cell of the same query grouped by
`d`, by executed values, for any dimension `d` reachable from the population. The
broadcast default does not satisfy this correspondence; its warning hint (per Loud
degradation) is the required disclosure.

#### Scenario: Filtered value equals the sliced cell
- **WHEN** an associate-mode query filters `status = 'ok'` with `customers.spend:sum`
  rooted at `orders`, and its counterpart groups by `status` instead
- **THEN** the filtered run's value equals the `ok` cell of the grouped run, by
  executed values, across a generated family of query shapes
