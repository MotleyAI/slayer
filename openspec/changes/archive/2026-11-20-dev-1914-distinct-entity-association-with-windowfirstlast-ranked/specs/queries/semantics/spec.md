## MODIFIED Requirements

### Requirement: Attribution by determination
A query dimension SHALL be attributable to an aggregate iff a chain of provably
many-to-one join hops (per `models/join-cardinality` evidence) leads from the
aggregate's root model to the dimension. Attributable dimensions retain exact per-cell
values — the cells partition the root rows and sum to the total. Every other dimension —
reached across a fanning or an unproven hop alike — resolves per the query's
`to_many_handling` mode: when the mode is omitted or `"associate"`, each cell aggregates
over the distinct root entities associated with it (per `queries/attribution-modes`),
whatever its aggregation — `first`/`last` and windowed included; under
`"broadcast"` the aggregate broadcasts across it (its value repeats across that
dimension's cells); under `"error"` the query refuses. In no mode is an aggregation's
input multiplied by a join across a fanning or unproven path.

#### Scenario: Attributable dimension partitions, unattributable broadcasts
- **WHEN** a query rooted at `orders` with `to_many_handling: "broadcast"` selects
  `sum(customers.spend)` by one customer-determined dimension and one orders-level
  dimension
- **THEN** by executed values, cells vary along the determined dimension, sum to the
  total across it, and repeat unchanged across the orders-level dimension

#### Scenario: Associate mode keeps attributable dimensions exact
- **WHEN** the same query runs omitting the mode, and again with
  `to_many_handling: "associate"`
- **THEN** by executed values, in both runs cells along the customer-determined
  dimension are unchanged from the broadcast run, while cells along the orders-level
  dimension carry per-cell distinct-entity values

### Requirement: Loud degradation
Whenever partition semantics is unavailable — an implicit attribution-loss broadcast,
or a filter excluded from a producer — the response SHALL carry a machine-readable
warning naming the affected aggregate and the cause, and `to_many_handling: "error"`
SHALL turn it into an error (per the metadata and mode requirements in
`queries/cross-model-aggregates` and `queries/attribution-modes`). A broadcast under
explicit `"broadcast"` SHALL carry the dice–slice hint: filtering on the dimension
restricts the metric by association while slicing broadcasts, and omitting the mode (or
`"associate"`) reconciles the two. Omitting the mode never broadcasts. Associated resolution SHALL warn that the cells' entity populations may overlap across
the named dimensions and are not additive. Explicit `partition_by=` is requested grain
and SHALL NOT warn — broadcasting and associating alike. Warnings and errors SHALL name
a dimension by its query name — a named dimension's `name`, else its canonical dotted
path — never an internal alias.

#### Scenario: Implicit broadcast warns, explicit grain does not
- **WHEN** one query under `to_many_handling: "broadcast"` broadcasts a metric over an
  unattributable dimension and another declares the same coarser grain via
  `partition_by=`
- **THEN** the first response carries the broadcast warning and the second carries none

#### Scenario: Broadcast warning carries the dice–slice hint
- **WHEN** any query under `to_many_handling: "broadcast"` broadcasts a metric over an
  unattributable dimension
- **THEN** the broadcast warning entry names omitting the mode (or `"associate"`) as the
  reconciliation, whether or not the query contains a filter

#### Scenario: A fallback broadcast carries its reason instead
- **WHEN** a query omitting the mode selects a `first`/`last` aggregate that needs
  association
- **THEN** there is no fallback broadcast: the response carries the associated-cells
  warning and no broadcast warning

#### Scenario: Associate warns about overlapping populations
- **WHEN** a query omitting the mode, or under `"associate"`, resolves a metric over an
  unattributable dimension implicitly (no explicit `partition_by=`)
- **THEN** the response carries a warning naming the metric and the dimensions whose
  cells are not additive, and the same event surfaces as a Python-level warning

#### Scenario: Diagnostics name dimensions by their query name
- **WHEN** a re-aggregation broadcasts over the joined dimension `customers.regions.name`
  (e.g. `avg(sum(amount, partition_by=amount))` rooted at `corders`), and another over a
  named computed dimension `is_p` defined as `product = 'P'`, each under
  `to_many_handling: "broadcast"` and again under `"error"`
- **THEN** the broadcast warnings and the errors name `customers.regions.name` and
  `is_p` — never a `__`-joined path or a generated `grain_…` alias

### Requirement: Dice–slice correspondence under associate
When the mode is omitted or `to_many_handling: "associate"`, filtering the population to
`d = v` SHALL yield the same value for every aggregate — `first`/`last` and windowed ones
included — as slicing the `v` cell of the same query grouped by `d`, by executed values,
for any dimension `d` reachable from the population. Explicit `"broadcast"` does not satisfy this
correspondence; its warning hint (per Loud degradation) is the required disclosure.

#### Scenario: Filtered value equals the sliced cell
- **WHEN** a query omitting the mode, and again under `"associate"`, filters
  `status = 'ok'` with `sum(customers.spend)` rooted at `orders`, and its counterpart
  groups by `status` instead
- **THEN** the filtered run's value equals the `ok` cell of the grouped run, by
  executed values, across a generated family of query shapes

#### Scenario: Filtered first/last and window equal the sliced cell
- **WHEN** the same pair of queries selects `last(customers.spend, customers.signup_at)`,
  and another pair selects `sum(customers.spend, window='1y')` by the month of `ordered_at`
- **THEN** each filtered run equals the `ok` cell of its grouped run, by executed values
