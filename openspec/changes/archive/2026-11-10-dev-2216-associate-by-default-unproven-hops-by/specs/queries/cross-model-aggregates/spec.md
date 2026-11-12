## MODIFIED Requirements

### Requirement: Fan-out-safe grain with broadcast
A metric's implicit grain is the query dimension set. Under
`to_many_handling: "broadcast"`, each aggregate SHALL be computed at the subset of that
grain attributable to its root — the dimensions whose path from the root crosses only
provably many-to-one join hops — and its value SHALL be broadcast across the remaining
dimensions. When the mode is omitted or `"associate"`, the remaining dimensions resolve
by distinct-entity association per `queries/attribution-modes`; under `"error"` they
refuse. Dimensions at the aggregate's own root or reached from it over provably-safe
hops MUST retain exact per-dimension values in every mode; only unattributable
dimensions are mode-dependent.

#### Scenario: Attributable dimension keeps exact values
- **WHEN** a query rooted at `orders` selects `sum(customers.spend)` by a dimension on
  `customers` (or reached from `customers` over a provably many-to-one join)
- **THEN** each dimension value's cell carries the exact aggregate for that slice, by
  executed values

#### Scenario: Unattributable dimension broadcasts
- **WHEN** a query rooted at `orders` with `to_many_handling: "broadcast"` selects
  `sum(customers.spend)` by an
  `orders`-level dimension (unreachable from `customers` over provably many-to-one
  hops)
- **THEN** every cell of the same attributable slice carries the same broadcast value —
  the aggregate computed without that dimension — and the result grain is unchanged

#### Scenario: Unproven join arity broadcasts rather than fanning out
- **WHEN** a dimension is reachable from the aggregate's root only across a join whose
  arity is neither declared many-to-one/one-to-one nor structurally proven
- **THEN** under `to_many_handling: "broadcast"` the aggregate broadcasts across that
  dimension instead of joining through it; omitting the mode it associates over its
  distinct entities with the associated-cells warning; in no mode does it silently
  double-count

#### Scenario: Associate mode replaces the broadcast with per-cell association
- **WHEN** the same unattributable-dimension query runs omitting the mode, and again
  with `to_many_handling: "associate"`
- **THEN** in both runs each cell carries the distinct-entity value per
  `queries/attribution-modes`, with the result grain unchanged

### Requirement: Broadcast metadata
Whenever an aggregate's implicit grain loses a dimension to broadcasting, the response
SHALL carry a machine-readable warning naming the affected metric, each broadcast
dimension, and a per-dimension reason that reflects the actual path classification: a
fanning or unproven join hop when a path exists, or unreachable when no join path
resolves — a reachable-but-fanning dimension MUST NOT be reported as unreachable, and a
dimension on a prefix of the aggregate's own path is reachable back over that prefix,
never a round trip through the query root. A
broadcast warning SHALL carry the dice–slice hint, or, for a fallback of an
association-ineligible aggregate under the default, the unsupported-association reason
and remedy (per `queries/semantics` › Loud degradation). One warning SHALL be emitted
per distinct aggregate and fallback reason: an aggregate is identified by its public
measure name when directly selected, else by its canonical aggregate form and role
(expression, order, or filter). Hidden and filter-only aggregate uses emit
warnings too. Explicit `partition_by=` broadcasting is by design and MUST NOT warn.

#### Scenario: Broadcast is reported per metric and dimension
- **WHEN** a query with `to_many_handling: "broadcast"` broadcasts `sum(customers.spend)`
  over `orders.status`
- **THEN** the response warnings include an entry naming that measure, the `status`
  dimension, the reason, and the dice–slice hint, and a matching Python-level warning
  is emitted

#### Scenario: The same aggregate in several roles warns once
- **WHEN** one broadcast aggregate appears as a measure, in a filter, and in ORDER BY
- **THEN** exactly one broadcast warning is emitted for it

#### Scenario: Fanning dimension names the hop, not unreachability
- **WHEN** a broadcast dimension is reachable from the aggregate's root only across a
  fanning or unproven hop
- **THEN** the warning's reason names that hop classification — never "unreachable"

#### Scenario: A prefix-side dimension names the fanning hop
- **WHEN** a query with `to_many_handling: "broadcast"` rooted at `orders` selects
  `sum(customers.regions.countries.gdp)` by `customers.tier`, `regions → countries`
  provably to-one
- **THEN** the warning's reason names the fanning reverse hop to `regions` on the way
  back from `countries` to `customers` — never "unreachable", which the round trip
  through `orders` used to report

### Requirement: Explicit grain and window on cross-model aggregates
Cross-model aggregates SHALL accept `partition_by=`, `window=`, and `first`/`last`.
Under `"broadcast"` and `"error"` modes, every explicit partition key MUST be
attributable from the aggregate's root — an unattributable key is a hard error naming
the remedy. When the mode is omitted or `"associate"`, an explicit partition key not
attributable from the root is legal: the aggregate attributes at the declared grain by distinct-entity
association (per `queries/attribution-modes`), without warning (requested grain). A
windowed aggregate — cross-model or rooted at the query population — requires the query's
active time dimension attributable from its home dataset, else fails at plan time with a
typed error naming the time dimension and the remedy, in every `to_many_handling` mode and
whether or not the query filters: the time bucket is a grain member of the windowed
aggregate that its home must determine, never a fanning join multiplying the windowed rows.
Windowing by association is deferred to DEV-1914: a windowed or `first`/`last` aggregate
that needs association broadcasts with its reason when the mode is omitted and fails
under `"associate"` (per `queries/attribution-modes`).

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
  `time_dimensions: [{"dimension": "orders.ordered_at", "granularity": "month"}]`, with or
  without `filters: ["orders.status = 'ok'"]`, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming `orders.ordered_at` and the
  remedy, containing no issue reference — never a value counting a customer once per order

### Requirement: Cross-model aggregates compose in expressions and dimensions
Cross-model aggregates SHALL be legal wherever local aggregates are: in arithmetic and scalar-call composites (including mixed with local aggregates and with aggregates from different joined models in one expression), inside transforms, in dimension expressions, in filters, and in ORDER BY. Composite legality is uniform across the composite's own shape: a cross-model operand SHALL compile whether the composite combines it with local aggregates, with literals, with several cross-model operands, or wraps it in scalar calls — the compiled route never depends on which seam the composite would otherwise render through, and no composite shape reaches an internal not-supported seam error. A computed dimension whose expression columns are all attributable from a metric's root participates in that metric's grain; otherwise it is unattributable to the metric and resolves per `to_many_handling`. Consumption-position rules match local aggregates exactly: a combined-position consumer of a cross-model partitioned aggregate needs query-dimension partition keys (per the partitioned-aggregates combined-consumer requirement), while row-scope references to a computed dimension's own aggregate stay legal at any partition grain.

#### Scenario: Local and cross-model aggregates in one expression
- WHEN a query selects the measure `sum(orders.revenue) / sum(customers.spend)`
- THEN each cell's value is the ratio of the two correctly-computed aggregates, by executed values

#### Scenario: Scalar call wrapping a cross-model operand executes
- **WHEN** a query selects a scalar-call composite over a cross-model aggregate mixed with
  a local aggregate and a literal (for example `round(sum(customers.spend) / sum(amount), 2)`)
- **THEN** the composite executes with correct hand-computed values on SQLite and DuckDB —
  never the former AGGREGATE-phase-composite not-yet-supported error

#### Scenario: Multiple cross-model operands in one composite execute
- **WHEN** a composite combines two cross-model aggregates (same or different joined
  models) with no local operand
- **THEN** each operand is computed in its own producer and the composed value is correct
  by executed values

#### Scenario: Cross-model aggregate source inside a computed dimension
- WHEN a query declares a dimension banding `sum(customers.spend, partition_by=<customer-level dimension>)`
- THEN rows group by the band with correct executed values and unchanged cardinality

#### Scenario: Computed dimension coexists with a cross-model measure
- WHEN a query combines an aggregation-derived dimension (banded, bare, or transform-root) with a cross-model measure
- THEN both are correct by executed values in one result, replacing the former fail-closed guard

#### Scenario: Filter on a cross-model partitioned aggregate executes
- WHEN a query filters on `sum(customers.spend, partition_by=<customer-level dimension>)` with that partition key among the query dimensions, whether or not the aggregate is also selected
- THEN qualifying rows survive with values identical to the unfiltered query's, by executed values — never the former not-yet-supported error

#### Scenario: Keyless-grain dual-role partitioned aggregate is rejected
- WHEN the same cross-model partitioned aggregate is consumed by a computed dimension and selected as a measure (or named as a raw ORDER BY target) while its partition key is not among the query dimensions
- THEN the query fails at plan time with the clear partition-key error the local variant raises — naming the key and the remedy — never with an internal join-back failure

#### Scenario: Keyless filter over the dimension's own cross-model aggregate executes
- WHEN a query with a computed dimension banding a keyless cross-model partitioned aggregate filters on that same aggregate
- THEN the query executes with the filter row-routed exactly like the local shape — the predicate applies per base row against the attached partition-grain value before aggregation — and the emitted SQL contains one producer relation for the aggregate, not a combined twin

#### Scenario: Keyless ORDER BY the computed dimension's name executes
- WHEN a query with a computed dimension banding a keyless cross-model partitioned aggregate orders by that dimension's name
- THEN rows sort by the banded value, by executed values, with no combined attach synthesized for the order reference

### Requirement: Intermediate-hop dimensions are supported
A dimension lying on an intermediate hop of a cross-model aggregate's join chain SHALL be legal. It follows the attribution rule like any other dimension: exact when attributable from the aggregate's root, otherwise resolved per `to_many_handling` (associated by default, broadcast with metadata under `"broadcast"`) — never an internal not-implemented error.

#### Scenario: Intermediate-hop dimension broadcasts under attribution
- WHEN a query rooted at `orders` selects `sum(customers.regions.pop)` by a `customers`-level dimension
- THEN the query executes (no not-implemented error); since a region's population is not attributable per customer, omitting the mode each customer-level cell aggregates the distinct regions its customers reach, with the associated-cells warning, and under `to_many_handling: "broadcast"` the metric broadcasts across the customer-level dimension with metadata
