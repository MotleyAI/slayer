# queries/cross-model-aggregates delta

## MODIFIED Requirements

### Requirement: Fan-out-safe grain with broadcast
A metric's implicit grain is the query dimension set. Under
`to_many_handling: "broadcast"` (the default), each aggregate SHALL be computed at the
subset of that grain attributable to its root — the dimensions whose path from the
root crosses only provably many-to-one join hops — and its value SHALL be broadcast
across the remaining dimensions. Under `"associate"`, the remaining dimensions resolve
by distinct-entity association per `queries/attribution-modes`; under `"error"` they
refuse. Dimensions at the aggregate's own root or reached from it over provably-safe
hops MUST retain exact per-dimension values in every mode; only unattributable
dimensions are mode-dependent.

#### Scenario: Attributable dimension keeps exact values
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by a dimension on
  `customers` (or reached from `customers` over a provably many-to-one join)
- **THEN** each dimension value's cell carries the exact aggregate for that slice, by
  executed values

#### Scenario: Unattributable dimension broadcasts
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by an
  `orders`-level dimension (unreachable from `customers` over provably many-to-one
  hops)
- **THEN** every cell of the same attributable slice carries the same broadcast value —
  the aggregate computed without that dimension — and the result grain is unchanged

#### Scenario: Unproven join arity broadcasts rather than fanning out
- **WHEN** a dimension is reachable from the aggregate's root only across a join whose
  arity is neither declared many-to-one/one-to-one nor structurally proven
- **THEN** the aggregate broadcasts across that dimension instead of joining through
  it, and never silently double-counts

#### Scenario: Associate mode replaces the broadcast with per-cell association
- **WHEN** the same unattributable-dimension query runs with
  `to_many_handling: "associate"`
- **THEN** each cell carries the distinct-entity value per
  `queries/attribution-modes`, with the result grain unchanged

### Requirement: Broadcast metadata
Whenever an aggregate's implicit grain loses a dimension to broadcasting, the response
SHALL carry a machine-readable warning naming the affected metric, each broadcast
dimension, and a per-dimension reason that reflects the actual path classification: a
fanning or unproven join hop when a path exists, or unreachable when no join path
resolves — a reachable-but-fanning dimension MUST NOT be reported as unreachable. Every
broadcast warning SHALL carry the dice–slice hint (per `queries/semantics` › Loud
degradation). One warning SHALL be emitted per distinct aggregate: identified by its
public measure name when directly selected, else by its canonical aggregate form and
role (expression, order, or filter). Hidden and filter-only aggregate uses emit
warnings too. Explicit `partition_by=` broadcasting is by design and MUST NOT warn.

#### Scenario: Broadcast is reported per metric and dimension
- **WHEN** a broadcast-mode query broadcasts `customers.spend:sum` over `orders.status`
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

### Requirement: Explicit grain and window on cross-model aggregates
Cross-model aggregates SHALL accept `partition_by=`, `window=`, and `first`/`last`.
Under `"broadcast"` and `"error"` modes, every explicit partition key MUST be
attributable from the aggregate's root — an unattributable key is a hard error naming
the remedy. Under `"associate"`, an explicit partition key not attributable from the
root is legal: the aggregate attributes at the declared grain by distinct-entity
association (per `queries/attribution-modes`), without warning (requested grain). A
windowed cross-model aggregate requires the query's active time dimension attributable
from its root, else errors.

#### Scenario: Cross-model partitioned aggregate computes at the declared grain
- **WHEN** a query selects `customers.spend:sum(partition_by=<customer-level dimension>)`
- **THEN** the value is computed at exactly the declared grain and broadcast to the
  query rows, by executed values

#### Scenario: Unattributable explicit partition key errors
- **WHEN** a cross-model aggregate declares `partition_by=` naming a dimension not
  attributable from its root, under `"broadcast"` or `"error"` mode
- **THEN** the query fails with a clear error naming the key and the remedy

#### Scenario: Unattributable explicit partition key attributes under associate
- **WHEN** the same aggregate runs under `to_many_handling: "associate"`
- **THEN** the value is computed at the declared grain over distinct associated
  entities and attached to the query rows, by executed values, with no warning

#### Scenario: Cross-model first/last and windowed aggregates work
- **WHEN** a query selects a `first`/`last` or `window=` aggregate over a joined
  model's column with an attributable grain
- **THEN** the value is correct by executed values and result cardinality is unchanged

### Requirement: Producer filter routing
A ROW-phase filter conjunct whose references are all attributable from an aggregate's
root SHALL apply inside that aggregate's computation. A conjunct reachable from the
root only across hops that are not provably many-to-one SHALL still restrict the
aggregate's population, by semi-join: the aggregate is computed over exactly the root
rows related to at least one row (combination) passing the conjunct — never over
join-multiplied rows — uniformly with inline inheritance, in every
`to_many_handling` mode. Each semi-join-pushed conjunct SHALL be reported through a
machine-readable informational entry on the response naming the affected aggregate and
the filter — carried on the response only, with no Python-level warning, and never an
error in any mode. On provably many-to-one hops the semi-join is semantically identical
to inline application, and inline remains a pure optimization (no informational entry).
Reference resolution uses each reference's full dependency set: a derived (SQL-defined)
column's classification follows the models its definition actually reads, not just its
declared location.

Semi-join pushdown SHALL apply uniformly to every producer — plain, partitioned,
ranked, windowed, nested computed-dimension, and association producers. For an
association producer the same routing applies with the producer's root taken as the
host: host-attributable conjuncts apply inline, unsafe-but-reachable conjuncts push by
semi-join, and membership of the association is identical to the semi-join semantics
above. Conjuncts pushed into the same producer that share their first reverse hop SHALL
be satisfied by the same related row (combination); conjuncts on different branches are
satisfied independently.

A conjunct SHALL remain excluded from the producer — reported through the established
dropped-filter warning (and erroring under `to_many_handling: "error"`) while still
applying to the result rows — when it is genuinely unreachable (no resolvable join path
from the root), when its cross-path references span multiple distinct join branches
within one conjunct, or when root-local and cross-path references mix under a
disjunction or negation. The reverse path resolves through the same bidirectional
traversal as every other hop: any declared edge, in either orientation, with oriented
provability governing inline-vs-semi-join classification. A hop of the correlation path
connected by two or more edges SHALL fail closed in both modes with the ambiguous-hop
error naming the candidate edges — never dropped, never guessed. AGGREGATE-phase
predicates keep aggregate-filter semantics uniform with local aggregates: they restrict
the result rows by the aggregate's attached value, including when the aggregate appears
only in the filter.

#### Scenario: Attributable filter restricts the metric
- **WHEN** a query rooted at `orders` filters on a customer-level predicate and selects
  `customers.spend:sum`
- **THEN** the metric is computed over only the customers passing the predicate

#### Scenario: Aggregate-phase filter restricts result rows uniformly
- **WHEN** a query rooted at `orders` groups by a customer-level dimension and filters
  on `customers.spend:sum > 100`
- **THEN** only groups passing the predicate remain in the result — exactly as a local
  aggregate filter behaves — whether or not the aggregate is also selected

#### Scenario: Unsafe filter no longer fans out the producer
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `customers.spend:sum`
- **THEN** the metric counts exactly the customers with at least one order passing the
  predicate, each customer's spend once (never double-counted through the reverse hop),
  with unchanged result cardinality

#### Scenario: Pushed filter carries the informational entry
- **WHEN** a query pushes a filter into a producer by semi-join, in any mode
- **THEN** the response carries a machine-readable entry naming the aggregate and the
  filter, no Python-level warning is emitted, and the query does not error under
  `to_many_handling: "error"`

#### Scenario: Pushed filter still restricts the result rows
- **WHEN** a query pushes a filter into a producer by semi-join
- **THEN** the filter also still applies to the result rows exactly as before

#### Scenario: Filters sharing a branch bind to the same related row
- **WHEN** a query rooted at `orders` filters `status = 'paid'` and `channel = 'app'`
  and selects `customers.spend:sum`, and a customer has a paid order and an app order
  but no single paid app order
- **THEN** that customer is excluded from the metric's population — both predicates
  must hold on one related row, by executed values

#### Scenario: Pushdown works without a declared reverse join
- **WHEN** the only stored edge is the forward `orders → customers` join (default join
  type) and a query rooted at `orders` filters on an orders-level predicate with
  `customers.spend:sum` selected
- **THEN** the filter pushes down by semi-join over that edge's reverse orientation,
  with correct executed values

#### Scenario: Ambiguous correlation hop fails closed
- **WHEN** the filtered model reaches the producer root only across a pair of models
  connected by two or more edges and no edge name resolves the hop
- **THEN** the query fails in both modes with the ambiguous-hop error naming the
  candidate edges, rather than dropping the conjunct or guessing a correlation

#### Scenario: Mixed disjunction stays dropped and warned
- **WHEN** a single conjunct mixes a root-local predicate with a cross-path predicate
  under an OR, or its cross-path references span multiple distinct join branches
- **THEN** it is excluded with the established dropped-filter warning (error mode
  errors), never pushed with altered semantics

#### Scenario: Derived-column dependencies drive classification
- **WHEN** a filter references a SQL-defined column whose definition reads a model
  across a hop that is not provably many-to-one from the producer root
- **THEN** the conjunct is classified by those actual dependencies — pushed by
  semi-join (or excluded when outside pushdown scope), never inlined through the
  unsafe hop

#### Scenario: Pushdown reaches every producer kind
- **WHEN** a query with an unsafe-but-reachable filter uses ranked, windowed, nested
  computed-dimension, or association producers
- **THEN** each such producer's population is restricted by the same semi-join
  semantics, by executed values

#### Scenario: Filter on a sibling fan-out branch restricts the association correctly
- **WHEN** an associate-mode query's filter references a to-many branch different from
  the metric's association path, including a NULL-sensitive predicate
- **THEN** the association's membership equals the semi-join semantics — entities of
  root rows with at least one related row passing the predicate — by executed values

#### Scenario: ClickHouse below 25.4 fails closed
- **WHEN** a semi-join pushdown query targets a ClickHouse server older than 25.4 or of
  undeterminable version
- **THEN** the query fails with a clear error naming the version requirement instead of
  executing with different semantics; on 25.4+ the required correlated-subquery setting
  is applied automatically and the query executes

#### Scenario: Genuinely unreachable filter keeps the established behavior
- **WHEN** a filter references a model with no resolvable join path from the producer
  root
- **THEN** it is excluded with the dropped-filter warning and error mode errors,
  exactly as before

## REMOVED Requirements

### Requirement: Strict mode
**Reason**: The `strict` boolean is retired; its behavior is absorbed unchanged by
`to_many_handling: "error"` (see `queries/attribution-modes` › Error mode refuses
silent semantics), which additionally covers local aggregates over fanning dimensions.
**Migration**: Set `to_many_handling: "error"` instead of `strict: true`. Stored
queries migrate automatically on load; query input still containing `strict` fails
with a typed error naming the replacement.
