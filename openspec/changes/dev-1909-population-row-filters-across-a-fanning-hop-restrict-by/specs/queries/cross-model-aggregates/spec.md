## MODIFIED Requirements

### Requirement: Producer filter routing
A ROW-phase filter conjunct whose references are all attributable from an aggregate's
root SHALL apply inside that aggregate's computation. A conjunct reachable from the
root only across hops that are not provably many-to-one SHALL still restrict the
aggregate's population, by semi-join: the aggregate is computed over exactly the root
rows related to at least one row (combination) passing the conjunct — never over
join-multiplied rows — uniformly with inline inheritance, in every
`to_many_handling` mode. Each semi-join-pushed conjunct SHALL be reported through a
machine-readable informational entry on the response naming the affected aggregate and
the filter — or, when the query population itself is restricted, naming the filter with
no aggregate — carried on the response only, with no Python-level warning, and never an
error in any mode. On provably many-to-one hops the semi-join is semantically identical
to inline application, and inline remains a pure optimization (no informational entry).
Reference resolution uses each reference's full dependency set: a derived (SQL-defined)
column's classification follows the models its definition actually reads, not just its
declared location.

Semi-join pushdown SHALL apply uniformly to every producer — plain, partitioned,
ranked, windowed, nested computed-dimension, and association producers. Every producer
rooted at the query population — partitioned, windowed, first/last, association, and a
local aggregate routed through the mode axis — SHALL inherit the population's own filter
disposition, computed once: conjuncts the population applies inline apply inline,
conjuncts restricting the population by association restrict the producer by the same
semi-join, and excluded conjuncts are dropped from the producer with the dropped-filter
warning; a producer nested inside such a producer and rooted at the same relation inherits
the same. A conjunct whose non-determining paths the producer's own grain materialises —
its partition keys, its window time axis, the branch of an association dimension — applies
inline to the same related row. For an association producer the same routing applies with
the producer's root taken as the host: host-attributable conjuncts apply inline,
unsafe-but-reachable conjuncts push by semi-join, and membership of the association is
identical to the semi-join semantics above. Conjuncts pushed into the same producer that
share their first reverse hop SHALL be satisfied by the same related row (combination);
conjuncts on different branches are satisfied independently.

A conjunct SHALL remain excluded from the producer — reported through the established
dropped-filter warning (and erroring under `to_many_handling: "error"`) while still
applying to the result rows — when it is genuinely unreachable (no resolvable join path
from the root), when its cross-path references span multiple distinct join branches
within one conjunct, or when root-local and cross-path references mix under a
disjunction or negation. The reverse path resolves through the same bidirectional
traversal as every other hop: any declared edge, in either orientation, with oriented
provability governing inline-vs-semi-join classification. A hop of the correlation path
connected by two or more parallel edges (candidate edges for that single hop) SHALL fail closed in all three modes with the ambiguous-hop
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
  connected by two or more parallel edges (candidate edges for that single hop) and no edge name resolves the hop
- **THEN** the query fails in all three modes with the ambiguous-hop error naming the
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

#### Scenario: Partitioned local producer restricts by association
- **WHEN** a query rooted at `customers` selects `sum(spend, partition_by=tier)` by `tier`
  with `filters: ["orders.status = 'ok'"]`, and one gold customer has two `ok` orders
- **THEN** by executed values each tier cell equals the spend of that tier's distinct
  customers with at least one `ok` order, each once (gold 190 and silver 230 on the
  reference dataset, never gold 290), and the response carries a `semi_join_pushed` entry
  naming the measure alongside the population's entry naming no aggregate

#### Scenario: Nested producer rooted at the population inherits the restriction
- **WHEN** a query rooted at `customers` selects `avg(sum(spend, partition_by=tier))` with
  `dimensions: ["tier"]` and `filters: ["orders.status = 'ok'"]`
- **THEN** by executed values the inner per-tier totals count each customer once (the
  average over tiers is 210 on the reference dataset, never 260), and the generated SQL
  carries the semi-join in the host-rooted producer body and no `orders` join

#### Scenario: Association producer binds a same-branch filter to the same row
- **WHEN** a query rooted at `customers` selects `dimensions: ["orders.status"]`, the local
  `spend:sum`, and `filters: ["orders.amount in (20, 30)"]` under
  `to_many_handling: "associate"`, where the amount-20 order is `new` and belongs to one
  customer and the amount-30 order is `ok` and belongs to another
- **THEN** by executed values the `new` cell holds only the first customer's spend and the
  `ok` cell only the second's (100 and 150 on the reference dataset, never 250 in both) —
  each entity is associated with the cell of its own qualifying row

#### Scenario: Fanning-axis window keeps its frame rows
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` over
  `time_dimensions: [{"dimension": "orders.ordered_at", "granularity": "month"}]` with
  `filters: ["orders.status = 'ok'"]` and a `date_range` that starts inside the data
- **THEN** by executed values each bucket in the range equals the same bucket of the query
  without the `date_range` — the range bounds the visible buckets and never truncates the
  window's earlier rows

#### Scenario: Population restriction reaches the producer-only spine
- **WHEN** a query rooted at `customers` selects only `orders.amount:sum` with
  `filters: ["orders.status = 'ok'"]`, and a second run uses a predicate no order passes
- **THEN** the first run returns one row with the producer's value by executed values (82 on
  the reference dataset) and the second returns zero rows — never one row carrying a NULL
