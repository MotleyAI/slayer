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
ranked, windowed, and nested computed-dimension producers. An association producer is
rooted at the aggregate's home dataset and applies every reachable conjunct inline on
its own joins: attributable conjuncts inline as in every producer, and a conjunct
reachable only across hops that are not provably many-to-one inlines on the fanning
join, where the per-entity deduplication makes the fan-out harmless. Membership of the
association is identical to the semi-join semantics above — entities with at least one
related row passing the conjunct, conjuncts on one branch satisfied by the same related
row, branches independent — and a conjunct sharing a hop with an association dimension
is satisfied by the same related row that carries the dimension value. Each such
conjunct is reported through the same informational entry as a semi-join-pushed
conjunct. Conjuncts pushed into the same producer that share their first reverse hop
SHALL be satisfied by the same related row (combination); conjuncts on different
branches are satisfied independently.

Every producer rooted at the query population and built by the host regroup path —
partitioned, windowed, first/last, host-grain wrap, broadcast-local — SHALL additionally
inherit the query population's own filter disposition, computed once at the host root:
a population conjunct the host applies inline applies inline, a population conjunct
restricting the population by association restricts the producer by the same semi-join,
and an excluded population conjunct is dropped from the producer with the dropped-filter
warning; a producer nested inside such a producer and rooted at the same population
inherits the same disposition during its own compilation. Such a producer's own grain — its
partition keys and its window time axis — is attributable from the population (an
unattributable partition key or window time axis is a typed error, per *Explicit grain and
window on cross-model aggregates*), so it takes the population's semi-join, never an inline
join across the fanning hop. This
population inheritance is keyed on the producer's kernel, not on whether its root equals
the host: an association producer (even one whose root is the host) keeps the association
routing of the paragraph above and receives no population semi-join, and a target-rooted
producer keeps its own metric-root disposition.

A conjunct SHALL remain excluded from the producer — reported through the established
dropped-filter warning (and erroring under `to_many_handling: "error"`) while still
applying to the result rows — when it is genuinely unreachable (no resolvable join path
from the root), when its cross-path references span multiple distinct join branches
within one conjunct, or when root-local and cross-path references mix under a
disjunction or negation. The reverse path resolves through the same bidirectional
traversal as every other hop: any declared edge, in either orientation, with oriented
provability governing inline-vs-semi-join classification; a home several hops from the
population root reverses every hop of its path. A hop of the correlation path
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
  semantics, by executed values — the association producer by inlining the conjunct on
  its home-rooted joins

#### Scenario: Filter on a sibling fan-out branch restricts the association correctly
- **WHEN** an associate-mode query's filter references a to-many branch different from
  the metric's association path, including a NULL-sensitive predicate
- **THEN** the association's membership equals the semi-join semantics — entities of
  root rows with at least one related row passing the predicate — by executed values

#### Scenario: Host filter and host dimension bind to the same population row
- **WHEN** an associate-mode query rooted at `orders` filters `channel = 'app'` and
  selects `customers.spend:sum` by `status`, or the same query rooted at `customers`
  selects `spend:sum` by `orders.status` with the filter `orders.channel = 'app'`
- **THEN** each status cell aggregates the distinct customers having an app order with
  that status — never a customer whose app order and status order are different rows —
  by executed values on SQLite and DuckDB

#### Scenario: Association-restricted conjunct carries the informational entry
- **WHEN** an association producer applies a reachable-but-unsafe conjunct inline on
  its joins
- **THEN** the response carries the same machine-readable entry as a semi-join-pushed
  conjunct, naming the aggregate and the filter, with no Python-level warning and no
  error in any mode

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

#### Scenario: Windowed producer over a local axis inherits the restriction
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` over
  `time_dimensions: [{"dimension": "customers.signup_at", "granularity": "month"}]` with
  `filters: ["orders.status = 'ok'"]`, and one customer has two `ok` orders
- **THEN** by executed values each bucket sums the trailing-year signups among the distinct
  customers with at least one `ok` order, each once (April 420 on the reference dataset,
  never 520), the window's source relation carries the semi-join and no `orders` join, and
  the response carries the producer's and the population's `semi_join_pushed` entries

#### Scenario: Nested producer rooted at the population inherits the restriction
- **WHEN** a query rooted at `customers` selects `avg(sum(spend, partition_by=tier))` with
  `dimensions: ["tier"]` and `filters: ["orders.status = 'ok'"]`
- **THEN** by executed values the inner per-tier totals count each customer once (the
  average over tiers is 210 on the reference dataset, never 260), and the generated SQL
  carries the semi-join in the host-rooted producer body and no `orders` join

#### Scenario: Population restriction reaches the producer-only spine
- **WHEN** a query rooted at `customers` selects only `orders.amount:sum` with
  `filters: ["orders.status = 'ok'"]`, and a second run uses a predicate no order passes
- **THEN** the first run returns one row with the producer's value by executed values (82 on
  the reference dataset) and the second returns zero rows — never one row carrying a NULL

### Requirement: Explicit grain and window on cross-model aggregates
Cross-model aggregates SHALL accept `partition_by=`, `window=`, and `first`/`last`.
Under `"broadcast"` and `"error"` modes, every explicit partition key MUST be
attributable from the aggregate's root — an unattributable key is a hard error naming
the remedy. Under `"associate"`, an explicit partition key not attributable from the
root is legal: the aggregate attributes at the declared grain by distinct-entity
association (per `queries/attribution-modes`), without warning (requested grain). A
windowed aggregate — cross-model or rooted at the query population — requires the query's
active time dimension attributable from its home dataset, else fails at plan time with a
typed error naming the time dimension and the remedy, in every `to_many_handling` mode and
whether or not the query filters: the time bucket is a grain member of the windowed
aggregate that its home must determine, never a fanning join multiplying the windowed rows.
Windowing by association under `"associate"` is deferred to DEV-1914.

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

#### Scenario: Population-rooted window over a fanning time axis fails closed
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` over
  `time_dimensions: [{"dimension": "orders.ordered_at", "granularity": "month"}]`, with or
  without `filters: ["orders.status = 'ok'"]`, in any `to_many_handling` mode
- **THEN** the query fails at plan time with a typed error naming `orders.ordered_at` and the
  remedy, containing no issue reference — never a value counting a customer once per order
