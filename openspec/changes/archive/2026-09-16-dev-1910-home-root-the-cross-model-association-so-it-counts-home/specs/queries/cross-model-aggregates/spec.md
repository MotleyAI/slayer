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
the filter — carried on the response only, with no Python-level warning, and never an
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
