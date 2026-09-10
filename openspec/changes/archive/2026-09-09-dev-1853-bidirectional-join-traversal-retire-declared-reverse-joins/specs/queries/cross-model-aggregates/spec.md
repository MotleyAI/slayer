# queries/cross-model-aggregates Delta

## MODIFIED Requirements

### Requirement: Strict mode
`SlayerQuery.strict` (default false) SHALL turn every silent-semantics event into a clear error: an implicit-grain broadcast, or a filter actually excluded from a producer (unreachable, or outside semi-join pushdown scope). A filter applied by semi-join pushdown is correctly applied and MUST NOT error. The error names the metric, the dimension or filter, and the remedy (declare join cardinality, a covering unique key, or remove the dimension/filter). An ambiguous correlation path is not a strict-mode concern: it errors in both modes. Explicit `partition_by=` broadcasting does not error.

#### Scenario: Strict query with a broadcast errors
- **WHEN** a query with `strict=true` would broadcast a metric over an unattributable dimension
- **THEN** the query fails with an error naming the metric, the dimension, and the unproven or unreachable hop — not with wrong numbers

#### Scenario: Strict passes when everything is attributable
- **WHEN** a `strict=true` query's metrics are all computable at the full query grain
- **THEN** the query succeeds with values identical to the lenient run

#### Scenario: Strict passes on a pushable filter
- **WHEN** a `strict=true` query's only cross-root filter is unsafe-but-reachable and pushes down by semi-join
- **THEN** the query succeeds, with the metric computed over the filtered population

#### Scenario: Strict still errors on an excluded filter
- **WHEN** a `strict=true` query has a filter that is excluded from a producer (unreachable or outside pushdown scope)
- **THEN** the query fails with an error naming the filter and the remedy

## REMOVED Requirements

### Requirement: Producer filter inheritance
**Reason**: Reverse-path resolution through the shared bidirectional walker retires the semi-join-only inversion carve-out and the drop+warn handling of ambiguous reverse paths this requirement pinned.
**Migration**: Replaced by "Producer filter routing" below; ambiguity now fails closed per `models/join-traversal`.

## ADDED Requirements

### Requirement: Producer filter routing
A ROW-phase filter conjunct whose references are all attributable from an aggregate's root SHALL apply inside that aggregate's computation. A conjunct reachable from the root only across hops that are not provably many-to-one SHALL still restrict the aggregate's population, by semi-join: the aggregate is computed over exactly the root rows related to at least one row (combination) passing the conjunct — never over join-multiplied rows — silently and without metadata, uniformly with inline inheritance. On provably many-to-one hops the semi-join is semantically identical to inline application, and inline remains a pure optimization. Reference resolution uses each reference's full dependency set: a derived (SQL-defined) column's classification follows the models its definition actually reads, not just its declared location.

Semi-join pushdown SHALL apply uniformly to every target-rooted producer — plain, partitioned, ranked, windowed, and nested computed-dimension producers. Conjuncts pushed into the same producer that share their first reverse hop SHALL be satisfied by the same related row (combination); conjuncts on different branches are satisfied independently.

A conjunct SHALL remain excluded from the producer — reported through the established dropped-filter warning (and erroring under strict) while still applying to the result rows — when it is genuinely unreachable (no resolvable join path from the root), when its cross-path references span multiple distinct join branches within one conjunct, or when root-local and cross-path references mix under a disjunction or negation. The reverse path resolves through the same bidirectional traversal as every other hop: any declared edge, in either orientation, with oriented provability governing inline-vs-semi-join classification. A hop of the correlation path connected by two or more edges SHALL fail closed in both modes with the ambiguous-hop error naming the candidate edges — never dropped, never guessed. AGGREGATE-phase predicates keep aggregate-filter semantics uniform with local aggregates: they restrict the result rows by the aggregate's attached value, including when the aggregate appears only in the filter.

#### Scenario: Attributable filter restricts the metric
- **WHEN** a query rooted at `orders` filters on a customer-level predicate and selects `customers.spend:sum`
- **THEN** the metric is computed over only the customers passing the predicate

#### Scenario: Aggregate-phase filter restricts result rows uniformly
- **WHEN** a query rooted at `orders` groups by a customer-level dimension and filters on `customers.spend:sum > 100`
- **THEN** only groups passing the predicate remain in the result — exactly as a local aggregate filter behaves — whether or not the aggregate is also selected

#### Scenario: Unsafe filter no longer fans out the producer
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects `customers.spend:sum`
- **THEN** the metric counts exactly the customers with at least one order passing the predicate, each customer's spend once (never double-counted through the reverse hop), with no warning and unchanged result cardinality

#### Scenario: Pushed filter still restricts the result rows
- **WHEN** a lenient-mode query pushes a filter into a producer by semi-join
- **THEN** the filter also still applies to the result rows exactly as before

#### Scenario: Filters sharing a branch bind to the same related row
- **WHEN** a query rooted at `orders` filters `status = 'paid'` and `channel = 'app'` and selects `customers.spend:sum`, and a customer has a paid order and an app order but no single paid app order
- **THEN** that customer is excluded from the metric's population — both predicates must hold on one related row, by executed values

#### Scenario: Pushdown works without a declared reverse join
- **WHEN** the only stored edge is the forward `orders → customers` join (default join type) and a query rooted at `orders` filters on an orders-level predicate with `customers.spend:sum` selected
- **THEN** the filter pushes down by semi-join over that edge's reverse orientation, with correct executed values

#### Scenario: Ambiguous correlation hop fails closed
- **WHEN** the filtered model reaches the producer root only across a pair of models connected by two or more edges and no edge name resolves the hop
- **THEN** the query fails in both modes with the ambiguous-hop error naming the candidate edges, rather than dropping the conjunct or guessing a correlation

#### Scenario: Mixed disjunction stays dropped and warned
- **WHEN** a single conjunct mixes a root-local predicate with a cross-path predicate under an OR, or its cross-path references span multiple distinct join branches
- **THEN** it is excluded with the established dropped-filter warning (strict errors), never pushed with altered semantics

#### Scenario: Derived-column dependencies drive classification
- **WHEN** a filter references a SQL-defined column whose definition reads a model across a hop that is not provably many-to-one from the producer root
- **THEN** the conjunct is classified by those actual dependencies — pushed by semi-join (or excluded when outside pushdown scope), never inlined through the unsafe hop

#### Scenario: Pushdown reaches every producer kind
- **WHEN** a query with an unsafe-but-reachable filter uses ranked, windowed, or nested computed-dimension producers
- **THEN** each such producer's population is restricted by the same semi-join semantics, by executed values

#### Scenario: ClickHouse below 25.4 fails closed
- **WHEN** a semi-join pushdown query targets a ClickHouse server older than 25.4 or of undeterminable version
- **THEN** the query fails with a clear error naming the version requirement instead of executing with different semantics; on 25.4+ the required correlated-subquery setting is applied automatically and the query executes

#### Scenario: Genuinely unreachable filter keeps the established behavior
- **WHEN** a filter references a model with no resolvable join path from the producer root
- **THEN** it is excluded with the dropped-filter warning and strict errors, exactly as before
