# queries/cross-model-aggregates Specification

## Purpose
Defines how aggregates over joined models compose with the query surface: where a cross-model aggregate's value is computed, which query dimensions it may vary along (fan-out safety and attribution), how it broadcasts across the rest, the strict mode that turns silent broadcasts into errors, and the metadata that reports them.

## Requirements

### Requirement: Target-rooted computation with metric independence
A cross-model aggregate SHALL be computed over the rows of the model its source names (its root), never over the query root's join-multiplied rows. Adding a cross-model aggregate MUST NOT change the result row count, any other column's values, or any other metric's values, and its own value MUST NOT depend on which other metrics are present.

#### Scenario: Joined sum is not multiplied by join fan-out
- WHEN a query rooted at `orders` selects `customers.spend:sum` grouped by a customer-level dimension, and customers have several orders each
- THEN each cell's value counts every customer's spend exactly once, by executed values, regardless of how many orders each customer has

#### Scenario: Adding a cross-model measure is cardinality-neutral
- WHEN any supported query runs with and without an additional cross-model measure
- THEN both runs return the same rows and identical values in all shared columns

### Requirement: Fan-out-safe grain with broadcast
A metric's implicit grain is the query dimension set. Each aggregate SHALL be computed at the subset of that grain attributable to its root — the dimensions whose path from the root crosses only provably many-to-one join hops — and its value SHALL be broadcast across the remaining dimensions. Dimensions at the aggregate's own root or reached from it over provably-safe hops MUST retain exact per-dimension values; only unattributable dimensions broadcast.

#### Scenario: Attributable dimension keeps exact values
- WHEN a query rooted at `orders` selects `customers.spend:sum` by a dimension on `customers` (or reached from `customers` over a provably many-to-one join)
- THEN each dimension value's cell carries the exact aggregate for that slice, by executed values

#### Scenario: Unattributable dimension broadcasts
- WHEN a query rooted at `orders` selects `customers.spend:sum` by an `orders`-level dimension (unreachable from `customers` over provably many-to-one hops)
- THEN every cell of the same attributable slice carries the same broadcast value — the aggregate computed without that dimension — and the result grain is unchanged

#### Scenario: Unproven join arity broadcasts rather than fanning out
- WHEN a dimension is reachable from the aggregate's root only across a join whose arity is neither declared many-to-one/one-to-one nor structurally proven
- THEN the aggregate broadcasts across that dimension instead of joining through it, and never silently double-counts

### Requirement: Broadcast metadata
Whenever an aggregate's implicit grain loses a dimension to broadcasting, the response SHALL carry a machine-readable warning naming the affected metric, each broadcast dimension, and a per-dimension reason (unproven join hop, or unreachable from the root). One warning SHALL be emitted per distinct aggregate: identified by its public measure name when directly selected, else by its canonical aggregate form and role (expression, order, or filter). Hidden and filter-only aggregate uses emit warnings too. Explicit `partition_by=` broadcasting is by design and MUST NOT warn.

#### Scenario: Broadcast is reported per metric and dimension
- WHEN a lenient-mode query broadcasts `customers.spend:sum` over `orders.status`
- THEN the response warnings include an entry naming that measure, the `status` dimension, and the reason, and a matching Python-level warning is emitted

#### Scenario: The same aggregate in several roles warns once
- WHEN one broadcast aggregate appears as a measure, in a filter, and in ORDER BY
- THEN exactly one broadcast warning is emitted for it

### Requirement: Strict mode
`SlayerQuery.strict` (default false) SHALL turn every silent-semantics event into a clear error: an implicit-grain broadcast, or a filter actually excluded from a producer (unreachable, ambiguous, or outside semi-join pushdown scope). A filter applied by semi-join pushdown is correctly applied and MUST NOT error. The error names the metric, the dimension or filter, and the remedy (declare join cardinality, a covering unique key, or remove the dimension/filter). Explicit `partition_by=` broadcasting does not error.

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

### Requirement: Unsafe aggregate inputs fail closed
An aggregate whose inputs — positional args, keyword args (including aggregation-parameter fragments), or measure-level column filter references — cross a join hop that is not provably many-to-one from the aggregate's root SHALL fail with a clear error in both modes, whatever the aggregate's root: target-rooted, host-rooted, and local aggregates alike. Multiplying a host-side operand through a fanning join is ambiguous and MUST never silently compute over multiplied rows. The rule applies per input role: a *filter reference* or *argument* crossing an unproven hop fails closed, whatever the aggregate's root. A crossing *source* stays legal only where the aggregate is evaluated over the join result at host grain — a host-grain wrap (an ORDER BY sort key over an unprojected joined column) consumes the target's values per matched row and keeps its established values. A target-rooted cross-model producer re-roots its source to the target; a source that then reads through an unproven hop fans the aggregate and fails closed like any other crossing input.

#### Scenario: Aggregate reading through an unproven join errors
- WHEN an aggregate's column filter references a column across a join with unproven arity from the aggregate's root
- THEN the query fails with an error naming the input and the join hop, and the remedy

#### Scenario: Local measure with a filter over a provably safe hop keeps exact values
- WHEN a local measure's column filter references a column reached over a provably many-to-one join
- THEN the query executes with values identical to the pre-unification behavior

#### Scenario: Local measure with a filter over an unproven hop errors instead of fanning
- WHEN a local measure's column filter references a column across an unproven or one-to-many hop
- THEN the query fails with an error naming the hop and the remedy — never the silently multiplied aggregate this shape previously produced

#### Scenario: Host-grain wrap over a to-many source stays legal
- WHEN a query orders by an aggregate of a joined column evaluated at host grain across a to-many join
- THEN the aggregate evaluates over the joined rows as before, with unchanged executed values

### Requirement: Explicit grain and window on cross-model aggregates
Cross-model aggregates SHALL accept `partition_by=`, `window=`, and `first`/`last`. Every explicit partition key MUST be attributable from the aggregate's root — an unattributable key is a hard error in both modes, naming the remedy. A windowed cross-model aggregate requires the query's active time dimension attributable from its root, else errors.

#### Scenario: Cross-model partitioned aggregate computes at the declared grain
- WHEN a query selects `customers.spend:sum(partition_by=<customer-level dimension>)`
- THEN the value is computed at exactly the declared grain and broadcast to the query rows, by executed values

#### Scenario: Unattributable explicit partition key errors
- WHEN a cross-model aggregate declares `partition_by=` naming a dimension not attributable from its root
- THEN the query fails with a clear error naming the key and the remedy, in lenient and strict mode alike

#### Scenario: Cross-model first/last and windowed aggregates work
- WHEN a query selects a `first`/`last` or `window=` aggregate over a joined model's column with an attributable grain
- THEN the value is correct by executed values and result cardinality is unchanged

### Requirement: Cross-model aggregates compose in expressions and dimensions
Cross-model aggregates SHALL be legal wherever local aggregates are: in arithmetic and scalar-call composites (including mixed with local aggregates and with aggregates from different joined models in one expression), inside transforms, in dimension expressions, in filters, and in ORDER BY. A computed dimension whose expression columns are all attributable from a metric's root participates in that metric's grain; otherwise the metric broadcasts across it. Consumption-position rules match local aggregates exactly: a combined-position consumer of a cross-model partitioned aggregate needs query-dimension partition keys (per the partitioned-aggregates combined-consumer requirement), while row-scope references to a computed dimension's own aggregate stay legal at any partition grain.

#### Scenario: Local and cross-model aggregates in one expression
- WHEN a query selects the measure `orders.revenue:sum / customers.spend:sum`
- THEN each cell's value is the ratio of the two correctly-computed aggregates, by executed values

#### Scenario: Cross-model aggregate source inside a computed dimension
- WHEN a query declares a dimension banding `customers.spend:sum(partition_by=<customer-level dimension>)`
- THEN rows group by the band with correct executed values and unchanged cardinality

#### Scenario: Computed dimension coexists with a cross-model measure
- WHEN a query combines an aggregation-derived dimension (banded, bare, or transform-root) with a cross-model measure
- THEN both are correct by executed values in one result, replacing the former fail-closed guard

#### Scenario: Keyless-grain dual-role partitioned aggregate is rejected
- WHEN the same cross-model partitioned aggregate is consumed by a computed dimension and selected as a measure (or named as a raw ORDER BY target) while its partition key is not among the query dimensions
- THEN the query fails at plan time with the clear partition-key error the local variant raises — naming the key and the remedy — never with an internal join-back failure

#### Scenario: Keyless filter over the dimension's own cross-model aggregate executes
- WHEN a query with a computed dimension banding a keyless cross-model partitioned aggregate filters on that same aggregate
- THEN the query executes with the filter row-routed exactly like the local shape — the predicate applies per base row against the attached partition-grain value before aggregation — and the emitted SQL contains one producer relation for the aggregate, not a combined twin

#### Scenario: Keyless ORDER BY the computed dimension's name executes
- WHEN a query with a computed dimension banding a keyless cross-model partitioned aggregate orders by that dimension's name
- THEN rows sort by the banded value, by executed values, with no combined attach synthesized for the order reference

### Requirement: Producer filter inheritance
A ROW-phase filter conjunct whose references are all attributable from an aggregate's root SHALL apply inside that aggregate's computation. A conjunct reachable from the root only across hops that are not provably many-to-one SHALL still restrict the aggregate's population, by semi-join: the aggregate is computed over exactly the root rows related to at least one row (combination) passing the conjunct — never over join-multiplied rows — silently and without metadata, uniformly with inline inheritance. On provably many-to-one hops the semi-join is semantically identical to inline application, and inline remains a pure optimization. Reference resolution uses each reference's full dependency set: a derived (SQL-defined) column's classification follows the models its definition actually reads, not just its declared location.

Semi-join pushdown SHALL apply uniformly to every target-rooted producer — plain, partitioned, ranked, windowed, and nested computed-dimension producers. Conjuncts pushed into the same producer that share their first reverse hop SHALL be satisfied by the same related row (combination); conjuncts on different branches are satisfied independently.

A conjunct SHALL remain excluded from the producer — reported through the established dropped-filter warning (and erroring under strict) while still applying to the result rows — when it is genuinely unreachable (no resolvable join path from the root), when its cross-path references span multiple distinct join branches within one conjunct, when root-local and cross-path references mix under a disjunction or negation, or when the reverse path is ambiguous. The reverse path resolves through stored join edges and, for semi-join correlation only, through inversion of a stored forward edge; inversion MUST never be used to classify a conjunct as safely inlineable. AGGREGATE-phase predicates keep aggregate-filter semantics uniform with local aggregates: they restrict the result rows by the aggregate's attached value, including when the aggregate appears only in the filter.

#### Scenario: Attributable filter restricts the metric
- **WHEN** a query rooted at `orders` filters on a customer-level predicate and selects `customers.spend:sum`
- **THEN** the metric is computed over only the customers passing the predicate

#### Scenario: Aggregate-phase filter restricts result rows uniformly
- **WHEN** a query rooted at `orders` groups by a customer-level dimension and filters on `customers.spend:sum > 100`
- **THEN** only groups passing the predicate remain in the result — exactly as a local aggregate filter behaves — whether or not the aggregate is also selected

#### Scenario: Unsafe filter no longer fans out the producer
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects `customers.spend:sum`
- **THEN** the metric counts exactly the customers with at least one order passing the predicate, each customer's spend once (never double-counted through the reverse join), with no warning and unchanged result cardinality

#### Scenario: Pushed filter still restricts the result rows
- **WHEN** a lenient-mode query pushes a filter into a producer by semi-join
- **THEN** the filter also still applies to the result rows exactly as before

#### Scenario: Filters sharing a branch bind to the same related row
- **WHEN** a query rooted at `orders` filters `status = 'paid'` and `channel = 'app'` and selects `customers.spend:sum`, and a customer has a paid order and an app order but no single paid app order
- **THEN** that customer is excluded from the metric's population — both predicates must hold on one related row, by executed values

#### Scenario: Pushdown works without a declared reverse join
- **WHEN** the only stored edge is the forward `orders → customers` join (default join type, no mirrored reverse edge) and a query rooted at `orders` filters on an orders-level predicate with `customers.spend:sum` selected
- **THEN** the filter pushes down by semi-join over the inverted forward edge, with correct executed values

#### Scenario: Ambiguous reverse path stays dropped and warned
- **WHEN** the filtered model reaches the producer root through several distinct forward joins and no stored reverse edge disambiguates the correlation
- **THEN** the conjunct is excluded with the established dropped-filter warning (strict errors) rather than guessing a correlation

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

### Requirement: Intermediate-hop dimensions are supported
A dimension lying on an intermediate hop of a cross-model aggregate's join chain SHALL be legal. It follows the attribution rule like any other dimension: exact when attributable from the aggregate's root, broadcast (with metadata) when not — never an internal not-implemented error.

#### Scenario: Intermediate-hop dimension broadcasts under attribution
- WHEN a query rooted at `orders` selects `customers.regions.pop:sum` by a `customers`-level dimension
- THEN the query executes (no not-implemented error); the metric broadcasts across the customer-level dimension with metadata, since a region's population is not attributable per customer

### Requirement: Every aggregate has exactly one disposition
Planning SHALL guarantee that every aggregate reference — measure, composite leaf, computed-dimension, filter-only, order-only, windowed, ranked, host-grain, or nested — is either computed inline, routed to exactly one producer, or rejected with a clear error. A discovery gap MUST surface as an explicit planner error, never as a silently dropped or wrong value.

#### Scenario: Unrouted shapes fail loudly
- WHEN a query contains an aggregate shape the planner cannot route
- THEN the query fails with a clear error naming the shape, never with missing or incorrect values

### Requirement: Existing cross-model behavior is preserved where already safe
Cross-model shapes supported before this change whose grains were already fan-out-safe SHALL keep identical executed values, and golden SQL stays byte-identical except individually approved divergences. Shapes whose values or errors change (arity-unsafe grains broadcasting, unsafe inputs and unsafe explicit partition keys erroring, previously-dropped reachable filters now restricting the metric by semi-join, and semi-join queries on pre-25.4 ClickHouse now failing closed) are enumerated and individually approved.

#### Scenario: Safe cross-model goldens hold
- **WHEN** the golden-SQL and executed-value suites for previously supported, fan-out-safe cross-model shapes run
- **THEN** executed values are unchanged and SQL divergences are only the individually approved ones

#### Scenario: Provably safe filter paths keep byte-identical SQL
- **WHEN** a filter's path from the producer root crosses only provably many-to-one hops
- **THEN** the generated SQL keeps the inline form, byte-identical to before this change
