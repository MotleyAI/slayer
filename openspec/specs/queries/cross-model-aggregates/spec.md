# queries/cross-model-aggregates Specification

## Purpose
Defines how aggregates over joined models compose with the query surface: where a cross-model aggregate's value is computed, which query dimensions it may vary along (fan-out safety and attribution), how it broadcasts across the rest, the strict mode that turns silent broadcasts into errors, and the metadata that reports them.

## Requirements

### Requirement: Target-rooted computation with metric independence
A cross-model aggregate SHALL be computed over the rows of its home dataset (its
root) — the model its source names for a single-column source, the dataset per
`queries/semantics` › Home dataset of a row-level aggregation source for an
expression source — never over the query root's join-multiplied rows. Adding a
cross-model aggregate MUST NOT change the result row count, any other column's
values, or any other metric's values, and its own value MUST NOT depend on which
other metrics are present.

#### Scenario: Joined sum is not multiplied by join fan-out
- WHEN a query rooted at `orders` selects `customers.spend:sum` grouped by a customer-level dimension, and customers have several orders each
- THEN each cell's value counts every customer's spend exactly once, by executed values, regardless of how many orders each customer has

#### Scenario: Expression source homed at the joined model is not multiplied
- **WHEN** a query rooted at `orders` selects `sum(customers.spend - customers.regions.pop)`
  grouped by a customer-level dimension, and customers have several orders each
- **THEN** the source is homed at `customers` (its single home), so `spend - pop` is
  evaluated once per customer — `regions.pop` joined once per customer, NOT once per
  region — and summed over the customers in each cell, by executed values, independent
  of how many orders each customer has
- Because the whole expression shares the `customers` home, this is NOT equal to
  `customers.spend:sum - customers.regions.pop:sum`, whose two operands home at different
  models (`pop` counted once per region): a valid comparison is only with an aggregation
  that shares the same `customers` home

#### Scenario: Adding a cross-model measure is cardinality-neutral
- WHEN any supported query runs with and without an additional cross-model measure
- THEN both runs return the same rows and identical values in all shared columns

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

### Requirement: Unsafe aggregate inputs fail closed
An aggregate whose inputs — positional args, keyword args (including aggregation-parameter fragments and the aggregation definition's non-overridden defaults), or measure-level column filter references — cross a join hop that is not provably many-to-one from the aggregate's root SHALL fail with a clear error in all three modes, whatever the aggregate's root: target-rooted, host-rooted, and local aggregates alike. An input's dependencies are its *dependency closure*: its own join path plus every join path the definition of any derived column it names crosses, recursively through chains of derived columns, whether the reference is bare or path-bearing — a derived column whose definition crosses a fanning hop is an unsafe input exactly as a structural reference across that hop is. An input whose derived-column definition cannot be analyzed for join dependencies SHALL fail closed with a typed error naming the aggregate and the column — never be treated as crossing nothing. Multiplying a host-side operand through a fanning join is ambiguous and MUST never silently compute over multiplied rows. The rule applies per input role: a *filter reference* or *argument* crossing an unproven hop fails closed, whatever the aggregate's root. A crossing *source* stays legal only where the aggregate is evaluated over the join result at host grain — a host-grain wrap (an ORDER BY sort key over an unprojected joined column) consumes the target's values per matched row and keeps its established values; a definition default's references local to its owner ride with the source. A target-rooted cross-model producer re-roots its source to the target; a source that then reads through an unproven hop fans the aggregate and fails closed like any other crossing input. An attached (aggregate- or transform-valued) input is opaque to this rule: its own inputs are judged by its own producer at its own home, never by the enclosing aggregate. When an explicit column argument is the violation, the error SHALL name that argument and the hop it crosses, taking precedence over the closure's hop-only message — a derived argument included, judged by its closure. The *ranking key* of a `first`/`last` aggregation is an input under this rule whichever way it is chosen: the explicit positional argument, else the producer's first temporal row dimension, else the time dimension's raw column, else the model's `default_time_dimension` — a key whose dependency closure crosses a hop that is not provably many-to-one from the producer's root SHALL fail closed in every mode, naming the column and the hop, for host-rooted, target-rooted and `window=` producers alike, and a key whose derived definition cannot be analyzed SHALL fail closed naming the column; a ranked producer's grain may still fan (each cell ranks the rows that reached it) — only the ordering key is refused.

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

#### Scenario: Path-bearing derived argument crossing a fanning hop fails closed
- WHEN a query rooted at `orders` selects `customers.spend:weighted_avg(weight=customers.regions.bad_pop)`, where `regions.bad_pop` is defined as `pop + region_events.value` over the one-to-many `regions → region_events` hop, in any `to_many_handling` mode
- THEN the query fails with the unproven-join-hop error naming the argument `bad_pop` and the hop `region_events`, never emitting the multiplying join; the same holds when the argument names a derived column defined over that derived column (`bad_pop * 2`)

#### Scenario: Local aggregate with a derived crossing argument fails closed
- WHEN a query rooted at `orders` selects `amount:weighted_avg(weight=customers.regions.bad_pop)`
- THEN the query fails with the unproven-join-hop error naming `region_events`

#### Scenario: Definition default naming a fanning derived column fails closed
- WHEN a custom aggregation on `customers` defaults a parameter to `regions.bad_pop`, or to the expression `regions.bad_pop * 1`, and a query selects `customers.spend:<that aggregation>`
- THEN the query fails with the unproven-join-hop error naming `region_events`

#### Scenario: Measure-local filter naming a fanning derived column fails closed
- WHEN a column's measure-level filter references a derived column on a joined model whose definition crosses a fanning hop (`regions.bad_pop > 0` on a `customers` column) and a query aggregates that column
- THEN the query fails with the unproven-join-hop error naming `region_events`

#### Scenario: Derived dependencies over provably to-one hops stay legal
- WHEN an argument or definition default names a derived column reached over a provably to-one path whose own definition is local to its model (`regions.derived_pop` = `pop * 2`)
- THEN the query executes with the derived expansion applied, by executed values, unchanged from today

#### Scenario: Unanalyzable derived definition fails closed
- WHEN an aggregate input names a derived column whose definition no supported dialect can parse
- THEN the query fails at plan time with a typed error naming the aggregate and the column, containing no issue reference — never a plan that treats the column as crossing nothing

#### Scenario: Host column as a target ranking key names the column and the hop
- WHEN a query rooted at `orders` selects `customers.spend:last(ordered_at)` — a host column ranking a `customers`-rooted pick
- THEN the query fails in every mode with the input-safety error naming `ordered_at`, that it is not attributable from `customers`, and the fanning hop to `orders`

#### Scenario: Argument violation is reported ahead of a source violation
- WHEN a target-rooted aggregate both reads its source through a fanning derived definition and ranks or weights by a host column, e.g. `customers.regions.bad_pop:last(ordered_at)` rooted at `orders`
- THEN the error names the argument `ordered_at` and its hop, not the source's hop — the more specific violation wins

#### Scenario: Implicit model-default ranking key across an unproven hop fails closed
- WHEN `orders.default_time_dimension` names a derived column `li_ts` defined as `line_items.created_at` over the undeclared, reverse-PK-only `orders → line_items` hop, and a query selects `amount:last` (or `amount:first`) with no time dimension and no temporal dimension
- THEN the query fails in every mode with the input-safety error naming `li_ts`, the hop `line_items` and the host `orders` — never the ranked CTE joining `line_items` and ranking over multiplied rows

#### Scenario: Target model default ranking key across an unproven hop fails closed
- WHEN `line_items.default_time_dimension` names a derived column `sh_ts` defined as `shipments.shipped_at` over an unproven `line_items → shipments` hop, and a query rooted at `orders` selects `line_items.qty:last` with no time dimension
- THEN the query fails in every mode with the cross-model input-safety error naming `sh_ts`, that it is not attributable from `line_items`, and the hop `shipments`

#### Scenario: Fanning temporal dimension as the implicit ranking key fails closed in every mode
- WHEN a query rooted at `orders` groups by `line_items.created_at` (a TIMESTAMP column across the unproven `orders → line_items` hop) and selects `amount:last`, under `broadcast`, `error` or `associate`, with or without `window=` on the measure alongside a safe time dimension
- THEN the query fails with the input-safety error naming `created_at` and the hop `line_items`, exactly as `amount:last(line_items.created_at)` does — never a ranked CTE partitioning and ranking by the fanning column

#### Scenario: Time dimension across an unproven hop as the implicit ranking key fails closed
- WHEN a query rooted at `orders` declares `line_items.created_at` as its only time dimension and selects `amount:last`
- THEN the query fails with the input-safety error naming `created_at` and the hop `line_items`

#### Scenario: Explicit derived ranking argument names the argument and the hop
- WHEN a query rooted at `orders` selects `amount:last(li_ts)`, `li_ts` being the derived column across the unproven hop
- THEN the error names the argument `li_ts` and the hop `line_items` (the "ranks/reads by" form), not the closure's hop-only message; `amount:last(line_items.created_at)` keeps naming `created_at` and `line_items`

#### Scenario: Default ranking key whose column filter crosses an unproven hop fails closed
- WHEN `orders.default_time_dimension` names a column whose `Column.filter` references `line_items.qty` across the unproven hop, and a query selects `amount:last` with no time dimension
- THEN the query fails with the input-safety error naming that column and the hop `line_items`

#### Scenario: Unanalyzable default ranking key fails closed
- WHEN `orders.default_time_dimension` names a derived column whose definition no supported dialect can parse, and a query selects `amount:last` with no time dimension
- THEN the query fails at plan time with the typed unanalyzable-dependency error naming the aggregate and the column — never a plan that ranks by it

#### Scenario: Safe ranking keys keep executing
- WHEN the resolved ranking key is a local column (the model default, or a filter-only temporal column with no default), a derived column over a provably to-one hop (`cust_signup` = `customers.signup_at` with `customers.id` a primary key), or — for `amount:last(window='30d')` on a `created_at` time dimension — the bucket's raw column while the model default crosses the unproven hop
- THEN each query executes with its established value (the proven-hop case joins `customers` and picks the row with the latest signup; the windowed case never joins `line_items`), and the derived key renders as its plain expansion with no added CAST in both the plain and the windowed ranked CTE

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

### Requirement: Cross-model aggregates compose in expressions and dimensions
Cross-model aggregates SHALL be legal wherever local aggregates are: in arithmetic and scalar-call composites (including mixed with local aggregates and with aggregates from different joined models in one expression), inside transforms, in dimension expressions, in filters, and in ORDER BY. Composite legality is uniform across the composite's own shape: a cross-model operand SHALL compile whether the composite combines it with local aggregates, with literals, with several cross-model operands, or wraps it in scalar calls — the compiled route never depends on which seam the composite would otherwise render through, and no composite shape reaches an internal not-supported seam error. A computed dimension whose expression columns are all attributable from a metric's root participates in that metric's grain; otherwise the metric broadcasts across it. Consumption-position rules match local aggregates exactly: a combined-position consumer of a cross-model partitioned aggregate needs query-dimension partition keys (per the partitioned-aggregates combined-consumer requirement), while row-scope references to a computed dimension's own aggregate stay legal at any partition grain.

#### Scenario: Local and cross-model aggregates in one expression
- WHEN a query selects the measure `orders.revenue:sum / customers.spend:sum`
- THEN each cell's value is the ratio of the two correctly-computed aggregates, by executed values

#### Scenario: Scalar call wrapping a cross-model operand executes
- **WHEN** a query selects a scalar-call composite over a cross-model aggregate mixed with
  a local aggregate and a literal (for example `round(customers.spend:sum / amount:sum, 2)`)
- **THEN** the composite executes with correct hand-computed values on SQLite and DuckDB —
  never the former AGGREGATE-phase-composite not-yet-supported error

#### Scenario: Multiple cross-model operands in one composite execute
- **WHEN** a composite combines two cross-model aggregates (same or different joined
  models) with no local operand
- **THEN** each operand is computed in its own producer and the composed value is correct
  by executed values

#### Scenario: Cross-model aggregate source inside a computed dimension
- WHEN a query declares a dimension banding `customers.spend:sum(partition_by=<customer-level dimension>)`
- THEN rows group by the band with correct executed values and unchanged cardinality

#### Scenario: Computed dimension coexists with a cross-model measure
- WHEN a query combines an aggregation-derived dimension (banded, bare, or transform-root) with a cross-model measure
- THEN both are correct by executed values in one result, replacing the former fail-closed guard

#### Scenario: Filter on a cross-model partitioned aggregate executes
- WHEN a query filters on `customers.spend:sum(partition_by=<customer-level dimension>)` with that partition key among the query dimensions, whether or not the aggregate is also selected
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
A dimension lying on an intermediate hop of a cross-model aggregate's join chain SHALL be legal. It follows the attribution rule like any other dimension: exact when attributable from the aggregate's root, broadcast (with metadata) when not — never an internal not-implemented error.

#### Scenario: Intermediate-hop dimension broadcasts under attribution
- WHEN a query rooted at `orders` selects `customers.regions.pop:sum` by a `customers`-level dimension
- THEN the query executes (no not-implemented error); the metric broadcasts across the customer-level dimension with metadata, since a region's population is not attributable per customer

### Requirement: Every aggregate has exactly one disposition
Planning SHALL guarantee that every aggregate reference — measure, composite leaf, computed-dimension, filter-only, order-only, windowed, ranked, host-grain, or nested — is either computed inline, routed to exactly one producer, or rejected with a clear error. A discovery gap MUST surface as an explicit planner error, never as a silently dropped or wrong value.

Beyond routing, planning SHALL assign every value the plan carries — row leaf, aggregate, producer placeholder, composite, transform, filter predicate — exactly one materialisation stage in the emitted statement's pipeline (host base, producer, combined, derived level — where a value reading a transform is staged one level above the deepest transform it reads: a transform is one more relation layer, a composite renders inline and is never a layer of its own), derived only from the stages of the values it references, together with whether any consumer requires it as a column. A plan in which a value references a value of a later stage, or in which any value is left unstaged, SHALL be rejected at plan time with a typed error before any SQL is generated. SQL generation SHALL place each value in the relation its stage names and read earlier relations by alias only, never re-deriving placement from the value's shape; a value whose stage names a relation that cannot render it MUST fail closed, never render at the wrong grain.

#### Scenario: Unrouted shapes fail loudly
- WHEN a query contains an aggregate shape the planner cannot route
- THEN the query fails with a clear error naming the shape, never with missing or incorrect values

#### Scenario: Every planned value carries one stage
- **WHEN** any query is planned, including the producer bodies nested inside it
- **THEN** every value in the plan carries exactly one materialisation stage; a value that
  reads a transform is staged strictly later than every transform it reads, and no value
  is staged earlier than any operand

#### Scenario: A transform over a composite over a transform
- **WHEN** a query projects `change(x)` and filters on `last(change(x))`
- **THEN** `change(x)` is staged one level above its `time_shift`, `last(change(x))`
  shares the composite's level (only transforms stratify; the composite renders inline),
  and the query executes selecting the declining partition (pinned by the DEV-1859
  last-over-change test)

#### Scenario: Staging is a function of the term alone
- **WHEN** the same query filters on `last(change(x))` with and without also projecting
  `change(x)`
- **THEN** every shared value carries the same stage in both plans and the surviving rows
  are identical

#### Scenario: Deeper alternation of transforms and composites
- **WHEN** a transform's operand is a composite over a transform over a transform (for
  example `last(change(cumsum(x)))`)
- **THEN** each transform is staged one level above the deepest transform it reads and
  the query plans with no construction-inspecting refusal

#### Scenario: Later-stage reference is rejected at plan time
- **WHEN** a plan is constructed in which a value references a value staged later than
  itself, in which a transform reads a transform not staged strictly earlier, or in which
  some value has no stage
- **THEN** plan construction fails with a typed error naming the values, and no SQL is
  generated

#### Scenario: Operands needed later are materialised earlier
- **WHEN** a value at a later stage (a transform, a composite reading a derived value, a
  filter predicate, an ORDER BY key) reads a value materialised at an earlier stage that
  is not otherwise selected
- **THEN** the earlier value is projected as a hidden column of its own relation and the
  later value reads it by alias, by executed values and by the generated SQL

#### Scenario: Already-legal shapes keep their SQL

- **WHEN** the golden-SQL suites for every previously supported shape run after
  materialisation becomes planner-owned
- **THEN** generated SQL is byte-identical except for individually approved divergences,
  and executed values are unchanged for every divergence

### Requirement: Existing cross-model behavior is preserved where already safe
Cross-model shapes supported before this change whose grains were already fan-out-safe SHALL keep identical executed values, and golden SQL stays byte-identical except individually approved divergences. Shapes whose values or errors change (arity-unsafe grains broadcasting, unsafe inputs and unsafe explicit partition keys erroring, previously-dropped reachable filters now restricting the metric by semi-join, and semi-join queries on pre-25.4 ClickHouse now failing closed) are enumerated and individually approved.

#### Scenario: Safe cross-model goldens hold
- **WHEN** the golden-SQL and executed-value suites for previously supported, fan-out-safe cross-model shapes run
- **THEN** executed values are unchanged and SQL divergences are only the individually approved ones

#### Scenario: Provably safe filter paths keep byte-identical SQL
- **WHEN** a filter's path from the producer root crosses only provably many-to-one hops
- **THEN** the generated SQL keeps the inline form, byte-identical to before this change

### Requirement: Producer filter routing
A ROW-phase filter conjunct whose references are all attributable from an aggregate's
root SHALL apply inside that aggregate's computation. A conjunct reachable from the
root only across hops that are not provably many-to-one SHALL still restrict the
aggregate's population, by semi-join: the aggregate is computed over exactly the root
rows for which the conjunct holds on at least one row of the root row's join product over
the branches the conjunct references — the product built as the inline path would join
it (each hop with its declared join type, LEFT by default, so a hop with no related row
contributes NULL columns), root-local references taking the root row's values, SQL
three-valued logic applying inside and the restriction itself never unknown — never over
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
conjunct. Conjuncts pushed into the same producer that share a join branch SHALL be
satisfied by the same related row (combination) of that branch, whichever spelling names
the branch's edge (its name or its target model); conjuncts on disjoint branches are
satisfied independently, and a conjunct spanning several branches is judged on their
product.

Every producer rooted at the query population and built by the host regroup path —
partitioned, windowed, first/last, host-grain wrap, broadcast-local — SHALL additionally
inherit the query population's own filter disposition, computed once at the host root:
a population conjunct the host applies inline applies inline, a population conjunct
restricting the population by association restricts the producer by the same semi-join;
a producer nested inside such a producer and rooted at the same population
inherits the same disposition during its own compilation. Such a producer's own grain — its
partition keys and its window time axis — is attributable from the population (an
unattributable partition key or window time axis is a typed error, per *Explicit grain and
window on cross-model aggregates*), so it takes the population's semi-join, never an inline
join across the fanning hop. This
population inheritance is keyed on the producer's kernel, not on whether its root equals
the host: an association producer (even one whose root is the host) keeps the association
routing of the paragraph above and receives no population semi-join, and a target-rooted
producer keeps its own metric-root disposition.

Pushdown SHALL be total over the conjunct's boolean shape: a root-local and a cross-path
reference mixed under a disjunction or negation, cross-path references spanning several
distinct join branches, and an atom comparing columns of two branches are all restricted by
association with the product semantics above — negation keeping the existential reading
(`NOT B` holds when some related row fails `B`) and a null-test on a related column holding
for a root row with no related row — never dropped and never an error in any mode. A
reference with no resolvable join path from the root SHALL be refused at resolution in
every mode with a typed error, never routed as if it crossed nothing — there is no
producer-level silent drop. The reverse path resolves through the same bidirectional
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

#### Scenario: Two spellings of one edge bind to the same related row
- **WHEN** the `customers → orders` edge is named `purchases` and a query rooted at
  `customers` selects `spend:sum` by `tier` with
  `filters: ["purchases.status = 'ok'", "orders.channel = 'app'"]`
- **THEN** by executed values each cell counts the customers having one order that is both
  `ok` and `app` (gold 60, silver 80 on the reference dataset, never gold 160 / silver 230),
  and the generated SQL correlates a single `orders` relation for both conjuncts

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
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by `customers.tier`
  with `filters: ["customers.tier = 'bronze' OR channel = 'app'"]`, in any mode
- **THEN** it is no longer dropped: by executed values each cell counts the distinct customers
  that are bronze or have at least one `app` order, each once (gold 160, silver 230, bronze 40
  on the reference dataset, never gold 245), the response carries the informational entry naming the
  aggregate and the filter with no dropped-filter warning, and `to_many_handling: "error"`
  does not error

#### Scenario: Mixed disjunction on the association producer binds to the same related row
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by `status` under
  `to_many_handling: "associate"` with `filters: ["customers.tier = 'gold' OR channel = 'app'"]`
- **THEN** by executed values each status cell counts the distinct customers having an
  order of that status that is itself `app` or belongs to a gold customer (ok 270, new 250 on
  the reference dataset), reported through the informational entry with no dropped-filter
  warning

#### Scenario: Association filter on a branch absent from the query restricts membership
- **WHEN** the same association query filters on
  `customers.tier = 'gold' OR customers.plans.level = 'basic'` with the `customers → plans`
  hop unproven
- **THEN** by executed values each status cell counts the distinct gold or basic-plan
  customers having an order of that status (ok 270, new 100 on the reference dataset) — the
  `plans` branch is joined for the filter alone

#### Scenario: Derived-column dependencies drive classification
- **WHEN** a filter references a SQL-defined column whose definition reads a model
  across a hop that is not provably many-to-one from the producer root
- **THEN** the conjunct is classified by those actual dependencies — pushed by
  semi-join, never inlined through the unsafe hop

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
- **WHEN** a filter references a model with no resolvable join path from the query root
- **THEN** the query fails with a typed error in every `to_many_handling` mode — the
  filter is never dropped from a producer and never routed as if it crossed nothing

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

#### Scenario: Existing semi-join shapes keep byte-identical SQL
- **WHEN** any query whose pushed conjuncts already restricted by semi-join before this
  change is planned again
- **THEN** its generated SQL is byte-identical on every Tier-1 dialect — every hop of every
  correlation tree renders as the inner correlation it rendered before — and only a conjunct
  whose predicate can hold on a hop's null-extended row renders that hop as a left join from
  a one-row spine
