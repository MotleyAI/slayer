# queries/attribution-modes Specification

## Purpose
Defines the query-level `to_many_handling` mode axis: how every aggregate — cross-model
and local — resolves query dimensions not attributable from its root (broadcast,
associate, or error), the association semantics and its eligibility rules, and the
retirement of the legacy `strict` flag.

## Requirements

### Requirement: Query-level mode selection
A query SHALL accept `to_many_handling` with values `"broadcast"`, `"associate"`, and
`"error"`, defaulting to `"broadcast"`. The mode governs, uniformly for cross-model and
local aggregates alike and in every consumer context (measure, composite leaf, filter,
ORDER BY, computed dimension, nested producer), how each pair of (aggregate, query
dimension unattributable from the aggregate's root) resolves. A dimension is attributable
from a root iff every path in its dependency closure — its own join path and every path
the definition of a derived column it names crosses, recursively — is provably
many-to-one from that root; a dimension naming a derived column whose definition crosses
a fanning hop is unattributable exactly as a structural fanning dimension is. The mode
MUST NOT affect aggregates all of whose grain dimensions are attributable, dimension-free
totals, or filter pushdown (semi-join pushdown applies in every mode). An unrecognized
value SHALL fail with a clear validation error.

#### Scenario: Default is broadcast and byte-identical for cross-model shapes
- **WHEN** a query omitting `to_many_handling` broadcasts a cross-model metric over an
  unattributable dimension
- **THEN** generated SQL and executed values are identical to the pre-change broadcast
  behavior, and the response carries the broadcast warning

#### Scenario: Fully attributable queries are mode-invariant
- **WHEN** the same query whose metrics are all computable at the full query grain runs
  once per mode value
- **THEN** all three runs return identical executed values with no mode-related
  warnings or errors

#### Scenario: Unrecognized mode value fails
- **WHEN** a query sets `to_many_handling` to a value outside the three modes
- **THEN** the query fails with a clear validation error naming the accepted values

#### Scenario: Derived fanning dimension broadcasts with a warning by default
- **WHEN** a query rooted at `orders` selects the local `amount:sum` and the cross-model
  `customers.spend:sum` by the dimension `customers.regions.bad_pop`, a derived column
  defined as `pop + region_events.value` over the one-to-many `regions → region_events`
  hop, omitting `to_many_handling`
- **THEN** each metric is broadcast across that dimension with a warning naming the hop,
  by executed values — never the figure multiplied once per region event

#### Scenario: Derived fanning dimension associates per cell
- **WHEN** the same query runs with `to_many_handling: "associate"`
- **THEN** each cell aggregates over the distinct root entities associated with that
  dimension value — each order once for the local metric, each customer once for the
  cross-model one — by hand-computed executed values on SQLite and DuckDB

#### Scenario: Derived fanning dimension refuses under error mode
- **WHEN** the same query runs with `to_many_handling: "error"`
- **THEN** the query fails with the error-mode broadcast refusal naming the metric and the
  dimension

### Requirement: Distinct-entity association semantics
Under `to_many_handling: "associate"`, an aggregate with at least one unattributable
grain dimension SHALL return, for each result cell, the aggregate over the distinct
home entities associated with that cell — each entity counted exactly once per cell,
deduplicated by the home model's unique key. Association is defined by the join path
from the aggregate's home dataset to the dimension (Axiom 3, Association): an entity
belongs to a cell iff its own path reaches the cell's dimension values, so a home entity
with no population row still counts in every cell its path reaches, and the origin of the
population's rows never restricts the association. A dimension the home reaches only back
through the population root associates an entity only when at least one population row
carries it: an entity with no population row is in no such cell, never in a manufactured
NULL cell. A population rooted at the home itself keeps its own row set: an entity with no
related row sits in the NULL cell exactly as its population row does. The result grain,
row count, sibling metrics, and other columns' values MUST be unchanged relative to the
same query without the aggregate. Entity populations of different cells may overlap;
cells are therefore not additive across the unattributable dimensions, and the response
warns accordingly. Attributable grain dimensions retain exact partition values identical
to broadcast mode.

#### Scenario: Cross-model metric attributes per cell by executed values
- **WHEN** a query rooted at `orders` with `to_many_handling: "associate"` selects
  `customers.spend:sum` by the orders-level dimension `status`
- **THEN** each status cell equals the summed spend of the distinct customers having
  at least one order with that status, by executed values, with unchanged result grain

#### Scenario: Local metric over a fanning dimension attributes per cell
- **WHEN** a query rooted at `customers` with `to_many_handling: "associate"` selects
  `spend:sum` by `orders.status`, and one customer has two orders with the same status
- **THEN** that customer's spend counts once in that status cell — by executed values,
  never the join-multiplied figure

#### Scenario: Adding an associated measure is cardinality-neutral
- **WHEN** any supported query runs with and without an additional associate-mode
  aggregate
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Multi-hop association attributes per cell
- **WHEN** a query rooted at `orders` under `associate` selects a metric rooted two
  hops away (e.g. `customers.regions.pop:sum`) by an orders-level dimension
- **THEN** each cell aggregates over the distinct entities of the metric's root
  associated with the cell, by executed values

#### Scenario: Home entity absent from the population counts in its home-determined cell
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum`
  and the local `amount:sum` by a regions-level dimension the customer's own path
  determines, and one South customer has no orders
- **THEN** the South cell of `customers.spend:sum` includes that customer's spend (every
  South customer once, on SQLite and DuckDB) while the local `amount:sum` cell is
  unchanged, and the response carries the associated-cells warning

#### Scenario: Dimension through the population root never manufactures a NULL cell
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum` by
  `status`, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL-status cell aggregates exactly the customers owning a NULL-status
  order, and the orderless customer appears in no cell, by executed values

#### Scenario: A population rooted at the home keeps its own NULL cell
- **WHEN** a query rooted at `customers` under `associate` selects `spend:sum` by
  `orders.status`, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL-status cell holds both the orderless customer and the owner of the
  NULL-status order, exactly the customers whose population rows carry a NULL status

#### Scenario: Mixed home-side and population-root dimensions
- **WHEN** a query rooted at `orders` under `associate` selects `customers.spend:sum` by
  both a regions-level dimension and `status`, and one South customer has no orders
- **THEN** every customer with orders is counted once in each of its (region, status)
  cells and the orderless customer is in no cell, by executed values

### Requirement: Association eligibility and input handling
Associate-mode resolution SHALL support the full plain scalar aggregation family
(including count, count_distinct, avg, min, max, median, percentile, and stddev-class
aggregations), subject to each dialect's existing aggregate capability — grouped
`median`/`percentile` remains a `NotImplementedError` on T-SQL and MySQL, unchanged by
mode (per the divergence ledger). Every input expression of the aggregation — arguments and
aggregation-parameter fragments alike — is evaluated per associated entity (constant
per entity under the established unsafe-aggregate-inputs rule, which keeps applying
unchanged); a column-reference or aggregate-valued parameter — explicit, positional, or
supplied by the aggregation definition's default — is legal exactly when the entity grain
determines it (per `queries/semantics` › Aggregation parameters are typed by the home
dataset's grain) and is then picked once per associated entity alongside the aggregate's
own value; `*:count` counts the distinct associated entities per cell. An
aggregation's own column filter restricts the associated entities before per-cell
aggregation. Combining associate-mode resolution with `window=` or `first`/`last` on
the same aggregate SHALL fail with a clear typed error naming the combination and the
remedy. An aggregate root model without a declared unique key SHALL fail associate-mode
resolution with a clear typed error naming the model and the remedy (declare a primary
or unique key).

#### Scenario: Percentile attributes over the association
- **WHEN** an associate-mode query slices a cross-model percentile aggregate by an
  unattributable dimension, on a dialect that supports grouped percentile
- **THEN** each cell's value is the percentile over the distinct associated entities'
  values, by executed values; on a dialect without grouped percentile (T-SQL, MySQL)
  the query raises the established `NotImplementedError`, unchanged by mode

#### Scenario: Star-count counts distinct associated entities
- **WHEN** an associate-mode query rooted at `orders` selects `customers.*:count` by an
  orders-level dimension
- **THEN** each cell counts the distinct customers associated with it, by executed
  values

#### Scenario: Measure-local filter restricts the association
- **WHEN** an associate-mode aggregate carries its own column filter
- **THEN** each cell aggregates only the associated entities passing the filter, and
  result cardinality is unchanged

#### Scenario: Weighted association by executed values
- **WHEN** an associate-mode query rooted at `orders` selects
  `customers.spend:weighted_avg(weight=customers.spend)`, the custom `customers.spend:wsum`
  whose `weight` defaults to `spend`, and `customers.spend:weighted_avg(weight=customers.regions.pop)`
  by the orders-level dimension `status`
- **THEN** each executes with hand-computed per-cell values over the distinct associated
  customers on SQLite and DuckDB — the explicit and defaulted spellings identical, a
  customer with two orders in one cell weighted once — with unchanged result grain and
  sibling values

#### Scenario: Windowed or first/last combination fails closed
- **WHEN** an associate-mode query needs association for an aggregate that also
  declares `window=` or uses `first`/`last`
- **THEN** the query fails with a clear typed error naming the unsupported combination
  — never a silently wrong value

#### Scenario: Root without a unique key fails closed
- **WHEN** an associate-mode query needs association for an aggregate whose root model
  declares no primary or unique key
- **THEN** the query fails with a clear typed error naming the model and the remedy

### Requirement: Error mode refuses silent semantics
Under `to_many_handling: "error"`, every event the retired strict flag rejected SHALL
fail with a clear typed error: an implicit-grain broadcast (cross-model or local) and a
filter actually excluded from a producer (unreachable, or outside semi-join pushdown
scope). The error names the metric, the dimension or filter, and the remedy. A filter
applied by semi-join pushdown is correctly applied and MUST NOT error; explicit
`partition_by=` broadcasting of an attributable declared grain MUST NOT error, while
an unattributable explicit `partition_by=` key is a hard error under `broadcast`/`error`
and associates only under `associate` — unless the key's own dependency closure crosses
a fanning hop from its host, which is an input-safety error that fails closed in every
mode, `associate` included (see "Partition key fanning from its host fails closed in
every mode"); an ambiguous correlation hop errors in every mode and is not an error-mode
concern.

#### Scenario: Broadcast-would-happen errors
- **WHEN** an error-mode query would broadcast a metric — cross-model or local — over
  an unattributable dimension
- **THEN** the query fails with an error naming the metric, the dimension, and the
  remedy, not with wrong numbers

#### Scenario: Excluded filter errors
- **WHEN** an error-mode query has a filter conjunct excluded from a producer
- **THEN** the query fails with an error naming the filter and the remedy

#### Scenario: Pushed filter and clean query pass
- **WHEN** an error-mode query's only cross-root filter pushes down by semi-join and
  every metric is computable at the full query grain
- **THEN** the query succeeds with values identical to the broadcast-mode run

### Requirement: The strict flag is retired
`SlayerQuery` SHALL NOT accept a `strict` field. Query input containing `strict` fails
with a clear typed error naming `to_many_handling: "error"` as the replacement. Stored
queries are migrated on load: `strict: true` becomes `to_many_handling: "error"`;
`strict: false` or absent maps to the default. Every surface that exposed `strict`
(REST query request, MCP query tool) SHALL expose `to_many_handling` instead.

#### Scenario: Strict input is rejected with the remedy
- **WHEN** a query is submitted with `strict: true` on any surface
- **THEN** it fails with a clear error naming `to_many_handling: "error"` as the
  replacement

#### Scenario: Stored strict queries migrate
- **WHEN** a stored query saved with `strict: true` (and another with `strict: false`)
  is loaded after the migration
- **THEN** the first loads with `to_many_handling: "error"`, the second with the
  default, and both execute with the semantics those modes define

#### Scenario: Surfaces accept the mode parameter
- **WHEN** a query sets `to_many_handling` through the REST API or the MCP query tool
- **THEN** the mode reaches the engine and governs resolution exactly as a direct
  `SlayerQuery` field does

### Requirement: Partition key fanning from its host fails closed in every mode

An explicit `partition_by=` key whose dependency closure — its own join path plus every
path the definition of a derived column it names crosses, recursively — crosses a
fanning or unproven hop **from the aggregate's host** SHALL fail with a clear typed error
in **every** `to_many_handling` mode, `associate` included. Such a key is not
single-valued at the host grain, so it can never be counted without multiplying rows;
this is an input-safety failure (mode-invariant), distinct from a dimension merely
unattributable from a further root (which resolves per the mode axis). The error names
the fanning hop and the remedy (declare join cardinality or a covering unique key on the
target). The rule applies uniformly wherever the key appears — a partitioned measure,
filter, ORDER BY target, computed-dimension aggregate, transform partition set,
re-aggregation inner, or windowed aggregate — and to both the path-less spelling
(`partition_by=<derived host column>`) and the path-bearing spelling
(`partition_by=<dotted reference across the hop>`). A partition key that is safe from the
host but unattributable only from a further (cross-model) root is unaffected: it keeps
its mode-aware resolution.

#### Scenario: Path-less derived fanning partition key fails closed in every mode
- **WHEN** a query rooted at `regions` selects `pop:sum(partition_by=bad_pop)`, where
  `bad_pop` is the host-local derived column `pop + region_events.value` over the
  one-to-many `regions → region_events` hop, under `broadcast`, `error`, or `associate`
- **THEN** the query fails with a typed error naming `region_events` and the remedy, in
  all three modes — never the join-multiplied value

#### Scenario: Chained derived fanning partition key fails closed
- **WHEN** the partition key is a derived column defined over another derived column that
  crosses the fanning hop (e.g. `bad_pop2 = bad_pop * 2`), in any mode
- **THEN** the query fails closed naming the fanning hop, exactly as for the direct
  derived key

#### Scenario: Fanning partition key fails closed in every position
- **WHEN** the fanning derived partition key appears as a filter target, an ORDER BY
  target, a computed-dimension aggregate, a transform's partition set, a re-aggregation
  inner aggregate, or a windowed aggregate, in any mode
- **THEN** the query fails closed naming the fanning hop in each position — never a
  silently multiplied value

#### Scenario: Unanalysable derived partition key fails closed without naming a hop
- **WHEN** the partition key names a derived column whose definition no supported dialect
  can analyse for join dependencies, in any mode
- **THEN** the query fails closed with a typed error that does not falsely attribute a
  specific fanning hop it could not prove

#### Scenario: Host-safe partition key keeps its mode-aware resolution
- **WHEN** a `customers`-rooted aggregate declares `partition_by=status`, where `status`
  is a plain column on the query's host model (safe from the host but unattributable from
  the cross-model root)
- **THEN** the key is not treated as a fanning-from-host safety error: it errors under
  `broadcast`/`error` and associates under `associate`, unchanged
