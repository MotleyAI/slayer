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
dimension unattributable from the aggregate's root) resolves. The mode MUST NOT affect
aggregates all of whose grain dimensions are attributable, dimension-free totals, or
filter pushdown (semi-join pushdown applies in every mode). An unrecognized value SHALL
fail with a clear validation error.

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

### Requirement: Distinct-entity association semantics
Under `to_many_handling: "associate"`, an aggregate with at least one unattributable
grain dimension SHALL return, for each result cell, the aggregate over the distinct
root entities associated with that cell's population rows — each entity counted exactly
once per cell, deduplicated by the root model's unique key. The result grain, row
count, sibling metrics, and other columns' values MUST be unchanged relative to the
same query without the aggregate. Entity populations of different cells may overlap;
cells are therefore not additive across the unattributable dimensions, and the
response warns accordingly. Attributable grain dimensions retain exact partition
values identical to broadcast mode.

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

### Requirement: Association eligibility and input handling
Associate-mode resolution SHALL support the full plain scalar aggregation family
(including count, count_distinct, avg, min, max, median, percentile, and stddev-class
aggregations), subject to each dialect's existing aggregate capability — grouped
`median`/`percentile` remains a `NotImplementedError` on T-SQL and MySQL, unchanged by
mode (per the divergence ledger). Every input expression of the aggregation — arguments and
aggregation-parameter fragments alike — is evaluated per associated entity (constant
per entity under the established unsafe-aggregate-inputs rule, which keeps applying
unchanged); `*:count` counts the distinct associated entities per cell. An
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
(it associates only under `associate`); an ambiguous correlation hop errors in
every mode and is not an error-mode concern.

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
