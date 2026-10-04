## MODIFIED Requirements

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
- **WHEN** a query rooted at `orders` selects the local `sum(amount)` and the cross-model
  `sum(customers.spend)` by the dimension `customers.regions.bad_pop`, a derived column
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
own value; `count(*)` counts the distinct associated entities per cell. An
aggregation's own column filter restricts the associated entities before per-cell
aggregation. Association is needed only when at least one grain dimension is
unattributable from the aggregate's home: an aggregate whose grain dimensions the home
all determines takes the plain path under `associate` exactly as under `broadcast`,
its attached inputs compiled at their own homes, so the eligibility rules below apply
only when association is needed. Combining associate-mode resolution with `window=` or
`first`/`last` on the same aggregate SHALL fail with a clear typed error naming the
combination and the remedy. An aggregate root model without a declared unique key SHALL
fail associate-mode resolution with a clear typed error naming the model and the remedy
(declare a primary or unique key).

#### Scenario: Percentile attributes over the association
- **WHEN** an associate-mode query slices a cross-model percentile aggregate by an
  unattributable dimension, on a dialect that supports grouped percentile
- **THEN** each cell's value is the percentile over the distinct associated entities'
  values, by executed values; on a dialect without grouped percentile (T-SQL, MySQL)
  the query raises the established `NotImplementedError`, unchanged by mode

#### Scenario: Star-count counts distinct associated entities
- **WHEN** an associate-mode query rooted at `orders` selects `count(customers.*)` by an
  orders-level dimension
- **THEN** each cell counts the distinct customers associated with it, by executed
  values

#### Scenario: Measure-local filter restricts the association
- **WHEN** an associate-mode aggregate carries its own column filter
- **THEN** each cell aggregates only the associated entities passing the filter, and
  result cardinality is unchanged

#### Scenario: Weighted association by executed values
- **WHEN** an associate-mode query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=customers.spend)`, the custom `wsum(customers.spend)`
  whose `weight` defaults to `spend`, and `weighted_avg(customers.spend, weight=customers.regions.pop)`
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

#### Scenario: Attributable dimensions need no association
- **WHEN** an associate-mode query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=sum(amount, partition_by=customers.regions.name))`
  by `customers.tier` against a `customers` model declaring no primary or unique key,
  the `orders → customers` hop declared many-to-one
- **THEN** the query executes with the same values as under `broadcast` and no
  association warning — the home determines every dimension, so no entity
  deduplication is needed and the unique-key rule does not apply

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
- **WHEN** a query rooted at `regions` selects `sum(pop, partition_by=bad_pop)`, where
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

### Requirement: Distinct-entity association over the virtual model
Under `to_many_handling: "associate"`, an aggregate with at least one unattributable
grain dimension SHALL return, for each result cell, the aggregate over the distinct
home entities associated with that cell — each entity counted exactly once per cell,
deduplicated by the home model's unique key. Association is defined by the join path
from the aggregate's home dataset to the dimension (Axiom 3, Association): an entity
belongs to a cell iff its own path reaches the cell's dimension values, so a home entity
with no population row still counts in every cell its path reaches, and the origin of the
population's rows never restricts the association. An entity whose path reaches no
related row carries NULL for that dimension and belongs to the NULL cell, whichever
dataset roots the population — the cells are those of the aggregate's virtual model
(`queries/semantics` › Aggregates are virtual models). The result grain, row count,
sibling metrics, and other columns' values MUST be unchanged relative to the same query
without the aggregate. Entity populations of different cells may overlap; cells are
therefore not additive across the unattributable dimensions, and the response warns
accordingly. Attributable grain dimensions retain exact partition values identical to
broadcast mode.

#### Scenario: Cross-model metric attributes per cell by executed values
- **WHEN** a query rooted at `orders` with `to_many_handling: "associate"` selects
  `sum(customers.spend)` by the orders-level dimension `status`
- **THEN** each status cell equals the summed spend of the distinct customers having
  at least one order with that status — the NULL cell also holding every customer with
  no orders — by executed values, with unchanged result grain

#### Scenario: Local metric over a fanning dimension attributes per cell
- **WHEN** a query rooted at `customers` with `to_many_handling: "associate"` selects
  `sum(spend)` by `orders.status`, and one customer has two orders with the same status
- **THEN** that customer's spend counts once in that status cell — by executed values,
  never the join-multiplied figure

#### Scenario: Adding an associated measure is cardinality-neutral
- **WHEN** any supported query runs with and without an additional associate-mode
  aggregate
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Multi-hop association attributes per cell
- **WHEN** a query rooted at `orders` under `associate` selects a metric rooted two
  hops away (e.g. `sum(customers.regions.pop)`) by an orders-level dimension
- **THEN** each cell aggregates over the distinct entities of the metric's root
  associated with the cell, by executed values

#### Scenario: Home entity absent from the population counts in its home-determined cell
- **WHEN** a query rooted at `orders` under `associate` selects `sum(customers.spend)`
  and the local `sum(amount)` by a regions-level dimension the customer's own path
  determines, and one South customer has no orders
- **THEN** the South cell of `sum(customers.spend)` includes that customer's spend (every
  South customer once, on SQLite and DuckDB) while the local `sum(amount)` cell is
  unchanged, and the response carries the associated-cells warning

#### Scenario: An entity with no related row sits in the NULL cell however the query is rooted
- **WHEN** a query rooted at `orders` under `associate` selects `sum(customers.spend)` by
  `status`, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL-status cell holds both the orderless customer and the owner of the
  NULL-status order — the same value as the customers-rooted spelling, by executed
  values on SQLite and DuckDB

#### Scenario: A population rooted at the home keeps its own NULL cell
- **WHEN** a query rooted at `customers` under `associate` selects `sum(spend)` by
  `orders.status`, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL-status cell holds both the orderless customer and the owner of the
  NULL-status order, exactly the customers whose population rows carry a NULL status

#### Scenario: A derived dimension crossing back keeps the orderless entity's NULL cell
- **WHEN** a query rooted at `orders` under `associate` selects `sum(customers.spend)` by
  a customers-level derived dimension whose definition reads the customer's orders'
  status, one order carries a NULL status, and one customer has no orders
- **THEN** the NULL cell holds both the orderless customer and the owner of the
  NULL-status order, by executed values

#### Scenario: Every position reads the same associated NULL cell
- **WHEN** the orders-rooted associated aggregate above is spelled with an explicit
  `partition_by=` naming `status`, or used only in a measure filter or only as an
  order key
- **THEN** its NULL-status value equals the measure's value in every position, by
  executed values

#### Scenario: Mixed home-side and population-root dimensions
- **WHEN** a query rooted at `orders` under `associate` selects `sum(customers.spend)` by
  both a regions-level dimension and `status`, and one South customer has no orders
- **THEN** every customer with orders is counted once in each of its (region, status)
  cells, and the orderless customer's value lands only in the virtual model's
  (South, NULL-status) cell, which the population lacks, so no result row carries it
  and the row count is unchanged, by executed values
