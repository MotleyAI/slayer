# queries/attribution-modes Specification

## Purpose
Defines the query-level `to_many_handling` mode axis: how every aggregate — cross-model
and local — resolves query dimensions not attributable from its root (broadcast,
associate, or error), the association semantics and its eligibility rules, and the
retirement of the legacy `strict` flag.

## Requirements

### Requirement: Error mode refuses silent semantics
Under `to_many_handling: "error"`, every event the retired strict flag rejected SHALL
fail with a clear typed error: an implicit-grain broadcast (cross-model or local) and a
filter actually excluded from a producer (unreachable, or outside semi-join pushdown
scope). The error names the metric, the dimension or filter, and the remedy. A filter
applied by semi-join pushdown is correctly applied and MUST NOT error; explicit
`partition_by=` broadcasting of an attributable declared grain MUST NOT error, while
an unattributable explicit `partition_by=` key is a hard error under `broadcast`/`error`
and associates when the mode is omitted or `associate` — unless the key's own dependency
closure crosses a fanning hop from its host, which is an input-safety error that fails
closed in every mode, the default and `associate` included (see "Partition key fanning
from its host fails closed in every mode"); an ambiguous correlation hop errors in every
mode and is not an error-mode concern.

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
- **THEN** the query succeeds with values identical to the run omitting the mode

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
in **every** `to_many_handling` mode, the default and `associate` included. Such a key is
not single-valued at the host grain, so it can never be counted without multiplying rows;
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
  one-to-many `regions → region_events` hop, omitting the mode or under `broadcast`,
  `error`, or `associate`
- **THEN** the query fails with a typed error naming `region_events` and the remedy, in
  every mode — never the join-multiplied value

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
  `broadcast`/`error` and associates when the mode is omitted or `associate`

### Requirement: Distinct-entity association over the virtual model
When an aggregate with at least one unattributable grain dimension associates (the mode
omitted, or `to_many_handling: "associate"`), it SHALL return, for each result cell, the
aggregate over the distinct home entities associated with that cell — each entity counted
exactly once per cell, deduplicated by the home model's unique key, else by its per-query
row identity. Association is defined by the join path from the aggregate's home dataset
to the dimension (Axiom 3, Association): an entity belongs to a cell iff its own path
reaches the cell's dimension values, so a home entity with no population row still counts
in every cell its path reaches, and the origin of the population's rows never restricts
the association. An entity whose path reaches no related row carries NULL for that
dimension and belongs to the NULL cell, whichever dataset roots the population — the
cells are those of the aggregate's virtual model (`queries/semantics` › Aggregates are
virtual models), exactly the dimension tuples its home rows reach, including tuples the
population lacks. The result grain, row count, sibling metrics, and other columns'
values MUST be unchanged relative to the same query without the aggregate. Entity
populations of different cells may overlap; cells are therefore not additive across the
unattributable dimensions, and the response warns accordingly. Attributable grain
dimensions retain exact partition values identical to broadcast mode.

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
- **WHEN** any supported query runs with and without an additional associated aggregate,
  omitting the mode and again under `"associate"`
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

#### Scenario: An associated producer supplies the tuples its home rows reach
- **WHEN** a query rooted at `orders` omitting the mode selects `count(customers.*)` by
  `status`, and one customer has no orders
- **THEN** the associated producer's cells are exactly the status values the customers'
  own paths reach, the orderless customer's NULL cell included, while the result keeps
  only the population's status rows

### Requirement: Mode selection and the association default
A query SHALL accept `to_many_handling` with values `"broadcast"`, `"associate"`, and
`"error"`, or omit it (`null`), which selects the default. The default SHALL resolve as
`"associate"`, except that an aggregate that needs association but is not
association-eligible broadcasts (per "Broadcast fallback for association-ineligible
aggregates"). The mode governs, uniformly for cross-model and local aggregates alike and
in every consumer context (measure, composite leaf, filter, ORDER BY, computed dimension,
nested producer), how each pair of (aggregate, query dimension unattributable from the
aggregate's home) resolves. A dimension is attributable from a home iff every path in
its dependency closure — its own join path and every path the definition of a derived
column it names crosses, recursively — is provably many-to-one from that home; a
fanning hop and an unproven hop (no declared to-one cardinality and no covering unique
key) make it unattributable alike, and a dimension naming a derived column whose
definition crosses such a hop is unattributable exactly as a structural one is. The
mode MUST NOT affect aggregates all of whose grain dimensions are attributable,
dimension-free totals, or filter pushdown (semi-join pushdown applies in every mode). An
unrecognized value SHALL fail with a clear validation error. A refinement leaving the
mode unset inherits the refined query's mode.

#### Scenario: The default associates
- **WHEN** a query omitting `to_many_handling` slices a cross-model metric by an
  unattributable dimension
- **THEN** each cell carries the per-cell value over the distinct associated home
  entities — identical to the same query under `"associate"` — and the response carries
  the associated-cells warning and no broadcast warning

#### Scenario: Explicit broadcast keeps the pre-change behaviour
- **WHEN** the same query sets `to_many_handling: "broadcast"`
- **THEN** generated SQL and executed values are identical to the pre-change broadcast
  behaviour, and the response carries the broadcast warning

#### Scenario: An unproven hop associates by default
- **WHEN** `customers` declares no primary or unique key, the `orders → customers` join
  declares no cardinality, and a query omitting `to_many_handling` selects
  `sum(orders.amount)` by `customers.region` over Ossie Appendix B.1 data (EAST C1, C2;
  WEST C3; NORTH C4; orders O1 C1 100, O2 C1 200, O3 C2 50, O4 C3 300)
- **THEN** the root-less query returns EAST 350, WEST 300 and NORTH NULL, the query
  rooted at `orders` returns EAST 350 and WEST 300, and explicit `"broadcast"` returns
  650 on every region with the broadcast warning, by executed values on SQLite and DuckDB

#### Scenario: Fully attributable queries are mode-invariant
- **WHEN** the same query whose metrics are all computable at the full query grain runs
  once omitting the mode and once per mode value
- **THEN** all four runs return identical executed values with no mode-related
  warnings or errors, and every cell partitions the home rows and sums to the total

#### Scenario: Unrecognized mode value fails
- **WHEN** a query sets `to_many_handling` to a value outside the three modes
- **THEN** the query fails with a clear validation error naming the accepted values

#### Scenario: Derived fanning dimension associates by default
- **WHEN** a query rooted at `orders` selects the local `sum(amount)` and the cross-model
  `sum(customers.spend)` by the dimension `customers.regions.bad_pop`, a derived column
  defined as `pop + region_events.value` over the one-to-many `regions → region_events`
  hop, omitting `to_many_handling` (and again with `"associate"`)
- **THEN** each cell aggregates over the distinct root entities associated with that
  dimension value — each order once for the local metric, each customer once for the
  cross-model one — identical in both runs, by hand-computed executed values on SQLite
  and DuckDB

#### Scenario: Derived fanning dimension broadcasts under explicit broadcast
- **WHEN** the same query runs with `to_many_handling: "broadcast"`
- **THEN** each metric is broadcast across that dimension with a warning naming the hop,
  by executed values — never the figure multiplied once per region event

#### Scenario: Derived fanning dimension refuses under error mode
- **WHEN** the same query runs with `to_many_handling: "error"`
- **THEN** the query fails with the error-mode broadcast refusal naming the metric and the
  dimension

### Requirement: Association eligibility, input handling and row identity
Association SHALL support the full plain scalar aggregation family (including count,
count_distinct, avg, min, max, median, percentile, and stddev-class aggregations),
subject to each dialect's existing aggregate capability — grouped `median`/`percentile`
remains a `NotImplementedError` on T-SQL and MySQL, unchanged by mode (per the
divergence ledger). Every input expression of the aggregation — arguments and
aggregation-parameter fragments alike — is evaluated per associated entity (constant per
entity under the established unsafe-aggregate-inputs rule, which keeps applying
unchanged in every mode); a column-reference or aggregate-valued parameter — explicit,
positional, or supplied by the aggregation definition's default — is legal exactly when
the entity grain determines it (per `queries/semantics` › Aggregation parameters are
typed by the home dataset's grain) and is then picked once per associated entity
alongside the aggregate's own value; `count(*)` counts the distinct associated entities
per cell. An aggregation's own column filter restricts the associated entities before
per-cell aggregation. Association is needed only when at least one grain dimension is
unattributable from the aggregate's home: an aggregate whose grain dimensions the home
all determines takes the plain path in every mode, its attached inputs compiled at
their own homes, so eligibility applies only when association is needed. An aggregate
is **association-eligible** unless it declares `window=`, uses `first`/`last`, or has a
row-valued parameter the entity grain does not determine. An ineligible aggregate that
needs association SHALL fail under `to_many_handling: "associate"` with a clear typed
error naming the combination and the remedy, and SHALL broadcast with the reason when
the mode is omitted (per "Broadcast fallback for association-ineligible aggregates").
A home declaring no primary or unique key SHALL be associated through a per-query row
identity (per "Per-query row identity for keyless homes"), never refused.

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

#### Scenario: Windowed or first/last combination fails closed under explicit associate
- **WHEN** a query with `to_many_handling: "associate"` needs association for an
  aggregate that also declares `window=` or uses `first`/`last`
- **THEN** the query fails with a clear typed error naming the unsupported combination
  — never a silently wrong value

#### Scenario: A keyless home associates
- **WHEN** a query rooted at `orders`, omitting the mode and again under `"associate"`,
  selects `sum(customers.spend)` and the custom `wsum(customers.spend)` (whose `weight`
  defaults to `spend`) by `status`, against a `customers` model declaring no primary or
  unique key
- **THEN** both execute with the same per-cell values as against the keyed `customers`
  model, by executed values on SQLite and DuckDB

#### Scenario: Attributable dimensions need no association
- **WHEN** an associate-mode query rooted at `orders` selects
  `weighted_avg(customers.spend, weight=sum(amount, partition_by=customers.regions.name))`
  by `customers.tier` against a `customers` model declaring no primary or unique key,
  the `orders → customers` hop declared many-to-one
- **THEN** the query executes with the same values as under `broadcast` and no
  association warning — the home determines every dimension, so no entity
  deduplication is needed

### Requirement: Broadcast fallback for association-ineligible aggregates
When `to_many_handling` is omitted, an aggregate that needs association but is not
association-eligible (per "Association eligibility, input handling and row identity")
SHALL broadcast across its unattributable dimensions — its value repeats across their
cells — and the query SHALL NOT fail. The response SHALL carry a broadcast warning naming
the aggregate, the dimensions, and the reason association is unsupported, with a hint
naming the remedy (drop the `window=` / `first`/`last`, or make the dimension
attributable) instead of the dice–slice hint; one measure whose constituents fall back
for different reasons reports each reason. Under explicit `"associate"` the same
aggregate fails with its typed error, under `"broadcast"` it broadcasts with the ordinary
warning, and under `"error"` it is refused. The fallback never changes the result rows or
any other column's values. The fallback covers only the query's implicit grain: an
ineligible aggregate whose explicit `partition_by=` names a key it would have to
associate fails when the mode is omitted with the same typed error as under
`"associate"`, since requested grain is never broadcast. Input-safety failures are not
ineligibility: an input that crosses a fanning or unproven hop from its home, or a
partition key fanning from its host, fails in every mode, the default included.

#### Scenario: first/last broadcasts with its reason by default
- **WHEN** a query rooted at `orders` omitting the mode selects
  `last(customers.spend, customers.signup_at)` by `status`
- **THEN** the value repeats on every status row, and the response carries a broadcast
  warning naming the measure, `status`, the first/last reason and the fallback remedy

#### Scenario: window= broadcasts with its reason by default
- **WHEN** a query rooted at `orders` omitting the mode selects
  `sum(customers.spend, window='90d')` by `status` and a month of `customers.signup_at`
- **THEN** each month's value repeats across the statuses of that month, and the
  response carries a broadcast warning naming the window reason and the fallback remedy

#### Scenario: Each fallback reason is reported
- **WHEN** a query omitting the mode selects one measure combining a `first`/`last` and a
  `window=` aggregate that both need association
- **THEN** the response reports both reasons, each with the fallback remedy

#### Scenario: Explicit modes keep their behaviour for ineligible aggregates
- **WHEN** the queries above run under `"associate"`, `"broadcast"` and `"error"`
- **THEN** `"associate"` fails with the typed unsupported-combination error,
  `"broadcast"` broadcasts with the ordinary broadcast warning, and `"error"` refuses

#### Scenario: Requested grain is never broadcast by the fallback
- **WHEN** a query rooted at `orders` omitting the mode selects
  `last(customers.spend, customers.signup_at, partition_by=status)` by `status`
- **THEN** the query fails with the typed unsupported-combination error, exactly as
  under `"associate"`

#### Scenario: Input safety is not a fallback
- **WHEN** a query omitting the mode aggregates a `customers` column whose filter reads
  `orders.status` across the fanning `customers → orders` hop
- **THEN** the query fails with the input-safety error naming the hop, exactly as under
  every explicit mode

### Requirement: Per-query row identity for keyless homes
A home dataset declaring no primary or unique key that needs association SHALL be
deduplicated by a per-query row identity: each of its rows — after its model-level
filters, before any join — is told apart from every other, so each physical row counts
exactly once per cell. Two identical physical rows count as two rows; the copies a join
fan-out makes of one row count once. The identity is query-local, determines every
column of its own row, and never appears in results. It applies in every mode that
associates; an association over list children only keeps its key-free evaluation.

#### Scenario: Duplicate physical rows count separately, fan-out copies once
- **WHEN** a keyless `customers` model holds one customer's row twice, another customer
  has two `completed` orders, and a query rooted at `orders` omitting the mode selects
  `count(customers.*)` by `status`
- **THEN** each physical customer row counts once per status cell — the duplicated
  customer twice, the two-order customer once — by executed values on SQLite and DuckDB

#### Scenario: Fan-out under a joined filter still counts each row once
- **WHEN** a keyless home row reaches several rows of a joined dataset that a query
  filter reads, and the query associates an aggregate of that home by a dimension across
  the fanning hop
- **THEN** that home row counts once per cell it reaches, by executed values

#### Scenario: The row identity renders on every dialect
- **WHEN** a keyless-home association query is rendered for every supported dialect
- **THEN** each statement numbers the home rows with the dialect's accepted window
  syntax, executed on the in-process and locally runnable engines

### Requirement: Stored broadcast defaults migrate
A stored query saved with `to_many_handling: "broadcast"` — standalone, or a stage nested
in a stored model, with or without its own version — SHALL load with the mode omitted,
taking the default; stored `"associate"` and `"error"` SHALL load unchanged. A query
submitted fresh with `"broadcast"` SHALL keep it, and a query saved with the mode omitted
SHALL store no mode.

#### Scenario: A stored broadcast loads as the default
- **WHEN** a stored query of the previous version carrying `"broadcast"` is loaded, from
  YAML and from SQLite storage
- **THEN** it loads with `to_many_handling` omitted, and stored `"associate"` / `"error"`
  queries load unchanged

#### Scenario: Nested stages migrate
- **WHEN** a stored model's stages carry `"broadcast"`, on the final and a non-final
  stage, with and without their own version
- **THEN** every stage loads with the mode omitted

#### Scenario: Fresh explicit broadcast is kept
- **WHEN** a query is submitted with `to_many_handling: "broadcast"` and saved
- **THEN** it executes and reloads as `"broadcast"`

### Requirement: Ungrained re-aggregation inners resolve per the mode
A re-aggregation whose inner aggregate takes the query grain (no explicit
`partition_by=`) and needs association SHALL resolve that inner per the query's mode
exactly as the same aggregate standing alone: associated when the mode is omitted or
`"associate"`, broadcast under `"broadcast"`, refused under `"error"`; an
association-ineligible inner falls back or fails per "Broadcast fallback for
association-ineligible aggregates".

#### Scenario: A plain ungrained inner associates
- **WHEN** a query rooted at `orders` selects `sum(sum(customers.spend))` by `status`,
  omitting the mode and again under `"associate"`
- **THEN** each status cell equals the associated cell of the bare `sum(customers.spend)`
  (new 290, ok 420 on the reference data), by executed values on SQLite and DuckDB

#### Scenario: A ranked ungrained inner falls back or fails
- **WHEN** the query selects `sum(last(customers.spend, customers.signup_at))` by `status`
- **THEN** omitting the mode broadcasts it with the first/last reason, and `"associate"`
  fails with the typed unsupported-combination error
