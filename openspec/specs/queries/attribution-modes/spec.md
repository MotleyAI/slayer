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
closure crosses a fanning hop from the key's anchor, which is an input-safety error that
fails closed in every mode, the default and `associate` included (see "Partition key
fanning from its anchor fails closed in every mode"); an ambiguous correlation hop errors in
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
- **THEN** the query succeeds with values identical to the run omitting the mode

### Requirement: Partition key fanning from its anchor fails closed in every mode

An explicit `partition_by=` key whose dependency closure — its own join path plus every
path the definition of a derived column it names crosses, recursively — crosses a
fanning or unproven hop **from the key's anchor** (`queries/references` › A query reference
is anchored at its first segment; a converted pre-2.0 bare key is anchored at the population)
SHALL fail with a clear typed error in **every** `to_many_handling` mode, the default and
`associate` included. Such a key is not single-valued at its anchor's grain, so it can never
be counted without multiplying rows; this is an input-safety failure (mode-invariant),
distinct from a key merely unattributable from the aggregate's home (which resolves per the
mode axis). The error names
the fanning hop and the remedy (declare join cardinality or a covering unique key on the
target). The rule applies uniformly wherever the key appears — a partitioned measure,
filter, ORDER BY target, computed-dimension aggregate, transform partition set,
re-aggregation inner, or windowed aggregate — and to both the path-less spelling
(`partition_by=<derived field of the anchor>`) and the path-bearing spelling
(`partition_by=<dotted reference across the hop>`). A partition key that is safe from its
anchor but unattributable from the aggregate's home is unaffected: it keeps its mode-aware
resolution.

#### Scenario: Path-less derived fanning partition key fails closed in every mode
- **WHEN** a query rooted at `regions` selects `sum(pop, partition_by=bad_pop)`, where
  `bad_pop` is the `regions` derived column `pop + region_events.value` over the
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
- **WHEN** a `customers`-homed aggregate in a query with `population: "orders"` declares
  `partition_by=status`, the plain column `orders.status` (safe from its anchor `orders`, but
  unattributable from the aggregate's home `customers`)
- **THEN** the key is not treated as an input-safety error: it errors under
  `broadcast`/`error` and associates when the mode is omitted or `associate`

#### Scenario: A key reached from the population across a fanning hop but local to its anchor is safe
- **WHEN** a query with `population: "customers"` groups by `orders.status` and selects
  `sum(orders.amount, partition_by=[orders.status])`, in any mode
- **THEN** it executes: the key `orders.status` crosses no hop from its anchor `orders`, which is
  also the aggregate's home, so no input-safety error is raised, by executed values

### Requirement: Distinct-entity association over the virtual model
When an aggregate with at least one unattributable grain dimension associates (the mode
omitted, or `to_many_handling: "associate"`), it SHALL return, for each result cell, the
aggregate over the distinct home entities associated with that cell — each entity counted
exactly once per cell, deduplicated by the home model's primary key, else by its per-query
row identity (per "Per-query row identity for keyless homes"). Association is defined by the join path from the aggregate's home dataset
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
`"error"`, or omit it (`null`), which selects the default. The default SHALL resolve
exactly as `"associate"`. The mode governs, uniformly for cross-model and local
aggregates alike, for every aggregation (plain, `first`/`last`, windowed, windowed
`first`/`last`), and in every consumer context (measure, composite leaf, filter, ORDER BY,
computed dimension, nested producer), how each pair of (aggregate, query dimension unattributable from the
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
  warnings or errors, every cell partitions the home rows, and an additive aggregate's
  cells sum to its total

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
their own homes. Every aggregation is **association-eligible** — `first`/`last`,
windowed and windowed `first`/`last` aggregations included (per "Ranked and windowed
association"); a parameter the entity grain does not determine is a typed parameter-grain
error, never a reason to resolve the aggregate any other way. For an association over
list children only, each associated row is one root row, so the entity grain is the root
row and a parameter the root row determines is determined. A home declaring no primary key SHALL be associated through a per-query row
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

#### Scenario: Windowed or first/last combination executes under explicit associate
- **WHEN** a query with `to_many_handling: "associate"` needs association for an
  aggregate that also declares `window=` or uses `first`/`last`
- **THEN** the combination no longer fails: the query executes with the per-cell values
  of "Ranked and windowed association", by executed values — never a broadcast or a
  silently wrong value

#### Scenario: A list-child association reads a root-determined parameter
- **WHEN** a query selects `weighted_avg(contractors.rate, weight=contractors.rate)` by
  `contractors.skills.value`, omitting the mode and again under `"associate"`
- **THEN** each skill cell is the weighted average over the distinct contractors listing
  that skill (go 43.33, sql 16.67 on the reference data), by executed values on SQLite
  and DuckDB, with the associated-cells warning — never a broadcast or a parameter-grain
  error

#### Scenario: Every parameter shape associates or fails with its own typed error
- **WHEN** an associated aggregate takes a field-valued custom parameter, a definition
  default, an attached aggregate- or transform-valued parameter, a parameter over a
  provably to-one path, or a parameter across a fanning hop, against keyed and keyless
  homes, in every mode
- **THEN** each executes with hand-computed per-entity values, or fails before SQL
  generation with its established typed error (input safety across the fanning hop in
  every mode) — never a broadcast fallback

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

### Requirement: Per-query row identity for keyless homes
A home dataset whose model declares no primary key that needs association SHALL be
deduplicated by a per-query row identity, including when the model declares a standalone
unique column that may allow NULLs: each of its rows — after its model-level
filters, before any join — is told apart from every other, so each physical row counts
exactly once per cell. Two identical physical rows count as two rows; the copies a join
fan-out makes of one row count once. The identity is query-local, determines every
column of its own row, and never appears in results. It applies in every mode that
associates. A home whose model declares a primary key SHALL be deduplicated by that key,
whatever unique columns it also declares. An association over list children only keeps
its key-free evaluation.

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

#### Scenario: Model-level filters apply before numbering
- **WHEN** a keyless home model carries a model-level filter and the query associates an
  aggregate of that home across a fanning hop
- **THEN** only the rows passing the filter contribute, each once per cell it reaches,
  by executed values

#### Scenario: The row identity renders on every dialect
- **WHEN** a keyless-home association query is rendered for every supported dialect
- **THEN** each statement numbers the home rows with the dialect's accepted window
  syntax, executed on the in-process and locally runnable engines

#### Scenario: A nullable unique column preserves each associated row
- **WHEN** an aggregate associates over a fanning dimension, the home model has no primary
  key, and two real home rows have NULL in a standalone unique column
- **THEN** both home rows contribute once per cell they reach, even when a home row
  matches multiple joined rows in the cell, on SQLite and DuckDB

#### Scenario: A numbered home determines row-valued parameters
- **WHEN** the same home supplies both the value and weight for an associated weighted
  average
- **THEN** each cell's average uses the distinct home rows that reach it and their own
  weights, in default and explicit association modes

#### Scenario: A home with a primary key associates by it
- **WHEN** an aggregate associates over a home declaring a primary key and a standalone
  unique column holding NULLs
- **THEN** the home rows are deduplicated by the primary key without numbering, and
  every row with a NULL unique value still contributes, by executed values on SQLite and
  DuckDB

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
`"associate"`, broadcast under `"broadcast"`, refused under `"error"` — a `first`/`last`
or windowed inner included.

#### Scenario: A plain ungrained inner associates
- **WHEN** a query rooted at `orders` selects `sum(sum(customers.spend))` by `status`,
  omitting the mode and again under `"associate"`
- **THEN** each status cell equals the associated cell of the bare `sum(customers.spend)`
  (new 290, ok 420 on the reference data), by executed values on SQLite and DuckDB

#### Scenario: A ranked ungrained inner falls back or fails
- **WHEN** the query selects `sum(last(customers.spend, customers.signup_at))` by `status`,
  omitting the mode and again under `"associate"`
- **THEN** it neither falls back nor fails: each status cell equals the associated `last`
  of that cell (new 40, ok 30 on the reference data), by executed values on SQLite and
  DuckDB; under `"broadcast"` the inner broadcasts and under `"error"` the query refuses

### Requirement: The strict flag is a deprecated alias

`SlayerQuery` SHALL NOT have a `strict` field. Fresh query input carrying `strict` — no `version`, a version of 4 or later, or a version that is not an integer — SHALL be converted with a deprecation scheduled for removal in 3.0, once per query object: `strict: true` or a legacy true token becomes `to_many_handling: "error"`, and `strict: false` or a legacy false token is dropped; an explicit `to_many_handling` on the same object wins. An unrecognized `strict` token SHALL fail with a typed error naming `to_many_handling`. Stored queries saved before version 4 SHALL migrate silently by the same rules. The conversion SHALL apply to Python query objects, the REST query request, the MCP `query` tool, refinements of a saved query, and the queries of model and memory saves; a run-by-name REST request with a top-level `strict` SHALL stay rejected. Every surface SHALL expose `to_many_handling`, and none SHALL document `strict` outside the migration guide.

#### Scenario: Strict input converts with a deprecation
- **WHEN** a fresh query is submitted with `strict: true` through Python, REST and the MCP `query` tool
- **THEN** each runs under `to_many_handling: "error"` and reports one `deprecated_input` warning naming `to_many_handling`

#### Scenario: Strict false is dropped with a deprecation
- **WHEN** a fresh query carries `strict: false`, or the legacy token `"off"`
- **THEN** it runs under the default mode and reports one `deprecated_input` warning

#### Scenario: An explicit mode wins over strict
- **WHEN** a fresh query carries `strict: true` and `to_many_handling: "associate"`
- **THEN** it runs under `associate` and reports one `deprecated_input` warning

#### Scenario: An unrecognized strict token fails
- **WHEN** a fresh query, or a stored pre-version-4 query, carries `strict: "maybe"`
- **THEN** it fails with a typed error naming `to_many_handling`

#### Scenario: A malformed version is fresh input
- **WHEN** a query carrying `strict: true` has `version: 3.9`
- **THEN** it converts with a deprecation, exactly as a version-less query does

#### Scenario: Stored strict queries migrate
- **WHEN** a stored query saved with `strict: true` (and another with `strict: false`) before version 4 is loaded
- **THEN** the first loads with `to_many_handling: "error"`, the second with the default, both execute with the semantics those modes define, and neither emits a deprecation

#### Scenario: Run-by-name top-level strict stays rejected
- **WHEN** a REST run-by-name request carries a top-level `strict`
- **THEN** it is rejected like every other top-level query clause there

#### Scenario: Surfaces accept the mode parameter
- **WHEN** a query sets `to_many_handling` through the REST API or the MCP query tool
- **THEN** the mode reaches the engine and governs resolution exactly as a direct `SlayerQuery` field does

### Requirement: Ranked and windowed association
An aggregate that associates SHALL first take each cell's distinct associated home
entities — each counted once — and then apply its own reduction over them, whatever the
reduction:

- **`first` / `last`**: the cell's value is the value of the associated entity whose
  ranking key is lowest / highest. The ranking key SHALL be determined by the home (per
  "Unsafe aggregate inputs fail closed" in `queries/cross-model-aggregates`, unchanged in
  every mode); an entity with a NULL ranking key is never picked, a cell whose associated
  entities all have NULL keys is NULL, and ties are broken arbitrarily by the database.
- **`window=`**: a home entity belongs to the cell of bucket *t* iff one of its associated
  rows carrying the cell's other dimension values has its time in *t*'s trailing frame —
  the home row itself when the time axis is attributable from the home, the related rows
  reached over the home's join path when it is not. The entity counts once in that cell,
  however many of its rows fall in the frame, so cells are not additive across buckets. A
  population filter conjunct on the time axis's branch SHALL be satisfied by the same
  related row that places the entity in the frame. The buckets are the population's cells;
  a bucket whose frame holds no associated row — the NULL bucket included — takes the
  aggregation's empty value.
- **windowed `first` / `last`**: the pick runs over the entities in the cell's frame.

Every such aggregate SHALL carry the associated-cells warning when its grain is implicit,
naming each unattributable dimension — the time dimension included when the axis fans —
and none for an explicit `partition_by=` grain. Under `"broadcast"` and `"error"` it
broadcasts or refuses exactly as a plain aggregate does, except that a windowed aggregate
whose time axis is not attributable from its home SHALL fail in both modes with the typed
time-axis error naming the time dimension and naming association (omitting
`to_many_handling`) among the remedies.

#### Scenario: Cross-model first/last associates per cell
- **WHEN** a query rooted at `orders` selects `last(customers.spend, customers.signup_at)`
  and `first(customers.spend, customers.signup_at)` by `status`, omitting the mode and
  again under `"associate"`
- **THEN** `last` is new 40, ok 30 and `first` is new 100, ok 100 on the reference data —
  the pick among the distinct customers having an order of that status — by executed
  values on SQLite and DuckDB, with the associated-cells warning

#### Scenario: Unequal fan-out never changes the pick
- **WHEN** the associated `last` runs where the entity with the highest ranking key has
  one related row in the cell and other entities have several, some entities have a NULL
  ranking key, and the home is keyless
- **THEN** each cell picks the value of the highest non-NULL key among its distinct
  entities, identical for the keyed and the keyless home, by executed values

#### Scenario: Explicit partition key associates a ranked aggregate
- **WHEN** a query rooted at `orders` selects
  `last(customers.spend, customers.signup_at, partition_by=status)` by `status`, omitting
  the mode and again under `"associate"`
- **THEN** it returns new 40, ok 30 with no warning; under `"broadcast"` and `"error"` it
  fails with the unattributable-partition-key error

#### Scenario: Window over a fanning time axis associates
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` by the month of
  `orders.ordered_at`, and another rooted at `orders` selects
  `sum(customers.spend, window='1y')` by the month of `ordered_at`, omitting the mode and
  again under `"associate"`
- **THEN** both return Jan 100, Feb 250, Mar 350, Apr 460 on the reference data — each
  customer counted once in every month whose trailing year holds one of its orders — and
  the customers-rooted NULL bucket (the customer with no orders) is NULL, by executed
  values on SQLite and DuckDB, with the associated-cells warning naming the time dimension

#### Scenario: A windowed count counts distinct entities in the frame
- **WHEN** a query rooted at `orders` selects `count(customers.*, window='1y')` by the month
  of `ordered_at`, and a query rooted at `customers` selects `count(customers.*,
  window='1y')` by the month of `orders.ordered_at`
- **THEN** the months are 1, 2, 4, 6 — the active customers in each trailing year — and the
  customers-rooted NULL bucket is 0, never NULL

#### Scenario: Empty frames take each aggregation's empty value
- **WHEN** associated windowed `count(*)`, `count(<field>)`, `sum`, a custom aggregation and
  `collect` are read at a bucket whose frame holds no associated row, including the NULL
  bucket and a bucket whose source rows a filter removed
- **THEN** the counts are 0, `collect` is the empty list, and the others are NULL

#### Scenario: The axis filter binds to the in-frame related row
- **WHEN** a customer has an in-frame order whose status is not `ok` and an `ok` order
  outside the frame but no in-frame `ok` order, and the fanning-axis window queries above
  filter `orders.status = 'ok'`
- **THEN** that customer is absent from the bucket, by executed values on SQLite and
  DuckDB, in both the customers-rooted and the orders-rooted spelling (on the reference
  data without that customer: Jan 100, Feb 250, Mar 310, Apr 420)

#### Scenario: Window over an attributable axis associates the other dimensions
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` by
  `orders.status` and the month of `signup_at`, omitting the mode and again under
  `"associate"`
- **THEN** each cell sums the distinct customers having an order of that status and a
  signup in the trailing year — new 100, 250, 290; ok 100, 250, 310, 420; the NULL status
  in April 55 on the reference data — never a customer counted once per order

#### Scenario: Windowed first/last picks among the in-frame entities
- **WHEN** a query rooted at `customers` selects `last(spend, signup_at, window='1y')` by
  the month of `orders.ordered_at`
- **THEN** the months are 100, 150, 40, 30 on the reference data, by executed values; the
  same measure spelled `last(spend, window='1y')` — whose implicit ranking key is the
  fanning time dimension — fails in every mode with the input-safety error naming it

#### Scenario: A fanning time axis is refused outside association
- **WHEN** the fanning-axis window queries above run under `"broadcast"` and under
  `"error"`
- **THEN** each fails at plan time with the typed time-axis error naming the time
  dimension and the remedies, association among them, containing no issue reference

#### Scenario: Every position reads the associated ranked and windowed value
- **WHEN** an associated `last` and an associated window are used as a measure, in a
  measure filter, as an order key, and spelled with an explicit `partition_by=` naming the
  unattributable dimension
- **THEN** each position reads the same per-cell value, adding them never changes the
  result rows or any other column, and filtering to `d = v` equals the `v` cell of the
  grouped query, by executed values

#### Scenario: The windowed association renders on every dialect
- **WHEN** a ranked and a windowed association query are rendered for every supported
  dialect
- **THEN** each statement renders with the dialect's accepted syntax, and executes on the
  in-process and locally runnable engines

### Requirement: Local first/last and windowed aggregates follow the mode axis
A first/last or windowed aggregate homed at the query population SHALL resolve a query
dimension it does not attribute exactly as every other aggregate does: associated when the
mode is omitted or `"associate"`, broadcast with the ordinary warning under
`"broadcast"`, refused under `"error"`. It SHALL never be evaluated over the rows a fanning
join multiplies.

#### Scenario: Local first/last over a fanning dimension follows the mode
- **WHEN** a query rooted at `customers` selects `last(spend, signup_at)` by
  `orders.status`, omitting the mode, and under each explicit mode
- **THEN** omitting the mode and `"associate"` return new 40, ok 30, NULL 55 with the
  associated-cells warning; `"broadcast"` returns 55 on every status with the broadcast
  warning; `"error"` refuses naming the metric and `orders.status`

#### Scenario: A local window over a fanning dimension never double-counts
- **WHEN** a query rooted at `customers` selects `sum(spend, window='1y')` by
  `orders.status` and the month of `signup_at`, under each explicit mode
- **THEN** `"broadcast"` returns each month's trailing total on every status (Jan 100,
  Feb 250, Mar 350, Apr 515 on the reference data) with the broadcast warning, and
  `"error"` refuses — never the multiplied (ok, Jan) 200

### Requirement: Broadcast is a deprecated mode
`to_many_handling: "broadcast"` SHALL stay accepted and honoured throughout 2.x — generated SQL, executed values
and the `broadcast` warning unchanged — and SHALL be reported as deprecated, scheduled for removal in 3.0, through
the shared marker (`compatibility/removal-tripwire`). Its replacement is an explicit `partition_by=` on each
aggregate naming the dimensions that aggregate attributes, which returns the same values without a warning
(Axiom 10).

- **Run reporting.** Every query run — executed or dry run, in process, through REST, the MCP `query` tool or the
  HTTP client — SHALL carry exactly one `deprecated_input` warning (`old` naming `to_many_handling: "broadcast"`,
  `new` naming an explicit `partition_by=` on each aggregate, `removal` 3.0) per stage of its input whose
  effective mode is `"broadcast"`, whether that stage was submitted fresh or loaded from storage, emitted or
  pruned. A stage's effective mode is its own value, or the saved value a refinement leaving the mode unset
  inherits. Building one query and running it several times reports it on every run, never twice in one run.
- **Python warning.** Fresh input — a Python query object, a REST body, an MCP query object or a refinement,
  given without a `version` and outside a stored document — SHALL emit exactly one `FutureWarning` per query
  object at construction, attributed to the first line outside SLayer that supplied it. Loading a stored document
  SHALL emit none.
- **Saves.** A save whose query or stages carry `"broadcast"` SHALL report the `deprecated_input` payload on that
  save's response (a structured entry on REST model saves, its message on MCP model saves and memory saves), and
  SHALL persist `"broadcast"` as given.
- **Concrete replacement.** Each `broadcast` warning SHALL carry `partition_by`: the query dimensions its
  aggregate attributes, spelled as the query spells them — a time dimension as its field, `[]` for the grand
  total — and its hint SHALL name the aggregate call in canonical form, its references spelled as the query
  spells them (a leaf of a composite metric by
  its own call, never an internal label) and the `partition_by=[…]` returning the same values, keeping the
  dice–slice disclosure.
- **Not offered.** The published query and refinement schemas SHALL enumerate only `associate` and `error`, and
  an unrecognized mode value's validation error SHALL name only those two; validation SHALL still accept
  `"broadcast"`.

Omitting the mode, `"associate"` and `"error"` SHALL report no deprecation.

#### Scenario: Fresh broadcast reports one deprecation and keeps its values
- **WHEN** a query rooted at `orders` with `to_many_handling: "broadcast"` selects `sum(customers.spend)` by
  `orders.status`, through Python, REST and the MCP `query` tool
- **THEN** each response carries the same values, SQL and `broadcast` warning as before the change, plus exactly
  one `deprecated_input` warning naming `to_many_handling: "broadcast"`, `partition_by=` and 3.0, and the Python
  run records exactly one `FutureWarning`

#### Scenario: Every run reports the deprecation once
- **WHEN** a broadcast query object is built once and run three times
- **THEN** exactly one `FutureWarning` is recorded, at construction, and each of the three responses carries
  exactly one `deprecated_input` warning

#### Scenario: Each broadcast stage reports its own deprecation
- **WHEN** a multi-stage query whose two named stages set `"broadcast"` and whose root stage omits the mode is
  run, and again with one of those stages pruned from the emitted SQL
- **THEN** each response carries exactly two `deprecated_input` warnings

#### Scenario: A broadcast stage that never broadcasts still reports
- **WHEN** a `"broadcast"` query has only attributable dimensions, or no dimensions, and runs executed, as a dry
  run and with `explain`
- **THEN** each response carries exactly one `deprecated_input` warning and no `broadcast` warning

#### Scenario: A stored broadcast stage reports on every run
- **WHEN** a query-backed model whose source query stores `"broadcast"` at the current version is loaded and
  queried twice, and a saved query storing `"broadcast"` is run by name
- **THEN** loading records no `FutureWarning`, and every response carries exactly one `deprecated_input` warning
  per broadcast stage with unchanged values

#### Scenario: Refinements report the effective mode
- **WHEN** a saved query omitting the mode runs with `refine={"to_many_handling": "broadcast"}`, a saved
  `"broadcast"` query runs with a refinement leaving the mode unset, and the same saved query runs with
  `refine={"to_many_handling": "associate"}`, in process, through REST and through the MCP `query` tool
- **THEN** the first two responses each carry exactly one `deprecated_input` warning and the third none

#### Scenario: A save reports the deprecation and keeps broadcast
- **WHEN** a query-backed model whose source query carries `"broadcast"` is saved through REST and through MCP,
  and a memory whose query carries `"broadcast"` is saved
- **THEN** the REST response's `warnings` carry the `deprecated_input` payload, the MCP response and the memory
  save's `warnings` each carry its message exactly once, and every stored query reloads with `"broadcast"`

#### Scenario: The broadcast warning names the concrete partition
- **WHEN** broadcast queries select `sum(customers.spend)` rooted at `orders` by `orders.status` (and again by
  `orders.status` and `customers.tier`), `sum(customers.spend, window='1y')` rooted at `customers` by
  `orders.status` and the month of `customers.signup_at`, and the composite `sum(customers.spend) /
  sum(orders.amount)` by `orders.status`
- **THEN** the `broadcast` warnings carry `partition_by` `[]`, `[customers.tier]`, `[customers.signup_at]` and
  `[]` respectively, each hint names `sum(customers.spend)` (or the windowed call) and that `partition_by=[…]`,
  and each aggregate rewritten with that `partition_by=` and run omitting the mode returns identical values with
  no `broadcast`, `associated` or `deprecated_input` warning, by executed values on SQLite and DuckDB

#### Scenario: Aliased, repeated, filter and order positions name the call
- **WHEN** a broadcast query uses an aliased broadcast aggregate, the same aggregate twice in one composite, and
  a broadcast aggregate used only in a measure filter and only as an order key
- **THEN** each `broadcast` warning's hint names the aggregate's own call and its concrete `partition_by=[…]`

#### Scenario: The schema does not offer broadcast but validation accepts it
- **WHEN** the query and refinement JSON schemas are generated, a query sets `to_many_handling: "broadcast"`, and
  another sets `to_many_handling: "sometimes"`
- **THEN** the schemas enumerate exactly `associate` and `error`, the broadcast query validates, and the
  unrecognized value fails naming `associate` and `error` and not `broadcast`

#### Scenario: Other modes report no deprecation
- **WHEN** the same query runs omitting the mode, under `"associate"` and under `"error"`
- **THEN** no response carries a `deprecated_input` warning and no `FutureWarning` is recorded
