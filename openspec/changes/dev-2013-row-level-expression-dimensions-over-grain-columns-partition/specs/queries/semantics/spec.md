## MODIFIED Requirements

### Requirement: Loud degradation
Whenever partition semantics is unavailable — an implicit attribution-loss broadcast,
or a filter excluded from a producer — the response SHALL carry a machine-readable
warning naming the affected aggregate and the cause, and `to_many_handling: "error"`
SHALL turn it into an error (per the metadata and mode requirements in
`queries/cross-model-aggregates` and `queries/attribution-modes`). Every broadcast
warning SHALL carry the dice–slice hint: filtering on the dimension restricts the
metric by association while slicing broadcasts, and `to_many_handling: "associate"`
reconciles the two. Associate-mode resolution SHALL warn that the cells' entity
populations may overlap across the named dimensions and are not additive. Explicit
`partition_by=` is requested grain and SHALL NOT warn — broadcasting and associating
alike. Warnings and errors SHALL name a dimension by its query name — a named
dimension's `name`, else its canonical dotted path — never an internal alias.

#### Scenario: Implicit broadcast warns, explicit grain does not
- **WHEN** one query broadcasts a metric over an unattributable dimension and another
  declares the same coarser grain via `partition_by=`
- **THEN** the first response carries the broadcast warning and the second carries none

#### Scenario: Broadcast warning carries the dice–slice hint
- **WHEN** any query broadcasts a metric over an unattributable dimension
- **THEN** the broadcast warning entry names the associate mode as the reconciliation,
  whether or not the query contains a filter

#### Scenario: Associate warns about overlapping populations
- **WHEN** an associate-mode query resolves a metric over an unattributable dimension
  implicitly (no explicit `partition_by=`)
- **THEN** the response carries a warning naming the metric and the dimensions whose
  cells are not additive, and the same event surfaces as a Python-level warning

#### Scenario: Diagnostics name dimensions by their query name
- **WHEN** a re-aggregation broadcasts over the joined dimension `customers.regions.name`
  (e.g. `avg(sum(amount, partition_by=amount))` rooted at `corders`), and another over a
  named computed dimension `is_p` defined as `product == 'P'`, and each query also runs
  under `to_many_handling: "error"`
- **THEN** the broadcast warnings and the errors name `customers.regions.name` and
  `is_p` — never a `__`-joined path or a generated `grain_…` alias

### Requirement: Second-order aggregation over attached values
An aggregation whose source operand resolves entirely to attached values —
partitioned aggregates, or explicitly grained transforms over them, directly or
combined through arithmetic and scalar functions — SHALL aggregate over the
operand dataset's cells, never over the query's population rows. The operand
dataset is typed by the union of its constituents' grains: an aggregate
constituent at its declared `partition_by=` grain; a transform constituent at the
union of its inner aggregates' grains — each inner's explicit `partition_by=`, else
the query's dimensions and time buckets — where a windowed inner's grain always
includes the query's time bucket whether or not its `partition_by=` names it, and an
axis-collapsing transform (`first`, `last`) at that union minus its time axis; a
constituent with no declared grain is typed at the query's dimensions. Its rows are the distinct union-grain cells of the row-filtered
population; each constituent's value attaches null-safely at its own grain, a cell
a constituent lacks contributes NULL, and no constituent adds or removes cells. The
outer aggregation partitions those cells by the query dimensions attributable to
the operand dataset per Axiom 1 (Determination): a dimension is attributable iff
the grain determines it — a grain member, or a field reached from a grain member
over provably to-one join hops (a foreign-key grain field thus determines its
referenced model's fields and any column further along a to-one chain; an
entity-key grain field additionally determines all of its own model's columns).
A grain field does not determine its own model's other columns when the grain
does not fix that model's key — a foreign key does not identify the many-side
row — and an expression grain field (time bucket, computed dimension) determines
only itself — as a grain member it pins nothing further. Determination is closed
under row-level combination (Axiom 2.2): a dimension combining determined operands
through arithmetic, comparison, scalar functions or conditionals — a literal being
determined by every grain, a time bucket when the grain determines its column, and an
embedded aggregate when the grain determines its `partition_by=` members — is itself
attributable, whatever its spelling.
Unattributable dimensions resolve per `to_many_handling` exactly as for
model-rooted aggregates: broadcast with a self-announcing warning naming the
dimension and the remedy, per-cell association, or a clear error. Adding a
re-aggregated measure MUST NOT change the result row count or any other
column's values.

#### Scenario: Average of city totals per region
- **WHEN** a query over dimensions `[region]` selects the measure
  `avg(sum(amount, partition_by=[city, region]))`
- **THEN** each region row carries the unweighted average of that region's city
  totals, by executed values, distinguishable from the row-count-weighted value

#### Scenario: Grained transform constituent aggregates the transform's cells
- **WHEN** a query over a month time dimension selects
  `sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - 1)`
- **THEN** each month carries the sum over regions of that region's running total
  minus one per cell, by hand-computed executed values on SQLite and DuckDB

#### Scenario: Ungrained transform constituent is identity plus a warning
- **WHEN** a query selects `sum(cumsum(amount:sum))` over a month time dimension
- **THEN** the value equals `cumsum(amount:sum)` per cell and the response carries
  the degenerate-re-aggregation warning, exactly as `sum(sum(amount))` does

#### Scenario: Ungrained inner of a mixed operand types at the query grain
- **WHEN** a query over a month time dimension selects
  `sum(rank(amount:sum(partition_by=[region, ordered_at]) - amount:sum))`
- **THEN** the ungrained inner is the month total, computed at the query grain and
  broadcast onto the `(region, month)` cells before ranking — never re-evaluated per
  region — by hand-computed executed values distinguishable from the per-cell
  evaluation

#### Scenario: Collapsing transform constituent drops the time axis
- **WHEN** a query over a month time dimension selects
  `sum(last(amount:sum(partition_by=[region, ordered_at])))`
- **THEN** the constituent is typed at `(region)`: every month carries the sum over
  regions of each region's most recent monthly total, the response warns that the
  month dimension is broadcast, and a region absent from a month still counts —
  distinguishable from summing the `(region, month)` cells present in that month

#### Scenario: Collapsing and preserving constituents share one operand dataset
- **WHEN** the same query selects
  `sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - last(amount:sum(partition_by=[region, ordered_at])))`
- **THEN** the operand dataset is the `(region, month)` cells, the collapsed value
  broadcasts onto them, each month is attributable through the preserving
  constituent, and the value is correct with no warning

#### Scenario: Composite operand keeps the population's cells
- **WHEN** the operand combines aggregates at `[city, region]` and `[region]`
  grains and some union-grain cell has no value for one constituent (e.g. a
  measure-local filter eliminates its rows)
- **THEN** the operand dataset has exactly the population's distinct
  `(city, region)` cells, the region-grain value broadcast onto them, and the
  missing value contributes NULL to that cell without removing it

#### Scenario: Outer dimension attributed through a to-one chain
- **WHEN** the inner grain is an entity key (e.g. `customer_id`) and a query
  dimension is reached from it over a provably to-one join chain
- **THEN** the outer aggregation partitions the inner cells exactly by that
  dimension, with no broadcast warning

#### Scenario: Outer dimension seeded by a nested-path entity key
- **WHEN** the inner grain contains a joined model's unique key
  (e.g. `sum(amount, partition_by=customers.id)` rooted at `orders`) and a query
  dimension is a column of that model or reached from it over a to-one hop
  (`customers.region_id`, `customers.regions.name`)
- **THEN** the outer aggregation partitions the cells exactly by that dimension with
  no broadcast warning, by executed values

#### Scenario: A foreign-key grain field determines its to-one target
- **WHEN** the inner grain contains a foreign-key column
  (e.g. `sum(amount, partition_by=customers.region_id)` rooted at `orders`) and a
  query dimension is a field of the model that key points at over the provably
  to-one hop (`customers.regions.name`)
- **THEN** the outer aggregation partitions the cells exactly by that dimension with
  no broadcast warning, by executed values (Axiom 1: the fixed key value pins the
  to-one target row)

#### Scenario: A non-key grain field does not determine its own model's siblings
- **WHEN** the inner grain contains a joined model's foreign-key column
  (e.g. `partition_by=customers.region_id`, which does not identify a customer) and
  a query dimension is another column of that same model reached only by
  identifying its row (`customers.id`)
- **THEN** the dimension is not attributable and resolves per `to_many_handling`

#### Scenario: Unattributable outer dimension broadcasts with a warning
- **WHEN** a query over dimensions `[region]` selects
  `avg(sum(amount, partition_by=city))` under the default mode
- **THEN** every region row carries the global average of city totals and the
  response warns, naming `region`, the reason it is not attributable, and the
  remedy (add it to the inner `partition_by=`)

#### Scenario: Unattributable outer dimension associates on request
- **WHEN** the same query runs under `to_many_handling: "associate"`
- **THEN** each region cell aggregates the distinct city cells associated with
  it through the population — a city value co-occurring with two regions counts
  in both — by executed values

#### Scenario: Unattributable outer dimension refuses under error mode
- **WHEN** the same query runs under `to_many_handling: "error"`
- **THEN** it fails with a clear error naming the measure, the dimension, and
  the remedy — never wrong numbers

#### Scenario: Degenerate re-aggregation is identity plus a warning
- **WHEN** a query selects `avg(sum(amount))` (operand grain equals the outer
  grain)
- **THEN** the value equals `sum(amount)` per cell and the response carries a
  degenerate-re-aggregation warning naming both grains and the
  `partition_by=` remedy

#### Scenario: Row-level expression over grain members partitions exactly
- **WHEN** a query over dimensions `[region, city == 'Alpha']` selects
  `avg(sum(amount, partition_by=[city, region]))` under each `to_many_handling` mode
- **THEN** each `(region, city == 'Alpha')` row carries the average of exactly its own
  city cells — `(North, true)` 30, `(North, false)` 60, `(South, true)` 40,
  `(South, false)` 100, `(East, false)` 60, Gap's NULL-city cell 12, `(Gap, false)` 8,
  `(Void, false)` NULL — by executed values on SQLite and DuckDB, with no broadcast or
  association warning and no error

#### Scenario: Expression over a to-one-determined dimension matches its plain spelling
- **WHEN** the operand is `sum(amount, partition_by=customer_id)` rooted at `corders` and
  the query dimension is `customers.regions.name == 'North'`
- **THEN** the `true` cell carries 35 and the `false` cell 100 — the values the plain
  `customers.regions.name` dimension gives North and South — by executed values, with no
  warning

#### Scenario: Aggregate-carrying expression dimension grained by a determined key
- **WHEN** the same operand is grouped by the dimension
  `sum(amount, partition_by=customers.regions.name) > 80`
- **THEN** the `false` cell carries 35 and the `true` cell 100, by executed values, with no
  warning — identical to grouping by the bare aggregate dimension

#### Scenario: Expression over an undetermined column still resolves per mode
- **WHEN** a query over dimensions `[region, is_p]`, with `is_p` defined as
  `product == 'P'`, selects `avg(sum(amount, partition_by=[city, region]))`
- **THEN** under the default mode each region's value repeats across `is_p` and the
  broadcast warning names `is_p`, the reason that the operand grain does not determine
  it, and the `partition_by=` remedy; under `to_many_handling: "error"` the query fails
  naming `is_p`

### Requirement: Aggregation parameters are typed by the home dataset's grain
Every parameter of an aggregation — a keyword or positional parameter (`weight=`,
`other=`, a custom aggregation's declared parameters) and a parameter supplied by the
aggregation definition's default — SHALL be typed against the dataset the aggregation
runs over: with `D` that dataset and `G` its grain, a parameter `P` is legal iff `G`
determines `P` — `P` is a grain member, an aggregate each of whose `partition_by=`
members `G` determines (a cell value of the same dataset), or a column reached from a
grain member over provably to-one join hops (per Axiom 1, Determination), determination
being closed under row-level combination. A parameter naming a
derived column is determined only when `G` determines every dependency of that column's
definition, recursively — a derived parameter whose definition crosses a hop `G` does
not pin is not determined, however its own path is reached. A legal parameter is evaluated once per
cell of `D` and the aggregation reads that value; the origin of `D`'s rows — a model's
rows deduplicated per entity, or another aggregate's cells — MUST NOT affect the rule.
A parameter `G` does not determine SHALL fail at plan time with a typed error naming
the parameter, the grain, and the remedy (aggregate the parameter to that grain, or add
its determining keys to the operand's `partition_by=`) — never invalid SQL or a silently
arbitrary value. The rule applies identically in every `to_many_handling` mode and in
every consumer position.

#### Scenario: Parameter determined by the entity key under association
- **WHEN** a query rooted at `orders` with `to_many_handling: "associate"` selects
  `customers.spend:weighted_avg(weight=customers.spend)` by the orders-level dimension
  `status`
- **THEN** each status cell equals the spend-weighted average over the distinct customers
  associated with it, by hand-computed executed values on SQLite and DuckDB — each
  customer weighted once, never the join-multiplied figure — with unchanged result grain

#### Scenario: Definition-default parameter follows the same rule
- **WHEN** a model declares a custom aggregation whose parameter defaults to a column of
  the aggregate's root (e.g. `wsum` with `weight` defaulting to `spend`) and an
  associate-mode query selects `customers.spend:wsum` by an unattributable dimension
- **THEN** it executes with the default applied once per distinct entity, by executed
  values, identical to spelling the parameter explicitly

#### Scenario: Parameter determined over a to-one chain from the entity key
- **WHEN** an associate-mode aggregate's parameter is a column reached from the
  aggregate's root over a provably to-one join (e.g. `weight=customers.regions.pop`)
- **THEN** the query executes with the parameter read once per associated entity, by
  executed values

#### Scenario: Parameter as a cell of the operand dataset
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`
- **THEN** each region row carries its city totals averaged with each city's row count as
  the weight, by hand-computed executed values on SQLite and DuckDB, equal to the manual
  two-stage encoding of the same computation

#### Scenario: Parameter determined over a to-one chain from the operand grain key
- **WHEN** the operand grain is an entity key (e.g. `sum(amount, partition_by=customer_id)`
  rooted at `corders`) and the parameter is a column reached from it over a provably
  to-one join (`weight=customers.region_id`)
- **THEN** the query executes with the parameter read once per cell, by executed values

#### Scenario: Parameter not determined by the grain fails closed
- **WHEN** the outer aggregation's parameter is a population-row column against a coarser
  cell grain (`weighted_avg(sum(amount, partition_by=[city, region]), weight=id)`), a
  definition default naming such a column, or an aggregate grained outside the operand
  grain (`weight=sum(amount, partition_by=product)` over a `[city, region]` operand)
- **THEN** the query fails at plan time with a typed error naming the parameter, the
  grain, and the remedy, containing no issue reference

#### Scenario: Derived re-aggregation parameter over an unpinned fanning hop fails closed
- **WHEN** a query rooted at `orders` selects
  `weighted_avg(sum(amount, partition_by=customers.regions.id), weight=customers.regions.bad_pop)`,
  where `regions.bad_pop` is defined as `pop + region_events.value` over the one-to-many
  `regions → region_events` hop the operand grain does not pin
- **THEN** the query fails at plan time with the typed parameter error naming `weight`,
  the grain, and the `partition_by=` remedy — never the multiplying join

#### Scenario: Derived re-aggregation parameter with local-only dependencies executes
- **WHEN** the same query's weight is `customers.regions.derived_pop`, a derived column
  defined as `pop * 2` on `regions`, and the operand grain pins `regions` by its entity key
- **THEN** the query executes with each region cell weighted by twice its population, by
  hand-computed executed values on SQLite and DuckDB

#### Scenario: NULL parameter values follow SQL aggregate semantics
- **WHEN** a legal parameter is NULL for some cells (e.g. a to-one lookup with no match)
- **THEN** those cells contribute exactly as the underlying SQL aggregate treats NULL
  inputs (a NULL weight contributes nothing to `weighted_avg`), by executed values on
  SQLite and DuckDB

#### Scenario: Attached parameter grained by an expression over home-determined columns
- **WHEN** a cross-model aggregate rooted at `corders` and homed on `customers` carries an
  attached parameter whose `partition_by=` names a computed dimension `rid10` defined as
  `customers.region_id * 10`
- **THEN** the query executes, by executed values on SQLite and DuckDB, with values
  identical to the same query with `partition_by=[customers.region_id]` — never a
  parameter-grain error
