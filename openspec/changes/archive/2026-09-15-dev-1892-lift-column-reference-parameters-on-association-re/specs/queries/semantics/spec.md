# queries/semantics — delta

## ADDED Requirements

### Requirement: Aggregation parameters are typed by the home dataset's grain
Every parameter of an aggregation — a keyword or positional parameter (`weight=`,
`other=`, a custom aggregation's declared parameters) and a parameter supplied by the
aggregation definition's default — SHALL be typed against the dataset the aggregation
runs over: with `D` that dataset and `G` its grain, a parameter `P` is legal iff `G`
determines `P` — `P` is a grain member, an aggregate whose `partition_by=` grain is a
subset of `G` (a cell value of the same dataset), or a column reached from a grain
member over provably to-one join hops (per Axiom 1, Determination). A legal parameter is evaluated once per
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

#### Scenario: NULL parameter values follow SQL aggregate semantics
- **WHEN** a legal parameter is NULL for some cells (e.g. a to-one lookup with no match)
- **THEN** those cells contribute exactly as the underlying SQL aggregate treats NULL
  inputs (a NULL weight contributes nothing to `weighted_avg`), by executed values on
  SQLite and DuckDB

## MODIFIED Requirements

### Requirement: Second-order aggregation over attached values
An aggregation whose source operand resolves entirely to attached values —
partitioned aggregates, directly or combined through arithmetic and scalar
functions — SHALL aggregate over the operand dataset's cells, never over the
query's population rows. The operand dataset is typed by the union of its
constituents' grains (each constituent at its declared `partition_by=` grain; a
constituent with no declared grain is typed at the query's dimensions). Its rows
are the distinct union-grain cells of the row-filtered population; each
constituent's value attaches null-safely at its own grain, a cell a constituent
lacks contributes NULL, and no constituent adds or removes cells. The outer
aggregation partitions those cells by the query dimensions attributable to the
operand dataset per Axiom 1 (Determination): a dimension is attributable iff the
grain determines it — a grain member, or a field reached from a grain member
over provably to-one join hops (a foreign-key grain field thus determines its
referenced model's fields and any column further along a to-one chain; an
entity-key grain field additionally determines all of its own model's columns).
A grain field does not determine its own model's other columns when the grain
does not fix that model's key — a foreign key does not identify the many-side
row — and an expression grain field (time bucket, computed dimension) determines
only itself.
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
  (e.g. `sum(amount, partition_by=customers.id)` rooted at `corders`) and a query
  dimension is a column of that model or reached from it over a to-one hop
  (`customers.region_id`, `customers.regions.name`)
- **THEN** the outer aggregation partitions the cells exactly by that dimension with
  no broadcast warning, by executed values

#### Scenario: A foreign-key grain field determines its to-one target
- **WHEN** the inner grain contains a foreign-key column
  (e.g. `sum(amount, partition_by=customers.region_id)` rooted at `corders`) and a
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
