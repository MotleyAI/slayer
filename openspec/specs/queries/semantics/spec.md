# queries/semantics Specification

## Purpose
States the query-wide semantic axioms every query obeys: which rows aggregations run
over, which dimensions they may vary along, what the result grain is, how different-grain
values combine, what filters do to populations, and what stays independent of what.
Producer mechanics and per-shape detail live in `queries/cross-model-aggregates`; the
complete end-target algebra (including not-yet-implemented semantics) is mapped with
status tags in `architecture/semantics.arc42.md`.

## Requirements

### Requirement: Attribution by determination
A query dimension SHALL be attributable to an aggregate iff a chain of provably
many-to-one join hops (per `models/join-cardinality` evidence) leads from the
aggregate's root model to the dimension. Attributable dimensions retain exact per-cell
values — the cells partition the root rows and sum to the total. Every other dimension
resolves per the query's `to_many_handling` mode: under `"broadcast"` (the default) the
aggregate broadcasts across it (its value repeats across that dimension's cells);
under `"associate"` each cell aggregates over the distinct root entities associated
with it (per `queries/attribution-modes`); under `"error"` the query refuses. In no
mode does an aggregate join through an unproven path.

#### Scenario: Attributable dimension partitions, unattributable broadcasts
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by one
  customer-determined dimension and one orders-level dimension
- **THEN** by executed values, cells vary along the determined dimension, sum to the
  total across it, and repeat unchanged across the orders-level dimension

#### Scenario: Associate mode keeps attributable dimensions exact
- **WHEN** the same query runs with `to_many_handling: "associate"`
- **THEN** by executed values, cells along the customer-determined dimension are
  unchanged from the broadcast run, while cells along the orders-level dimension carry
  per-cell distinct-entity values

### Requirement: No double counting
Every aggregation — however consumed: directly selected, a composite leaf, filter-only,
order-only, inside a computed dimension, or nested in a producer, and local and
cross-model alike — SHALL run over the rows of its own root model, each root row
counted exactly once per result cell — never over the row product of a join; no join
fan-out may multiply an aggregation's inputs. The sole exception is the host-grain wrap
(an aggregate deliberately evaluated over the join result at host grain), as specified
in `queries/cross-model-aggregates`.

#### Scenario: Join fan-out never multiplies aggregation inputs
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` and each customer
  has several orders
- **THEN** by executed values every customer's spend is counted exactly once, however
  many orders fan the join

#### Scenario: Local aggregate over a fanning dimension never multiplies
- **WHEN** a query rooted at `customers` selects the local measure `spend:sum` by
  `orders.status`, and one customer has two orders with the same status
- **THEN** by executed values that customer's spend is never counted twice in a cell —
  under the default the metric broadcasts with a warning; under
  `to_many_handling: "associate"` the cell carries the distinct-entity value

#### Scenario: Fanned local aggregates are caught in every consumer context
- **WHEN** a local aggregate over a fanning dimension is consumed inside an arithmetic
  composite, an ORDER BY entry, an aggregate-phase filter, or a computed dimension
- **THEN** each context resolves it per the mode exactly as a directly selected
  measure — never through the silently multiplied join product

### Requirement: Grain guarantee
For a query with at least one measure, and for a measure-less query with distinct
dimension values enabled (the default), the result SHALL have exactly one row per
combination of dimension values present among the row-filtered population's rows.
Raw-row mode (`distinct_dimension_values=false`) is the documented exception and
returns one row per population row.

#### Scenario: One row per dimension combination
- **WHEN** an aggregating query groups by dimensions whose value combinations repeat
  across many population rows
- **THEN** the result contains each present combination exactly once

### Requirement: Grain-union broadcasting
When aggregates at different grains combine in one expression, the expression's grain
SHALL be the union of its operands' grains, each operand broadcast from its own grain
to that union; combining at different grains is never, by itself, an error. Broadcast
SHALL only go from coarser to finer; consuming an aggregate at a strictly coarser
grain SHALL fail with a typed error, never collapse implicitly.

#### Scenario: Different-grain arithmetic broadcasts to the union
- **WHEN** a measure divides a query-grain aggregate by a
  `partition_by=<coarser subset>` aggregate
- **THEN** by executed values the coarser operand repeats across the dimensions it
  lacks and each cell holds the ratio at the union grain

#### Scenario: Coarser consumption is a typed error
- **WHEN** a query shape would need an aggregate consumed at a strictly coarser grain
  than its own
- **THEN** the query fails with a clear typed error rather than implicitly collapsing
  or duplicating rows

### Requirement: Filters restrict by association or fail loudly
A row-level filter conjunct that reaches an aggregate's root only across
non-determining paths SHALL either restrict the aggregate's population by association
— the aggregate computes over exactly the root rows related to at least one surviving
row combination, each counted once — or be loudly excluded (dropped-filter warning,
an error under `to_many_handling: "error"`) per the pushdown-scope rules in
`queries/cross-model-aggregates`. A stated restriction SHALL never be silently
ignored and SHALL never fan out an aggregation's inputs.

#### Scenario: Cross-path filter restricts the population by association
- **WHEN** a query rooted at `orders` filters on an orders-level predicate and selects
  `customers.spend:sum`
- **THEN** by executed values the metric counts exactly the customers with at least
  one order passing the predicate, each once

#### Scenario: A restriction is never silently ignored
- **WHEN** a filter conjunct cannot be applied to an aggregate's population
- **THEN** the response carries the dropped-filter warning (or the query errors under
  `to_many_handling: "error"`) — never an unrestricted value presented as restricted

### Requirement: Compositionality
Each result cell's values SHALL depend only on the evaluated expression, the
population, and the row-level filters — never on which other measures, measure-typed
filters, or order entries the query contains. Adding or removing a projected measure
SHALL NOT change the row set or any other column's values. A measure-typed filter
masks result cells without changing any surviving cell's values; ORDER BY and LIMIT
select and order rows without changing any cell's values. A multi-measure query SHALL
return, cell by cell, the same values as its single-measure splits.

#### Scenario: Adding a measure changes nothing else
- **WHEN** any supported query runs with and without one additional measure
- **THEN** both runs return the same rows and identical values in all shared columns

#### Scenario: Measure-typed filter masks without altering values
- **WHEN** a query filters on an aggregate predicate
- **THEN** surviving cells carry exactly the values the unfiltered query gave them

#### Scenario: A query equals its single-measure splits
- **WHEN** a two-measure query and its two single-measure counterparts run
- **THEN** each measure's values match cell by cell across the runs

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
alike.

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

### Requirement: Dice–slice correspondence under associate
Under `to_many_handling: "associate"`, filtering the population to `d = v` SHALL yield
the same value for an **association-eligible** aggregate as slicing the `v` cell of the
same query grouped by `d`, by executed values, for any dimension `d` reachable from the
population. An aggregate whose shape is unsupported under association (`window=`,
`first`/`last`, a root without a unique key, or a column-reference parameter) is not
subject to this rule — it fails with its typed error instead. The broadcast default does
not satisfy this correspondence; its warning hint (per Loud degradation) is the required
disclosure.

#### Scenario: Filtered value equals the sliced cell
- **WHEN** an associate-mode query filters `status = 'ok'` with `customers.spend:sum`
  rooted at `orders`, and its counterpart groups by `status` instead
- **THEN** the filtered run's value equals the `ok` cell of the grouped run, by
  executed values, across a generated family of query shapes

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

### Requirement: Row-grain aggregation sources
An aggregation whose source operand combines row-level column references with
attached values — partitioned aggregates, directly or through arithmetic and
scalar functions — SHALL aggregate over the row-filtered population rows of its
home dataset, never over the attached operands' cells. The operand types at row
grain: the union of a row leaf's grain with any attached constituent's grain is
row grain, the finest. Each attached constituent is computed at its own declared
`partition_by=` grain (a constituent with no declared grain is typed at the
query's dimensions) and its value is broadcast onto each population row
null-safely: a row whose constituent lacks a value carries NULL for that
constituent, and the attachment never adds or removes rows. Per-row weighting is
the defined meaning of the shape and the broadcast SHALL NOT warn. The outer
aggregation evaluates at its consumer grain exactly as over any row-level
expression. Adding such a measure MUST NOT change the result row count or any
other column's values.

#### Scenario: Row-weighted value distinguishable from pure re-aggregation
- **WHEN** a query over dimensions `[region]` selects the measure
  `sum(quantity * avg(unit_price, partition_by=product))`
- **THEN** each region row carries the sum, over that region's base rows, of the
  row's `quantity` times its product's average unit price, by executed values,
  distinguishable from the pure re-aggregation
  `sum(avg(unit_price, partition_by=product))` and from
  `sum(quantity * unit_price)`

#### Scenario: Missing constituent value is NULL on a surviving row
- **WHEN** some base row's cell has no value for an attached constituent and the
  operand restores it through a NULL-restoring composite (e.g.
  `coalesce(avg(unit_price, partition_by=product), 0) * quantity`)
- **THEN** that row still contributes to the outer aggregation with the restored
  value — the population row exists and only its constituent value was NULL,
  unlike a fully-attached source, whose carrier excludes the absent cell

#### Scenario: Ungrained inner constituent types at the query's dimensions
- **WHEN** a query selects `sum(quantity * avg(unit_price))` with no
  `partition_by=` on the inner aggregate
- **THEN** the inner value is computed at the query's dimensions, broadcast onto
  each row, and weighted per row, with no degenerate-re-aggregation warning

#### Scenario: Row filters bound both the population and the constituents
- **WHEN** the query carries a row-level filter conjunct
- **THEN** it restricts both the population rows the outer aggregation consumes
  and each attached constituent's producer, and the executed value reflects both

### Requirement: Ungrained aggregate parameters type at the query grain
An aggregate-valued parameter with no declared `partition_by=` SHALL be typed at
the query's dimensions — exactly as an ungrained aggregation source constituent
is — before the parameter determination rule is applied: it is legal iff the
aggregation's operating grain determines the query's dimensions, and it is then
evaluated once per query-grain cell and attached into the aggregation's input
relation (a row-level source) or carried as a constituent of the operand dataset
(a re-aggregation), broadcast onto that dataset's cells. The rule SHALL apply
identically in every kernel and in every `to_many_handling` mode; rejecting the
ungrained form on construction grounds is a closure violation.

#### Scenario: Ungrained parameter on a row-level source
- **WHEN** a locally-rooted query over `[region]` selects the model-defined
  `wsum(amount, weight=sum(amount))` (`SUM({value} * {weight})`)
- **THEN** each region row equals the region's row sum times the region total —
  the query-grain value row-attached as the weight — by executed values

#### Scenario: Ungrained parameter under association
- **WHEN** an associate-mode query rooted at `orders` over `[status]` selects
  `customers.spend:wsum(weight=sum(amount))`
- **THEN** each status cell equals the sum of its distinct associated customers'
  spend times that status cell's order total, by executed values

#### Scenario: Ungrained parameter on a re-aggregation
- **WHEN** a query over `[region]` selects
  `wsum(sum(amount, partition_by=[city, region]), weight=count(id))`
- **THEN** the parameter is typed at `[region]`, carried as a constituent of the
  `[city, region]` operand dataset and broadcast onto its cells, and the
  executed value equals the manual encoding with
  `count(id, partition_by=region)` — never the not-determined rejection
