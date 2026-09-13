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
operand dataset — dimensions in its grain, or determined from an entity-key
grain field over provably to-one join hops; an expression grain field (time
bucket, computed dimension) determines only itself. Unattributable dimensions
resolve per `to_many_handling` exactly as for model-rooted aggregates: broadcast
with a self-announcing warning naming the dimension and the remedy, per-cell
association, or a clear error. Adding a re-aggregated measure MUST NOT change
the result row count or any other column's values.

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
