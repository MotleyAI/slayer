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
values — the cells partition the root rows and sum to the total; each aggregate SHALL
broadcast across every other dimension (its value repeats across that dimension's
cells) rather than join through an unproven path.

#### Scenario: Attributable dimension partitions, unattributable broadcasts
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` by one
  customer-determined dimension and one orders-level dimension
- **THEN** by executed values, cells vary along the determined dimension, sum to the
  total across it, and repeat unchanged across the orders-level dimension

### Requirement: No double counting
Every aggregation compiled through a producer SHALL run over the rows of its own root
model, each root row counted exactly once per cell — never over the row product of a
join; no join fan-out may multiply an aggregation's inputs. The sole exception is the
host-grain wrap (an aggregate deliberately evaluated over the join result at host
grain), as specified in `queries/cross-model-aggregates`.

#### Scenario: Join fan-out never multiplies aggregation inputs
- **WHEN** a query rooted at `orders` selects `customers.spend:sum` and each customer
  has several orders
- **THEN** by executed values every customer's spend is counted exactly once, however
  many orders fan the join

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
row combination, each counted once — or be loudly excluded (dropped-filter warning in
lenient mode, error in strict mode) per the pushdown-scope rules in
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
  strict) — never an unrestricted value presented as restricted

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
warning naming the affected aggregate and the cause, and strict mode SHALL turn it
into an error (per the metadata and strict-mode requirements in
`queries/cross-model-aggregates`). Explicit `partition_by=` broadcasting is requested
grain and SHALL NOT warn.

#### Scenario: Implicit broadcast warns, explicit grain does not
- **WHEN** one query broadcasts a metric over an unattributable dimension and another
  declares the same coarser grain via `partition_by=`
- **THEN** the first response carries the broadcast warning and the second carries none
