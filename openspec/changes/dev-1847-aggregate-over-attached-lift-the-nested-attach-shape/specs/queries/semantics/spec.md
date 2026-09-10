# queries/semantics delta

## ADDED Requirements

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
