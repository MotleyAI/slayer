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
  named computed dimension `is_p` defined as `product = 'P'`, and each query also runs
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
A derived column is determined when the grain determines every column its definition
reads (value and filter), recursively — exactly as its inline expression would be.
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
  `sum(cumsum(sum(amount, partition_by=[region, ordered_at])) - 1)`
- **THEN** each month carries the sum over regions of that region's running total
  minus one per cell, by hand-computed executed values on SQLite and DuckDB

#### Scenario: Ungrained transform constituent is identity plus a warning
- **WHEN** a query selects `sum(cumsum(sum(amount)))` over a month time dimension
- **THEN** the value equals `cumsum(sum(amount))` per cell and the response carries
  the degenerate-re-aggregation warning, exactly as `sum(sum(amount))` does

#### Scenario: Ungrained inner of a mixed operand types at the query grain
- **WHEN** a query over a month time dimension selects
  `sum(rank(sum(amount, partition_by=[region, ordered_at]) - sum(amount), direction='desc'))`
- **THEN** the ungrained inner is the month total, computed at the query grain and
  broadcast onto the `(region, month)` cells before ranking — never re-evaluated per
  region — by hand-computed executed values distinguishable from the per-cell
  evaluation

#### Scenario: Collapsing transform constituent drops the time axis
- **WHEN** a query over a month time dimension selects
  `sum(last(sum(amount, partition_by=[region, ordered_at])))`
- **THEN** the constituent is typed at `(region)`: every month carries the sum over
  regions of each region's most recent monthly total, the response warns that the
  month dimension is broadcast, and a region absent from a month still counts —
  distinguishable from summing the `(region, month)` cells present in that month

#### Scenario: Collapsing and preserving constituents share one operand dataset
- **WHEN** the same query selects
  `sum(cumsum(sum(amount, partition_by=[region, ordered_at])) - last(sum(amount, partition_by=[region, ordered_at])))`
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
- **WHEN** a query over dimensions `[region, city = 'Alpha']` selects
  `avg(sum(amount, partition_by=[city, region]))` under each `to_many_handling` mode
- **THEN** each `(region, city = 'Alpha')` row carries the average of exactly its own
  city cells — `(North, true)` 30, `(North, false)` 60, `(South, true)` 40,
  `(South, false)` 100, `(East, false)` 60, Gap's NULL-city cell 12, `(Gap, false)` 8,
  `(Void, false)` NULL — by executed values on SQLite and DuckDB, with no broadcast or
  association warning and no error

#### Scenario: Expression over a to-one-determined dimension matches its plain spelling
- **WHEN** the operand is `sum(amount, partition_by=customer_id)` rooted at `corders` and
  the query dimension is `customers.regions.name = 'North'`
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
  `product = 'P'`, selects `avg(sum(amount, partition_by=[city, region]))`
- **THEN** under the default mode each region's value repeats across `is_p` and the
  broadcast warning names `is_p`, the reason that the operand grain does not determine
  it, and the `partition_by=` remedy; under `to_many_handling: "error"` the query fails
  naming `is_p`

#### Scenario: Derived column over grain members partitions like its inline spelling
- **WHEN** the model declares a derived column `city_upper` defined as `UPPER(city)` and a
  query over dimensions `[region, city_upper]` selects
  `avg(sum(amount, partition_by=[city, region]))`
- **THEN** each row carries the average of exactly its own city cells, identical to the
  query over `[region, upper(city)]`, by executed values on SQLite and DuckDB, with no warning
