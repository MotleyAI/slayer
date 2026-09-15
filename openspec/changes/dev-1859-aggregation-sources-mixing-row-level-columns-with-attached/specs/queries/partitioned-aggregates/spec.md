# queries/partitioned-aggregates delta

## ADDED Requirements

### Requirement: Mixed sources carry the full expression-source surface
An aggregation source mixing row-level references with attached values SHALL
behave as a row-level expression source: everything legal for a plain
expression source is legal for it, and nothing more. The outer aggregation
SHALL support the plain scalar family, `count` (base rows with a non-null
operand value) and `count_distinct`, parametric and model-defined custom
aggregations — including multi-input built-ins and column-reference parameters
resolvable at row scope — and its own `partition_by=` and `window=`, each
resolved by the same rules as over a plain expression source. `first`/`last`
over the mixed expression SHALL keep the existing not-supported-over-an-
expression error. The shape SHALL be legal in measure, filter (typing as a
measure: pruning result rows without altering surviving values), and ORDER BY
positions, and as a computed dimension when grain-self-contained (the outer and
every inner aggregate explicitly grained). No producer or grouping step may
group by the row leaf or the attached value themselves — they feed the outer
aggregation only — and the mixed shape SHALL never be compiled through the
fully-attached carrier (whose cell-over-cell value differs). Row leaves keep
every existing expression-source restriction; attached constituents may be
cross-model. A source nesting a transform stays rejected.

#### Scenario: Outer partition_by over a mixed source
- **WHEN** a query over `[region, product]` selects
  `sum(quantity * avg(unit_price, partition_by=product), partition_by=region)`
- **THEN** every row of a region carries that region's row-weighted total,
  broadcast exactly as any explicit-grain partitioned measure, by executed
  values

#### Scenario: Outer window over a mixed source
- **WHEN** a query with a month time dimension selects
  `sum(quantity * avg(unit_price, partition_by=product), window='90d')`
- **THEN** each bucket carries the trailing-90-day row-weighted total as of that
  bucket, by executed values

#### Scenario: Parametric outer with a row-valued parameter
- **WHEN** a query selects
  `wavg(quantity * avg(unit_price, partition_by=product), weight=qty)` (a
  weighted average whose weight is a row column)
- **THEN** it executes with the hand-computed row-weighted average, never a
  column-parameter rejection

#### Scenario: Custom and multi-input aggregations over a mixed source
- **WHEN** a model-defined custom aggregation or a two-input built-in (e.g.
  `corr(quantity * avg(unit_price, partition_by=product), qty)`) consumes a
  mixed source
- **THEN** each executes with correct values through its own rendering path

#### Scenario: Count semantics over a mixed source
- **WHEN** a query selects `count(quantity * avg(unit_price, partition_by=product))`
- **THEN** each cell counts its base rows whose operand value is non-null

#### Scenario: Ranked aggregation over a mixed source keeps the expression error
- **WHEN** a query selects `first(quantity * avg(unit_price, partition_by=product))`
- **THEN** it fails with the existing error that `first` is not supported over
  an expression — never a mixing error and never wrong values

#### Scenario: Filter and order positions
- **WHEN** the mixed measure appears only in a filter
  (`sum(quantity * avg(unit_price, partition_by=product)) > 100`) or only as an
  ORDER BY target
- **THEN** the filter types as a measure — pruning result rows with surviving
  values unchanged — and the order sorts by the same value the measure form
  returns

#### Scenario: Grain-self-contained dimension position
- **WHEN** a query groups by a computed dimension banding
  `sum(quantity * avg(unit_price, partition_by=product), partition_by=region)`
- **THEN** rows group by the band with correct executed values and unchanged
  cardinality

#### Scenario: Adding a mixed measure is cardinality-neutral
- **WHEN** any supported query runs with and without an additional mixed-source
  measure
- **THEN** both runs return the same rows and identical values in all shared
  columns, and the emitted SQL leaks no internal placeholder names

#### Scenario: Never the fully-attached carrier
- **WHEN** the emitted SQL for a mixed-source measure is inspected
- **THEN** no relation groups by the attached value or the row leaf; the outer
  aggregation consumes base rows with the attached value joined on the
  constituent's complete grain

### Requirement: Attached parameters on row-level sources
An aggregation over a row-level source whose column-reference parameter
resolves to an attached value SHALL compile when the aggregation's operating
grain determines the parameter: the parameter's producer is computed at its own
declared grain and its value attached into the aggregation's input relation —
per entity row for an associate-mode aggregation, per population row otherwise
— null-safely on the producer's complete grain. A NULL grain-key value forms
its own cell and attaches null-safely, per the established null rules. A
parameter the operating grain does not determine SHALL keep its typed
rejection. Outer parameters on fully-attached (re-aggregation) sources keep
their existing rules.

#### Scenario: Associate-mode attached parameter executes
- **WHEN** a query rooted at `orders` over `[status]` under
  `to_many_handling: "associate"` selects
  `customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`
- **THEN** each status cell carries the average of its distinct associated
  customers' spend, weighted by each customer's region total, by hand-computed
  executed values on SQLite and DuckDB

#### Scenario: A NULL-region entity weights by the NULL cell
- **WHEN** an associated customer has no region
- **THEN** its weight is the NULL-region cell's total — the NULL key forms its
  own cell and attaches null-safely — by executed values

#### Scenario: Ordinary-mode attached parameter executes
- **WHEN** a locally-rooted query selects
  `spend:weighted_avg(weight=sum(amount, partition_by=region))` with `region`
  determined per row
- **THEN** it executes with the row-attached per-region weight, by executed
  values

#### Scenario: Undetermined attached parameter stays rejected
- **WHEN** the aggregation's operating grain does not determine the attached
  parameter
- **THEN** the query fails with the typed determination error, never wrong
  values
