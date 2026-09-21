## MODIFIED Requirements

### Requirement: Nested producers compose to arbitrary depth
Producers SHALL nest to arbitrary depth: a producer's sub-plan may carry nested
producers that themselves carry producers, at any grain admitted by the
consumption seam (equality included). Every attach at every depth joins on the
nested producer's complete grain; structurally identical producers render once
across all depths and scopes; the emitted statement carries one flat `WITH`;
each warning surfaces once per semantic event regardless of consuming depth.

#### Scenario: Depth-three re-aggregation
- **WHEN** a query over `[product]` selects
  `max(avg(sum(amount, partition_by=[city, region, product]), partition_by=[region, product]))`
- **THEN** it executes with hand-computed values — per product, the maximum over
  regions of the average over cities — with one flat `WITH` and closed scopes

#### Scenario: One producer consumed at two depths renders once
- **WHEN** a computed dimension bands `sum(amount, partition_by=[city, region])`
  and a measure selects `avg(sum(amount, partition_by=[city, region]))`
- **THEN** the emitted SQL contains a single city-region producer relation,
  consumed by both the row attach and the outer producer, with correct values

#### Scenario: A producer attached at two phases emits its nested producer first
- **WHEN** a producer carrying a nested producer (a re-aggregation over an attached
  operand) is attached both before aggregation (as a mixed constituent) and after it
  (standalone), so the base relation and the final select both read it
- **THEN** on every Tier-1 dialect the one flat `WITH` lists the nested producer before
  the producer that reads it, and that producer before the base relation — no relation
  references one declared later — and the statement executes on engines that reject
  forward references

### Requirement: Mixed sources carry the full expression-source surface
An aggregation source mixing row-level references with attached values SHALL
behave as a row-level expression source: everything legal for a plain
expression source is legal for it, and nothing more. Row leaves MAY be host-model
or joined-model columns, homed per `queries/semantics` › Home dataset of a
row-level aggregation source; an explicitly grained transform is an attached
constituent exactly like a partitioned aggregate, and so is a re-aggregation — an
aggregate over attached values, hand-written or produced by the `first`/`last`
collapse — evaluated at its own grain and broadcast per partition onto the source's
rows, an empty grain broadcasting its one value onto every row. The outer aggregation
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
cross-model, windowed, grained transforms, or themselves attached-input
aggregations. Discovery SHALL treat an aggregation that
owns attached inputs as opaque below its inputs (source and parameters): those
inputs belong to it — row-attached when it evaluates inline, owned by its own
producer when it is itself a producer answer — and are never discovered as
consumers of the enclosing level; its partition keys are not inputs and stay
visible to the enclosing level, so an attach-carrying computed dimension in
its `partition_by=` still gets the outer attach the grain join needs.

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

#### Scenario: Windowed constituent inside a mixed source
- **WHEN** a query with a month time dimension selects
  `sum(qty * sum(revenue, window='90d'))`
- **THEN** the windowed inner is exactly one nested producer at the bucket
  grain, row-attached into the outer aggregation's input relation — never an
  attach of the enclosing level — and each bucket carries the hand-computed
  row-weighted value, by executed values

#### Scenario: Transform constituent inside a mixed source
- **WHEN** a query over `[region]` selects
  `sum(quantity * rank(avg(unit_price, partition_by=product)))`
- **THEN** the transform is exactly one nested producer at its `(product)` grain,
  row-attached into the outer aggregation's input relation, and each region carries
  the hand-computed row-weighted value, by executed values — never the former
  nested-transform rejection

#### Scenario: Collapsing constituent mixed with a row leaf fails closed
- **WHEN** a query over a month time dimension selects
  `sum(amount * last(amount:sum(partition_by=[region, ordered_at])))`
- **THEN** it no longer fails with the collapsing-transform error — it executes per the
  next scenario; the former fail-closed pin is retired

#### Scenario: Collapsing transform constituent inside a mixed source
- **WHEN** a query over a month time dimension selects `sum(amount * last(X))` with
  `X = amount:sum(partition_by=[region, ordered_at])`
- **THEN** it executes with the hand-computed values Jan 375 / Feb 825 / Mar 900 on
  SQLite and DuckDB: `last(X)` collapses to one value per region, broadcast onto each
  row of that region, multiplied by the row's `amount` and summed per month — never
  the former fail-closed collapse error

#### Scenario: Hand-written re-aggregation constituent inside a mixed source
- **WHEN** a query over a month time dimension selects
  `sum(amount * min(X, partition_by=region))`
- **THEN** it executes with hand-computed values on SQLite and DuckDB, the plan carries
  exactly one producer for the re-aggregation at its `(region)` grain, row-attached on
  `region` and never attached at the enclosing level, the emitted statement has one
  flat `WITH`, scopes are closed, no placeholder leaks, and the query's row count is
  unchanged — never the internal grain-cover assertion

#### Scenario: Empty-grain re-aggregation constituent broadcasts one value
- **WHEN** a query over a month time dimension selects
  `sum(amount * last(amount:sum(partition_by=ordered_at)))`
- **THEN** the collapsed constituent has an empty grain — one value — attached with no
  join keys onto every row, and the query executes with hand-computed values on both
  engines with unchanged cardinality, never an empty join predicate

#### Scenario: Re-aggregation constituent as an attached parameter
- **WHEN** a query over a month time dimension selects
  `weighted_avg(amount, weight=min(X, partition_by=region))`
- **THEN** the parameter is attached at its `(region)` grain through the same path as a
  source constituent and the query executes with hand-computed values on both engines

#### Scenario: Mixed re-aggregation root combined with a coarser measure
- **WHEN** a query over `[region]` with a month time dimension selects
  `sum(amount * min(X, partition_by=region)) + amount:sum(partition_by=region)`
- **THEN** it executes with hand-computed values on both engines, the coarser term
  broadcast across months exactly as any explicit-grain partitioned measure, with
  unchanged cardinality

#### Scenario: Mixed collapse in filter and order positions
- **WHEN** `sum(amount * last(X))` appears only in a filter or only as an ORDER BY target
- **THEN** the filter types as a measure — pruning result rows with surviving values
  unchanged — and the order sorts by the same value the measure form returns

#### Scenario: The mode axis is not bypassed by an attached re-aggregation
- **WHEN** a query selecting `sum(amount * min(X, partition_by=region))` also groups by
  a dimension the source's home does not determine
- **THEN** under broadcast mode the value repeats across that dimension's cells with
  the self-announcing warning, and under error mode the query fails with the mode
  error — exactly as a mixed source with a plain attached aggregate

#### Scenario: Joined-model row leaf inside a mixed source
- **WHEN** a query rooted at `orders` over `[status]` selects
  `sum(customers.discount * avg(amount, partition_by=status))`
- **THEN** the source is homed at `orders`, the attached average is row-attached per
  order, and each status carries the hand-computed value, by executed values

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
  cardinality, and the plan carries one attach for the banded aggregation with
  the inner `avg` nested inside its producer — no separate top-level attach for
  the inner

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

#### Scenario: A producer-bound mixed aggregation owns its inners
- **WHEN** a mixed aggregation is itself a producer answer (it carries its own
  `partition_by=`, `window=`, or is cross-model) and the plan is inspected
- **THEN** its inner constituents appear only inside that producer's plan,
  never as attaches of the enclosing level

#### Scenario: Partition keys stay visible through an opaque root
- **WHEN** a mixed aggregation declares `partition_by=` on an attach-carrying
  computed dimension of the query
- **THEN** that dimension's own attach is still planned at the enclosing level
  and the aggregation's producer joins back on it, with executed values equal
  to the same query spelled with a plain dimension

#### Scenario: One producer per distinct attached input
- **WHEN** the same attached aggregate appears twice — as a source constituent
  and as a parameter of one aggregation, or as an input of two different
  row-attach aggregations
- **THEN** the plan carries exactly one producer and one attach for it, every
  occurrence substitutes to that attach, total routing holds after
  substitution, and the executed values are correct

#### Scenario: A re-aggregation used both standalone and as a mixed constituent is deferred
- **WHEN** one query selects both `min(X, partition_by=region)` on its own and
  `sum(amount * min(X, partition_by=region))` — the same re-aggregation standalone
  (combined phase) and as a mixed row-level constituent (row phase) — over `[region]`
  and a month time dimension, in measure, filter, or order position
- **THEN** it is no longer deferred — never the former checker error — and executes on
  every engine with the values each use has alone: the
  standalone value is the region minimum broadcast onto every month cell (North 10,
  South 5, NULL where the region has no non-null cell) and the mixed value is the
  per-cell `amount` times that minimum, summed (North 100 / 200 / 300, South 25 / 75);
  the emitted statement carries exactly one producer relation for the re-aggregation
  and one for its nested operand, and adding either measure changes no other value
