# queries/partitioned-aggregates — Stage 2 delta

## MODIFIED Requirements

### Requirement: Partitioned aggregates nested inside transforms
A transform SHALL accept a partitioned aggregate as its input when used as a measure — rank-family transforms and temporal transforms (`time_shift`, `change`, `change_pct`, `lag`, `lead`, `cumsum`, `consecutive_periods`) alike. The transform evaluates at the query grain over the attached partition-grain value (the grain of its containing context) and MUST never fail with an internal error.

#### Scenario: Running total of partition-grain values
- WHEN a query selects dimensions `[region, city, month(ordered_at)]` and the measure `cumsum(revenue:sum(partition_by=[region, month(ordered_at)]))`
- THEN each row's value is the cumulative sum across months, within the row's non-time dimensions, of the attached region-month totals, verified by executed values

#### Scenario: Ranking result rows by an attached total
- WHEN a query selects the measure `rank(revenue:sum(partition_by=region))`
- THEN result rows are ranked by their attached region total at the query grain

#### Scenario: Change over a partitioned aggregate executes
- WHEN a query selects `change(amount:sum(partition_by=region))` or `change_pct(amount:sum(partition_by=region))` over a month time dimension
- THEN the query executes with the hand-computed bucket-over-previous-bucket difference (or ratio) of the attached value, instead of failing with an internal rendering error

## ADDED Requirements

### Requirement: Bare windowed and ranked measures compose as full-grain partitioned aggregates
A windowed aggregation without `partition_by=` and a `first`/`last` aggregation without `partition_by=` SHALL behave as partitioned at the query's full projected grain: they compose with transforms, arithmetic/composite/scalar expressions, and filters exactly as partitioned aggregates do, while keeping their established public result keys, aliases, and executed values. Explicit `partition_by=` on the same aggregation remains a strict generalization; the bare form and an explicit form at the same effective grain are equivalent.

#### Scenario: Transform over a bare windowed measure
- WHEN a query selects `cumsum(amount:sum(window='90d'))` over a month time dimension
- THEN the running total of the rolling window executes with hand-computed values

#### Scenario: Bare windowed measure inside arithmetic
- WHEN a query selects `amount:sum(window='90d') / amount:sum`
- THEN the composite evaluates per result row over the attached rolling total and the plain aggregate, correct by executed values

#### Scenario: Filter-only reference to a bare windowed measure
- WHEN a query filters on `amount:sum(window='90d') > 20` without selecting that measure
- THEN qualifying rows survive with unchanged values and the emitted SQL contains no leaked internal names

#### Scenario: One predicate mixing a bare windowed and a plain aggregate
- WHEN a single filter predicate combines `amount:sum(window='90d')` with `amount:sum`
- THEN the whole predicate evaluates after attachment, correct by executed values

#### Scenario: Temporal transform over a bare first/last measure
- WHEN a query selects `time_shift(amount:last, -1)` over a month time dimension
- THEN each row carries the previous bucket's last value, correct by executed values

#### Scenario: Bare and explicit partition twins are equivalent
- WHEN one query selects a bare windowed (or `first`/`last`) measure and another declares the same aggregation with `partition_by=` naming the full projected grain
- THEN both return identical executed values and render one shared producer relation when combined in a single query

#### Scenario: Migrated families keep their executed values
- WHEN previously supported bare windowed and `first`/`last` queries run after the migration
- THEN executed values are identical to before; generated-SQL divergences are individually approved and recorded

### Requirement: Structurally identical producers render once
When several consumed aggregates resolve to the same producer — same source, same effective grain, same normalized aggregate set including per-measure filters and ranking context — the query SHALL compute that producer once and attach it at every consuming position, across both attach phases (dimension and measure roles).

#### Scenario: Same aggregate in both roles shares one producer
- WHEN the same partitioned aggregate appears inside a computed dimension and as a selected measure
- THEN the emitted SQL contains a single producer relation for it, with both roles' values correct

#### Scenario: Different producer inputs stay separate
- WHEN two aggregations differ in window duration, in a measure-level filter, or in an explicit ranking column
- THEN they render as separate producers and each value is correct

### Requirement: Measure-local filters stay inside the producer
An aggregation's own filter SHALL restrict only the rows aggregated by its producer, never the query's result rows; query- and model-level row filters SHALL apply consistently to both the query and the producer.

#### Scenario: A filtered measure is cardinality-neutral
- WHEN a windowed or `first`/`last` measure carrying its own filter is added beside unfiltered measures
- THEN the row count and every companion value are unchanged, and only the filtered measure's value reflects the filter
