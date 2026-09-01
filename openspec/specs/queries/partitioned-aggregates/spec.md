# queries/partitioned-aggregates Specification

## Purpose
Defines how `partition_by=` aggregations (measures computed at an explicitly declared grain, attached back to the query rows) compose with the rest of the query surface: `window=`, `first`/`last`, transforms, filters, ORDER BY, and other measure kinds.

## Requirements

### Requirement: Partitioned aggregate combined with window=
An aggregation SHALL accept `partition_by=` and `window=` together. The value is the rolling-window aggregate computed at the partition grain, evaluated per time bucket of the query's active time dimension, attached to every query row of the partition.

#### Scenario: Rolling total at a coarser grain
- WHEN a query selects dimensions `[region, city, month(ordered_at)]` and the measure `revenue:sum(partition_by=region, window='90d')`
- THEN every row carries the trailing-90-day region-wide revenue total as of that row's month, identical across cities of the same region and month, and adding the measure changes neither the row count nor any other column's values

#### Scenario: Requires a resolvable time dimension
- WHEN `window=` is combined with `partition_by=` in a query with no resolvable time dimension
- THEN the query fails with the same clear time-dimension resolution error that plain `window=` measures raise

### Requirement: Partitioned first and last aggregations
`first` and `last` aggregations SHALL accept `partition_by=`, returning the value at the earliest/latest ranking timestamp within the partition, attached to every query row of the partition without multiplying rows.

#### Scenario: Region-wide latest value on city rows
- WHEN a query selects dimensions `[region, city]` and the measure `price:last(partition_by=region)`
- THEN every city row of a region shows the price at the region's latest ranking timestamp, and the row count equals the same query without the measure

#### Scenario: Temporal partition keys do not hijack the ranking column
- WHEN the partition set contains a time-bucket dimension and the aggregation does not name an explicit ranking column
- THEN ranking still uses the model's resolved ranking time column, not the partition key

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

### Requirement: Filters referencing partitioned aggregates
Query filters SHALL be able to reference partitioned aggregates. Such predicates apply after attachment: they prune result rows and MUST NOT alter the aggregate values of surviving rows. Each top-level conjunct of a filter routes independently to the earliest scope where all its references resolve; a predicate whose references share no common scope fails with a clear error.

#### Scenario: Keep rows whose partition total qualifies
- WHEN a query over dimensions `[region, city]` filters on `revenue:sum(partition_by=region) > 5000`
- THEN only rows belonging to qualifying regions remain and every remaining value equals the unfiltered query's value for that row

#### Scenario: Conjunction splits by scope
- WHEN one filter string is an AND of a partitioned-aggregate predicate and a row-level predicate
- THEN the results equal the same query with the two predicates given as separate filters

#### Scenario: Mixing with a plain aggregate in one predicate is legal
- WHEN a single predicate combines a partitioned-aggregate reference with a plain aggregate reference (e.g. `revenue:sum(partition_by=region) > 5000 AND revenue:sum > 100`)
- THEN the whole predicate evaluates after aggregation and attachment, and the results are correct by executed values

#### Scenario: No common scope fails closed
- WHEN a single OR predicate mixes a partitioned-aggregate reference with a reference resolvable only before aggregation
- THEN the query fails with a clear error stating the predicate cannot resolve in one scope and must be rewritten so each top-level AND conjunct's references resolve together (an OR across the two scopes cannot be split into separate filters without changing its meaning), not with an internal error

### Requirement: Row and combined attachment coexistence
A query SHALL support partitioned aggregates consumed inside computed dimensions and as measures simultaneously, whether they share the same partition set, use independent partition sets, or are the very same aggregate in both roles.

#### Scenario: Dimension banding and a partitioned measure together
- WHEN a query has a computed dimension banding `amount:sum(partition_by=city)` and the measure `amount:sum(partition_by=region)`
- THEN both the band and the measure are correct by executed values in one result

#### Scenario: Same aggregate in both roles
- WHEN the same partitioned aggregate appears inside a computed dimension and as a directly selected measure
- THEN both values are correct and consistent with each other

#### Scenario: ORDER BY the raw aggregate alongside a computed dimension using it
- WHEN a computed dimension bands `amount:sum(partition_by=city)` and `order` names the raw `amount:sum(partition_by=city)`
- THEN rows sort by the partition-grain value; ordering by the computed dimension's name instead sorts by the banded value; neither form raises an internal placeholder error

### Requirement: Coexistence with other isolated measure kinds
A partitioned aggregate SHALL be usable in the same query as windowed, `first`/`last`, cross-model, and transform measures, with every measure retaining the value it has when queried alone.

#### Scenario: Partitioned plus windowed measure
- WHEN a query selects both `revenue:sum(partition_by=region)` and `revenue:sum(window='90d')`
- THEN each measure's executed values equal its value in a query where it appears alone

#### Scenario: Partitioned plus first/last, cross-model, and transform measures
- WHEN a query combines a partitioned aggregate with a `first`/`last` measure, a cross-model measure, or a transform measure
- THEN all measures are correct by executed values and the row count is unchanged

### Requirement: Attachment preserves cardinality structurally
Attaching a partitioned aggregate MUST never change the query's row count or any other column's values. The planner SHALL verify structurally that the attachment joins on the producer's complete unique key, and that a keyless attachment is provably single-row.

#### Scenario: Adding a partitioned measure is cardinality-neutral
- WHEN any supported query runs with and without an additional partitioned-aggregate measure
- THEN both runs return the same rows and identical values in all shared columns

#### Scenario: Empty partition set attaches the overall total
- WHEN a measure declares `partition_by=[]`
- THEN every row carries the overall total and the row count is unchanged

### Requirement: Producers may require their own intermediate relations
A partitioned aggregate whose computation itself needs intermediate relations (rolling windows, rankings, transform steps) SHALL render correctly, including several such producers in one query, with no name collisions in the generated SQL.

#### Scenario: Two complex producers in one query
- WHEN a query uses two partitioned aggregates whose producers each need internal intermediate relations of the same shape
- THEN the generated SQL is valid on every supported dialect's emission path and executes with correct values

### Requirement: Existing partitioned-aggregate behavior is preserved
All partitioned-aggregate shapes supported before this change SHALL keep byte-identical generated SQL against the committed golden baselines, except divergences individually approved and recorded.

#### Scenario: Golden baselines hold
- WHEN the golden-SQL suites for previously supported partitioned-aggregate shapes run
- THEN every baseline matches byte-for-byte

### Requirement: Temporal transforms compose with partitioned measures
A partitioned measure SHALL be usable in the same query as `time_shift`, `change`, and `change_pct`, producing valid SQL on every supported dialect: the shifted re-aggregation groups only by real query dimensions, and reserved internal placeholder names never reach the emitted statement.

#### Scenario: Partitioned measure with a time-shift measure executes
- WHEN a query selects `amount:sum(partition_by=region)` and `time_shift(amount:sum, periods=-1)` over a month time dimension
- THEN the query executes with correct values for both measures and the emitted SQL contains no reserved placeholder prefix

#### Scenario: Shifted re-aggregation grain excludes the attached value
- WHEN the shifted comparison period is computed for such a query
- THEN it groups by the query's dimensions and shifted time bucket only, and joins back on exactly that grain

### Requirement: Grain-union broadcasting across consumption contexts
An arithmetic expression combining aggregates at different declared partition grains SHALL be well-defined at the union of those grains: each aggregate is computed at its own declared grain and broadcast over the grain keys it lacks. The expression SHALL be consumable at any grain refining the union — as a measure, the query grain — with every operand broadcast to the consuming rows; combining aggregates at different grains is never, by itself, an error. A transform over such an expression used as a measure SHALL evaluate at the query grain over the broadcast operands. Filter conjuncts referencing such expressions apply after attachment and MUST NOT alter surviving rows' values.

#### Scenario: Mixed-grain arithmetic as a measure
- WHEN a query over dimensions `[region, city]` selects the measure `a:sum(partition_by=region) - b:sum(partition_by=city)`
- THEN each row's value is its region total minus its city total, by executed values

#### Scenario: Same-grain partitioned arithmetic as a measure
- WHEN a query selects the measure `a:sum(partition_by=region) - b:sum(partition_by=region)` (the degenerate union of two identical grains)
- THEN each row's value is the difference of its two broadcast region totals, by executed values

#### Scenario: Plain and partitioned aggregates mix in one expression
- WHEN a query selects the measure `amount:sum - amount:sum(partition_by=region)`
- THEN each row's value is its query-grain total minus its broadcast region total, by executed values

#### Scenario: Transform over mixed-grain arithmetic as a measure
- WHEN a query selects the measure `rank(a:sum(partition_by=region) - b:sum(partition_by=city))`
- THEN result rows are ranked at the query grain by the broadcast difference, and adding the measure changes no other column's values

#### Scenario: Filter over mixed-grain arithmetic
- WHEN a query filters on `a:sum(partition_by=region) - b:sum(partition_by=city) > 0`
- THEN only qualifying rows remain and every surviving value equals the unfiltered query's value for that row

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
