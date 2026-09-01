## Purpose

Defines how `partition_by=` aggregations (measures computed at an explicitly declared grain, attached back to the query rows) compose with the rest of the query surface: `window=`, `first`/`last`, transforms, filters, ORDER BY, and other measure kinds.

## ADDED Requirements

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
A transform (e.g. `cumsum`, `rank`) SHALL accept a partitioned aggregate as its input when used as a measure. The transform evaluates at the query grain over the attached partition-grain value (the grain of its containing context).

#### Scenario: Running total of partition-grain values
- WHEN a query selects dimensions `[region, city, month(ordered_at)]` and the measure `cumsum(revenue:sum(partition_by=[region, month(ordered_at)]))`
- THEN each row's value is the cumulative sum across months, within the row's non-time dimensions, of the attached region-month totals, verified by executed values

#### Scenario: Ranking result rows by an attached total
- WHEN a query selects the measure `rank(revenue:sum(partition_by=region))`
- THEN result rows are ranked by their attached region total at the query grain

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
