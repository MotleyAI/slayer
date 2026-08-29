## Purpose

Defines computed (expression) dimensions: which expressions are legal as dimensions, how expressions containing explicitly-grained aggregates and transforms over them behave, and the error surface for shapes that are not (yet) expressible.

## ADDED Requirements

### Requirement: Measure-dimension symmetry with grain self-containment
Any measure-legal expression SHALL be legal as a computed dimension provided it is grain-self-contained: every aggregate in it is local to the query's source (not a cross-join source) and carries an explicit `partition_by=`, and every transform in it applies within such an explicitly-grained subexpression. Once declared, a computed dimension behaves everywhere as a plain dimension: it can be grouped by, banded, filtered on, ordered by, and used as a transform partition.

#### Scenario: Banded partitioned aggregate as a dimension
- WHEN a query declares the dimension `CASE WHEN amount:sum(partition_by=city) > 5000 THEN 'high' ELSE 'low' END`
- THEN rows group by the band, measures aggregate within each band, and executed values are correct

#### Scenario: Expression over two different partition sets
- WHEN a dimension expression combines `x:sum(partition_by=region)` and `y:sum(partition_by=country)` arithmetically
- THEN each aggregate is computed at its own declared grain and the expression is evaluated per row over the two attached values

### Requirement: Transforms inside dimension expressions
A transform whose input is an explicitly-grained aggregate SHALL be legal inside a dimension expression and SHALL evaluate at that input's declared grain (the grain of its containing context), unlike the same expression used as a measure, which evaluates at the query grain.

#### Scenario: Rank of partitions as a bandable dimension
- WHEN a query declares the dimension `rank(revenue:sum(partition_by=region))`
- THEN each row carries its region's rank among all regions by total revenue, and grouping or banding by that rank is legal and correct

#### Scenario: Context grain distinguishes dimension use from measure use
- WHEN `rank(revenue:sum(partition_by=region))` is used once as a dimension and once as a measure in otherwise identical queries
- THEN the dimension form ranks regions at region grain while the measure form ranks result rows at query grain

### Requirement: First and last inside dimension expressions
`first`/`last` aggregations with `partition_by=` SHALL be legal inside dimension expressions.

#### Scenario: Latest timestamp per partition as a dimension
- WHEN a query declares the dimension `ordered_at:last(partition_by=region)`
- THEN every row carries its region's latest order timestamp and rows group by it correctly

### Requirement: Windowed aggregations inside dimension expressions
`window=` aggregations with `partition_by=` SHALL be legal inside dimension expressions, evaluated per the partition keys plus the query's active time bucket; the time bucket need not itself be a selected dimension.

#### Scenario: Rolling partition total as a dimension
- WHEN a query with a resolvable time dimension declares a dimension banding `revenue:sum(window='90d', partition_by=region)`
- THEN each row is banded by its region's trailing-90-day total as of the row's time bucket

#### Scenario: Fails cleanly without a time dimension
- WHEN such a dimension is declared in a query with no resolvable time dimension
- THEN the query fails with the same clear time-resolution error as windowed measures

### Requirement: Dimension expression error surface
Expressions that are not grain-self-contained SHALL fail with clear errors naming the offending construct: a bare aggregate without `partition_by=`, an aggregate over another attached aggregate value, and a cross-model aggregate source inside a dimension expression.

#### Scenario: Bare aggregate in a dimension is rejected
- WHEN a dimension expression contains an aggregate with no `partition_by=`
- THEN the query fails with an error stating that aggregates in dimension expressions must declare `partition_by=`

#### Scenario: Aggregate over an attached value is rejected
- WHEN a dimension expression aggregates over a subexpression that itself contains a partitioned aggregate
- THEN the query fails with a clear not-yet-supported error, not an internal error

#### Scenario: Cross-model aggregate source is rejected
- WHEN a dimension expression contains an aggregate whose source crosses a join path
- THEN the query fails with a clear not-yet-supported error identifying the joined source

### Requirement: Computed dimensions cross stage boundaries as plain columns
A computed dimension derived from aggregation and banding SHALL be consumable by downstream query stages exactly like a stored column, and internal placeholder names MUST never appear in public schemas, response metadata, or emitted SQL column names.

#### Scenario: Downstream stage consumes a regrouped dimension
- WHEN a multi-stage query's earlier stage produces an aggregation-banded dimension and a later stage selects, filters on, orders by, and re-aggregates over it
- THEN the later stage behaves as if the column were stored, with correct executed values

#### Scenario: No internal names leak
- WHEN any query using computed dimensions over aggregates runs
- THEN result keys, stage schemas, and emitted SQL contain no internal placeholder prefixes
