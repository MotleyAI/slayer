# queries/computed-dimensions Specification

## Purpose
Defines computed (expression) dimensions: which expressions are legal as dimensions, how expressions containing explicitly-grained aggregates and transforms over them behave, and the error surface for shapes that are not (yet) expressible.

## Requirements

### Requirement: Measure-dimension symmetry with grain self-containment
Any measure-legal expression SHALL be legal as a computed dimension provided it is grain-self-contained: every aggregate in it is local to the query's source (not a cross-join source) and carries an explicit `partition_by=`, and every transform in it applies within such an explicitly-grained subexpression. Once declared, a computed dimension behaves everywhere as a plain dimension: it can be grouped by, banded, filtered on, ordered by, and used as a transform partition.

#### Scenario: Banded partitioned aggregate as a dimension
- WHEN a query declares the dimension `CASE WHEN amount:sum(partition_by=city) > 5000 THEN 'high' ELSE 'low' END`
- THEN rows group by the band, measures aggregate within each band, and executed values are correct

#### Scenario: Expression over two different partition sets
- WHEN a dimension expression combines `x:sum(partition_by=region)` and `y:sum(partition_by=country)` arithmetically
- THEN each aggregate is computed at its own declared grain and the expression is evaluated per row over the two attached values

### Requirement: Transforms inside dimension expressions
A transform inside a dimension expression SHALL evaluate at the union of its inner aggregates' declared partition grains — the grain of its containing context — unlike the same expression used as a measure, which evaluates at the query grain. Each inner aggregate is computed at its own declared grain and broadcast to the union-grain rows; when all inner aggregates share one grain the union degenerates to that grain (behavior unchanged). The rule is recursive: a nested transform evaluates at the union of its OWN inner aggregates' grains and its result is broadcast into the containing union like any other grained value. Keyword references on the transform (e.g. an explicit `partition_by=`) SHALL resolve against the union grain. A time-ordered transform (e.g. `cumsum`, `lag`, `time_shift`) inside a dimension expression SHALL fail with a clear error when its evaluation grain does not contain its time-ordering key — never duplicated result rows. A mixed-grain transform any of whose inner aggregates carries `window=` or is a `first`/`last` aggregation SHALL fail with a clear not-yet-supported error naming the combination (DEV-1835) — never silently misgrained values; single-grain windowed and `first`/`last` transform inputs remain legal.

#### Scenario: Rank of partitions as a bandable dimension
- WHEN a query declares the dimension `rank(revenue:sum(partition_by=region))`
- THEN each row carries its region's rank among all regions by total revenue, and grouping or banding by that rank is legal and correct

#### Scenario: Context grain distinguishes dimension use from measure use
- WHEN `rank(revenue:sum(partition_by=region))` is used once as a dimension and once as a measure in otherwise identical queries
- THEN the dimension form ranks regions at region grain while the measure form ranks result rows at query grain

#### Scenario: Different grains in one transform union and broadcast
- WHEN a dimension expression applies a transform over an arithmetic of two aggregates at different partition grains (e.g. `rank(a:sum(partition_by=region) - b:sum(partition_by=city))`)
- THEN each aggregate is computed at its own declared grain, both are broadcast to the (region, city) union rows, the transform evaluates over exactly those rows, and executed values are correct

#### Scenario: Keyless grain in a mixed transform
- WHEN a dimension expression ranks a share-of-total, e.g. `rank(amount:sum(partition_by=region) / amount:sum(partition_by=[]))`
- THEN the overall total broadcasts to every region row, the ratio and rank evaluate per region, and executed values are correct

#### Scenario: A subset grain computes at its own grain
- WHEN a mixed-grain transform combines an aggregate at the union grain with one at a strictly coarser grain (e.g. `rank(a:sum(partition_by=[region, city]) - a:sum(partition_by=region))`)
- THEN the union-grain aggregate is computed directly at the union grain while the coarser one is computed at its own grain and broadcast, and executed values are correct

#### Scenario: Nested transform evaluates at its own grain
- WHEN a mixed-grain transform contains a nested transform over a strictly coarser grain (e.g. `rank(cumsum(a:sum(partition_by=[region, ordered_at])) - b:sum(partition_by=city))`)
- THEN the inner transform evaluates over its own grain's rows (the cumulative sum accumulates across that grain's time buckets, not across union rows) before broadcasting into the union, and executed values are correct

#### Scenario: Temporal transform without its time axis in the grain fails cleanly
- WHEN a dimension expression applies a time-ordered transform over aggregates whose union grain lacks the transform's time-ordering key (e.g. `cumsum(amount:sum(partition_by=[region, city]))` in a query with a monthly time dimension)
- THEN the query fails with a clear error directing the author to include the time key in `partition_by`, and never returns duplicated rows

#### Scenario: Explicit transform partition over union rows
- WHEN a mixed-grain transform declares `partition_by=` naming a key of the union grain (e.g. `rank(a:sum(partition_by=region) - b:sum(partition_by=city), partition_by=region)`)
- THEN the transform partitions the union-grain rows by the declared key, and executed values are correct

#### Scenario: Transform keyword outside the union grain fails cleanly
- WHEN a mixed-grain transform declares `partition_by=` naming a key not in the union grain
- THEN the query fails with a clear reference error, not an internal producer-slot error

#### Scenario: Union attach is cardinality-neutral on the complete union grain
- WHEN a query runs with and without a mixed-grain transform dimension
- THEN the attach joins on the complete union grain, and both runs return the same rows with identical values in all shared columns

#### Scenario: Same mixed-grain transform as dimension and measure in one query
- WHEN the same mixed-grain transform expression appears both as a dimension and as a measure
- THEN the dimension form evaluates at the union grain, the measure form at the query grain, and both are correct in one result

#### Scenario: Different grains in one transform are deferred, not misgrained
- WHEN a mixed-grain transform's inner aggregates include a `window=` or `first`/`last` aggregation at a different grain than a sibling aggregate — the one different-grain combination still deferred
- THEN the query fails with a clear not-yet-supported error naming the combination (DEV-1835), rather than evaluating any aggregate at the wrong grain

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

### Requirement: Computed dimensions coexist with transform measures
A grain-self-contained computed dimension (one whose aggregates all carry explicit `partition_by=`) SHALL be legal in the same query as transform measures — `time_shift`, `change`, `change_pct`, `cumsum`, `lag`, `lead`, `consecutive_periods`, and rank-family transforms of a measure — alone and alongside plain and partitioned measures, with correct executed values and unchanged result cardinality.

#### Scenario: Banded dimension with a time-shift measure
- WHEN a query groups by a dimension banding `amount:sum(partition_by=city)` and selects `time_shift(amount:sum, periods=-1)` over a month time dimension
- THEN each row carries the previous month's total for its (band, other-dimension) group, and the band values match the same query without the transform measure

#### Scenario: Banded dimension with change and change_pct
- WHEN the same banded dimension is combined with `change(amount:sum)` or `change_pct(amount:sum)`
- THEN the derived values equal the hand-computed difference (or ratio) between the group's bucket and its previous bucket

#### Scenario: Banded dimension with a running total
- WHEN the same banded dimension is combined with `cumsum(amount:sum)`
- THEN each row carries the running total accumulated within its (band, other-dimension) group across time buckets

#### Scenario: Bare partitioned aggregate as a dimension with a transform measure
- WHEN a query groups directly by `amount:sum(partition_by=city)` as a dimension and selects a transform measure
- THEN the query executes with correct values for both

#### Scenario: Transform-root dimension with a transform measure
- WHEN a query groups by `rank(amount:sum(partition_by=city))` as a dimension and selects a transform measure
- THEN the producer-grain rank and the query-grain transform are both correct in one result

#### Scenario: Alongside a partitioned measure
- WHEN a computed dimension over a partitioned aggregate, a partitioned measure (`partition_by=`), and a transform measure appear in one query
- THEN all three are correct by executed values, each equal to its value when queried alone

#### Scenario: Adding a transform measure is cardinality-neutral
- WHEN a query with an aggregation-derived dimension (banded, bare, or transform-root) runs with and without an additional transform measure
- THEN both runs return the same rows and identical values in all shared columns

### Requirement: Transform grain includes every dimension
The grain at which a transform measure evaluates SHALL include every projected dimension of the query — plain columns, derived columns, computed expressions, and aggregation-derived dimensions alike — and SHALL exclude time buckets (the transform's ordering axis) and attached partitioned-measure values. Rank-family transforms keep their default of ranking across the whole result set; an explicit `partition_by=` on a transform always takes precedence.

#### Scenario: Running total partitions by a computed dimension
- WHEN a query groups by a computed expression dimension and a plain dimension and selects `cumsum(amount:sum)` over a time dimension
- THEN the running total accumulates separately per (expression value, plain value) group, not across the expression dimension's groups

#### Scenario: Attached partitioned-measure value never widens the grain
- WHEN a query selects a partitioned measure alongside any transform measure
- THEN the attached value participates in neither the transform's partition nor a shifted re-aggregation's grouping, and the transform's value is unchanged by adding the partitioned measure

#### Scenario: Explicit partition_by on the transform wins
- WHEN a transform declares an explicit `partition_by=`
- THEN the transform partitions exactly by the declared keys regardless of the query's dimensions

### Requirement: Filters compose across attach and transform phases
When a query carries an aggregation-derived dimension, top-level AND conjuncts of a single filter string SHALL split and route independently, each to its own phase: a predicate over the computed dimension applies at its established placement, a predicate over a transformed value applies to the final result, and a plain row predicate copies into the producer and consumer. A single predicate whose operands share no scope (e.g. joined by OR across phases) SHALL fail with the established split-the-filter error.

#### Scenario: Filter on a shifted value with a banded dimension
- WHEN a query with a banded dimension filters on `change(amount:sum) > 0`
- THEN only groups whose value grew survive, band values are unchanged, and no internal placeholder appears in the emitted SQL

#### Scenario: Conjunction splits across phases
- WHEN one filter string ANDs a predicate on the banded dimension with a predicate on a shifted value
- THEN each conjunct routes to its own phase and the result equals applying the two filters separately

#### Scenario: Conjunction with a row-level predicate splits too
- WHEN one filter string ANDs a predicate on the banded dimension with a plain row predicate (`band == 1 and status == 'ok'`)
- THEN the row conjunct copies into the producer and consumer, the band conjunct applies at the final stage, and the result equals applying the two filters separately

#### Scenario: Unsplittable mixed predicate still fails closed
- WHEN one filter string ORs a predicate on the banded dimension with a predicate from another phase
- THEN the query fails with the established split-the-filter directive

### Requirement: Transform coexistence deferrals fail closed
An aggregation-derived dimension combined with a windowed (`window=` without `partition_by=`), ranked (`first`/`last` without `partition_by=`), or cross-model measure, or nested where the query must render as a single CTE body, SHALL fail with a clear not-yet-supported error naming the unsupported combination — never with wrong numbers or invalid SQL.

#### Scenario: Windowed and ranked measures still guarded
- WHEN a query combines an aggregation-derived dimension with a bare `window=` or bare `first`/`last` measure
- THEN the query fails with the exact windowed/ranked-coexistence error

#### Scenario: Cross-model measures still guarded
- WHEN a query combines an aggregation-derived dimension with a cross-model measure
- THEN the query fails with the exact cross-model-coexistence error
