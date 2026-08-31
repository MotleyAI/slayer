# queries/computed-dimensions — Stage 2 delta

## MODIFIED Requirements

### Requirement: Transforms inside dimension expressions
A transform inside a dimension expression SHALL evaluate at the union of its inner aggregates' effective grains — the grain of its containing context — unlike the same expression used as a measure, which evaluates at the query grain. An inner aggregate's effective grain is its declared `partition_by=` set, plus the query's active time bucket when the aggregate is windowed (`window=`); a `first`/`last` inner aggregate contributes its declared partition set only. Each inner aggregate is computed at its own effective grain and broadcast to the union-grain rows; when all inner aggregates share one grain the union degenerates to that grain (behavior unchanged). The rule is recursive: a nested transform evaluates at the union of its OWN inner aggregates' grains and its result is broadcast into the containing union like any other grained value. Keyword references on the transform (e.g. an explicit `partition_by=`) SHALL resolve against the union grain. A time-ordered transform (e.g. `cumsum`, `lag`, `time_shift`) inside a dimension expression SHALL fail with a clear error when its evaluation grain does not contain its time-ordering key — never duplicated result rows. When a windowed inner aggregate contributes the active time bucket, that synthesized bucket IS the query's bucketed time dimension — one dimension for all grain purposes (union membership, deduplication, attachment keys) — and a mixed-grain transform with a windowed inner aggregate but no resolvable time dimension SHALL fail with the same time-resolution error as windowed measures; single-grain windowed and `first`/`last` transform inputs remain legal.

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
- WHEN a mixed-grain transform's inner aggregates include a `window=` or `first`/`last` aggregation at a different grain than a sibling aggregate
- THEN the query is no longer deferred: each aggregate is computed at its own effective grain (a windowed one contributing the active time bucket) and broadcast to the union rows, returning correct executed values — never a misgrained value and never the former DEV-1835 not-yet-supported error

#### Scenario: A windowed inner aggregate contributes the time bucket to the union
- WHEN a dimension expression applies a transform over `a:sum(window='90d', partition_by=region) - b:sum(partition_by=region)` in a query with a month time dimension
- THEN the union grain is (region, month bucket), the plain region total broadcasts across the region's buckets, and executed values are correct

#### Scenario: First/last inner aggregate mixes with a different-grain sibling
- WHEN a dimension expression applies a transform over `a:last(partition_by=region) - b:sum(partition_by=city)`
- THEN the union grain is (region, city), each value broadcasts from its own grain, and executed values are correct

#### Scenario: Windowed inner aggregate without a resolvable time dimension fails
- WHEN such a transform-in-dimension contains a windowed inner aggregate but the query has no resolvable time dimension
- THEN the query fails with the same clear time-resolution error as windowed measures

## REMOVED Requirements

### Requirement: Transform coexistence deferrals fail closed
**Reason**: The windowed/ranked coexistence deferral lifts in this change (the migrated families compose on the regroup primitive), so the requirement's scope no longer matches its name or scenarios.
**Migration**: The still-deferred combinations continue under the narrower "Remaining coexistence deferrals fail closed" requirement added below; the windowed/ranked combination is specified as supported under "Aggregation-derived dimensions coexist with windowed and ranked measures".

## ADDED Requirements

### Requirement: Remaining coexistence deferrals fail closed
An aggregation-derived dimension combined with a cross-model measure, or nested where the query must render as a single CTE body, SHALL fail with a clear not-yet-supported error naming the unsupported combination — never with wrong numbers or invalid SQL.

#### Scenario: Cross-model measures still guarded
- WHEN a query combines an aggregation-derived dimension with a cross-model measure
- THEN the query fails with the exact cross-model-coexistence error

#### Scenario: The lifted windowed/ranked guard leaves no residue
- WHEN the package sources are scanned for the former windowed/ranked-coexistence error
- THEN no reference to it remains

### Requirement: Aggregation-derived dimensions coexist with windowed and ranked measures
An aggregation-derived dimension (banded, bare partitioned aggregate, or transform-root) SHALL be legal in the same query as bare windowed (`window=` without `partition_by=`) and bare `first`/`last` measures, with correct executed values, unchanged result cardinality, and each measure equal to its value when queried alone.

#### Scenario: Banded dimension with a bare windowed measure
- WHEN a query groups by a dimension banding `amount:sum(partition_by=city)` and selects `amount:sum(window='1y')` over a month time dimension
- THEN both the band and the rolling total are correct by executed values in one result

#### Scenario: Bare partitioned aggregate as a dimension with a bare last measure
- WHEN a query groups directly by `amount:sum(partition_by=city)` as a dimension and selects `amount:last`
- THEN the query executes with correct values for both

#### Scenario: Transform-root dimension with a bare windowed or ranked measure
- WHEN a query groups by `rank(amount:sum(partition_by=city))` as a dimension and selects a bare windowed or bare `first`/`last` measure
- THEN the producer-grain rank and the measure are both correct in one result

#### Scenario: Adding a bare windowed or ranked measure is cardinality-neutral
- WHEN a query with an aggregation-derived dimension runs with and without an additional bare windowed or `first`/`last` measure
- THEN both runs return the same rows and identical values in all shared columns

#### Scenario: A dual-role aggregate coexists with a bare windowed measure
- WHEN the same partitioned aggregate appears inside a computed dimension and as a selected measure, alongside a bare windowed measure
- THEN all three values are correct and the dimension's grain treatment of the shared aggregate is unaffected by its measure role

### Requirement: Every dimension kind enters a windowed or ranked grain
A bare windowed or `first`/`last` measure SHALL compose with every legal dimension kind — plain columns, derived columns, scalar-expression computed dimensions, and aggregation-derived dimensions — with the measure evaluated at the full projected grain of the query.

#### Scenario: Scalar-expression dimension with a bare windowed measure
- WHEN a query groups by `lower(city)` and selects `amount:sum(window='1y')` over a month time dimension
- THEN each (expression value, month) group carries its rolling total, correct by executed values

#### Scenario: Scalar-expression dimension with a bare last measure
- WHEN a query groups by `lower(city)` and selects `amount:last`
- THEN each expression group carries its value at the latest ranking timestamp, correct by executed values
