## MODIFIED Requirements

### Requirement: Transforms inside dimension expressions
A transform inside a dimension expression SHALL evaluate at the union of its inner aggregates' effective grains — the grain of its containing context — over that grain's domain, exactly as the same expression does as a measure, a filter or an order key (`queries/transforms` › Transforms evaluate over their operand's domain). An inner aggregate's effective grain is its declared `partition_by=` set, plus the query's active time bucket when the aggregate is windowed (`window=`); a `first`/`last` inner aggregate contributes its declared partition set only. Each inner aggregate is computed at its own effective grain and broadcast to the union-grain rows; when all inner aggregates share one grain the union degenerates to that grain (behavior unchanged). The rule is recursive: a nested transform evaluates at the union of its OWN inner aggregates' grains and its result is broadcast into the containing union like any other grained value. Keyword references on the transform (e.g. an explicit `partition_by=`) SHALL resolve against the union grain. A time-ordered transform (e.g. `cumsum`, `lag`, `time_shift`) inside a dimension expression SHALL fail with a clear error when its evaluation grain does not contain its time-ordering key — never duplicated result rows. When a windowed inner aggregate contributes the active time bucket, that synthesized bucket IS the query's bucketed time dimension — one dimension for all grain purposes (union membership, deduplication, attachment keys) — and a mixed-grain transform with a windowed inner aggregate but no resolvable time dimension SHALL fail with the same time-resolution error as windowed measures; single-grain windowed and `first`/`last` transform inputs remain legal.

#### Scenario: Rank of partitions as a bandable dimension
- WHEN a query declares the dimension `rank(sum(revenue, partition_by=region), direction='desc')`
- THEN each row carries its region's rank among all regions by total revenue, and grouping or banding by that rank is legal and correct

#### Scenario: Context grain distinguishes dimension use from measure use
- WHEN `rank(sum(revenue, partition_by=region), direction='desc')` is used once as a dimension and once as a measure in otherwise identical queries
- THEN both forms rank the regions at region grain, each row carrying its region's rank

#### Scenario: Different grains in one transform union and broadcast
- WHEN a dimension expression applies a transform over an arithmetic of two aggregates at different partition grains (e.g. `rank(sum(a, partition_by=region) - sum(b, partition_by=city), direction='desc')`)
- THEN each aggregate is computed at its own declared grain, both are broadcast to the (region, city) union rows, the transform evaluates over exactly those rows, and executed values are correct

#### Scenario: Keyless grain in a mixed transform
- WHEN a dimension expression ranks a share-of-total, e.g. `rank(sum(amount, partition_by=region) / sum(amount, partition_by=[]), direction='desc')`
- THEN the overall total broadcasts to every region row, the ratio and rank evaluate per region, and executed values are correct

#### Scenario: A subset grain computes at its own grain
- WHEN a mixed-grain transform combines an aggregate at the union grain with one at a strictly coarser grain (e.g. `rank(sum(a, partition_by=[region, city]) - sum(a, partition_by=region), direction='desc')`)
- THEN the union-grain aggregate is computed directly at the union grain while the coarser one is computed at its own grain and broadcast, and executed values are correct

#### Scenario: Nested transform evaluates at its own grain
- WHEN a mixed-grain transform contains a nested transform over a strictly coarser grain (e.g. `rank(cumsum(sum(a, partition_by=[region, ordered_at])) - sum(b, partition_by=city), direction='desc')`)
- THEN the inner transform evaluates over its own grain's rows (the cumulative sum accumulates across that grain's time buckets, not across union rows) before broadcasting into the union, and executed values are correct

#### Scenario: Temporal transform without its time axis in the grain fails cleanly
- WHEN a dimension expression applies a time-ordered transform over aggregates whose union grain lacks the transform's time-ordering key (e.g. `cumsum(sum(amount, partition_by=[region, city]))` in a query with a monthly time dimension)
- THEN the query fails with a clear error directing the author to include the time key in `partition_by`, and never returns duplicated rows

#### Scenario: Explicit transform partition over union rows
- WHEN a mixed-grain transform declares `partition_by=` naming a key of the union grain (e.g. `rank(sum(a, partition_by=region) - sum(b, partition_by=city), partition_by=region, direction='desc')`)
- THEN the transform partitions the union-grain rows by the declared key, and executed values are correct

#### Scenario: Transform keyword outside the union grain fails cleanly
- WHEN a mixed-grain transform declares `partition_by=` naming a key not in the union grain
- THEN the query fails with a clear reference error, not an internal producer-slot error

#### Scenario: Union attach is cardinality-neutral on the complete union grain
- WHEN a query runs with and without a mixed-grain transform dimension
- THEN the attach joins on the complete union grain, and both runs return the same rows with identical values in all shared columns

#### Scenario: Same mixed-grain transform as dimension and measure in one query
- WHEN the same mixed-grain transform expression appears both as a dimension and as a measure
- THEN both forms evaluate over the union grain's domain and carry the same value in every row of one result

#### Scenario: Different grains in one transform are deferred, not misgrained
- WHEN a mixed-grain transform's inner aggregates include a `window=` or `first`/`last` aggregation at a different grain than a sibling aggregate
- THEN the query is no longer deferred: each aggregate is computed at its own effective grain (a windowed one contributing the active time bucket) and broadcast to the union rows, returning correct executed values — never a misgrained value and never the former DEV-1835 not-yet-supported error

#### Scenario: A windowed inner aggregate contributes the time bucket to the union
- WHEN a dimension expression applies a transform over `sum(a, window='90d', partition_by=region) - sum(b, partition_by=region)` in a query with a month time dimension
- THEN the union grain is (region, month bucket), the plain region total broadcasts across the region's buckets, and executed values are correct

#### Scenario: First/last inner aggregate mixes with a different-grain sibling
- WHEN a dimension expression applies a transform over `last(a, partition_by=region) - sum(b, partition_by=city)`
- THEN the union grain is (region, city), each value broadcasts from its own grain, and executed values are correct

#### Scenario: Windowed inner aggregate without a resolvable time dimension fails
- WHEN such a transform-in-dimension contains a windowed inner aggregate but the query has no resolvable time dimension
- THEN the query fails with the same clear time-resolution error as windowed measures
