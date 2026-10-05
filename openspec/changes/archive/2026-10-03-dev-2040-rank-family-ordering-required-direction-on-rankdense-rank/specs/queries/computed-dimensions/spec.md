## MODIFIED Requirements

### Requirement: Measure-dimension symmetry with grain self-containment
Any measure-legal expression SHALL be legal as a computed dimension provided it is grain-self-contained: every aggregate in it carries an explicit `partition_by=` whose keys are attributable from that aggregate's root (local or cross-model alike, over provably many-to-one join hops), and every transform in it applies within such an explicitly-grained subexpression. Once declared, a computed dimension behaves everywhere as a plain dimension: it can be grouped by, banded, filtered on, ordered by, and used as a transform partition.

#### Scenario: Banded partitioned aggregate as a dimension
- WHEN a query declares the dimension `CASE WHEN amount:sum(partition_by=city) > 5000 THEN 'high' ELSE 'low' END`
- THEN rows group by the band, measures aggregate within each band, and executed values are correct

#### Scenario: Expression over two different partition sets
- WHEN a dimension expression combines `x:sum(partition_by=region)` and `y:sum(partition_by=country)` arithmetically
- THEN each aggregate is computed at its own declared grain and the expression is evaluated per row over the two attached values

#### Scenario: Cross-model aggregate source in a dimension expression
- WHEN a dimension expression bands an aggregate whose source crosses a join (e.g. `customers.spend:sum(partition_by=<customer-level dimension>)`)
- THEN rows group by the band with correct executed values and unchanged cardinality

#### Scenario: Used as a transform partition
- WHEN a query declares the computed dimension `ureg` = `upper(region)` and selects `rank(sum(amount), partition_by=ureg, direction='desc')`
- THEN the transform partitions by the dimension's value exactly as `sum(amount, partition_by=ureg)` would, in the measure, aggregation-parameter, filter, order and computed-dimension positions (values per `queries/partitioned-aggregates` › Transform partition keys bind like aggregate partition keys)

### Requirement: Transforms inside dimension expressions
A transform inside a dimension expression SHALL evaluate at the union of its inner aggregates' effective grains — the grain of its containing context — unlike the same expression used as a measure, which evaluates at the query grain. An inner aggregate's effective grain is its declared `partition_by=` set, plus the query's active time bucket when the aggregate is windowed (`window=`); a `first`/`last` inner aggregate contributes its declared partition set only. Each inner aggregate is computed at its own effective grain and broadcast to the union-grain rows; when all inner aggregates share one grain the union degenerates to that grain (behavior unchanged). The rule is recursive: a nested transform evaluates at the union of its OWN inner aggregates' grains and its result is broadcast into the containing union like any other grained value. Keyword references on the transform (e.g. an explicit `partition_by=`) SHALL resolve against the union grain. A time-ordered transform (e.g. `cumsum`, `lag`, `time_shift`) inside a dimension expression SHALL fail with a clear error when its evaluation grain does not contain its time-ordering key — never duplicated result rows. When a windowed inner aggregate contributes the active time bucket, that synthesized bucket IS the query's bucketed time dimension — one dimension for all grain purposes (union membership, deduplication, attachment keys) — and a mixed-grain transform with a windowed inner aggregate but no resolvable time dimension SHALL fail with the same time-resolution error as windowed measures; single-grain windowed and `first`/`last` transform inputs remain legal.

#### Scenario: Rank of partitions as a bandable dimension
- WHEN a query declares the dimension `rank(revenue:sum(partition_by=region), direction='desc')`
- THEN each row carries its region's rank among all regions by total revenue, and grouping or banding by that rank is legal and correct

#### Scenario: Context grain distinguishes dimension use from measure use
- WHEN `rank(revenue:sum(partition_by=region), direction='desc')` is used once as a dimension and once as a measure in otherwise identical queries
- THEN the dimension form ranks regions at region grain while the measure form ranks result rows at query grain

#### Scenario: Different grains in one transform union and broadcast
- WHEN a dimension expression applies a transform over an arithmetic of two aggregates at different partition grains (e.g. `rank(a:sum(partition_by=region) - b:sum(partition_by=city), direction='desc')`)
- THEN each aggregate is computed at its own declared grain, both are broadcast to the (region, city) union rows, the transform evaluates over exactly those rows, and executed values are correct

#### Scenario: Keyless grain in a mixed transform
- WHEN a dimension expression ranks a share-of-total, e.g. `rank(amount:sum(partition_by=region) / amount:sum(partition_by=[]), direction='desc')`
- THEN the overall total broadcasts to every region row, the ratio and rank evaluate per region, and executed values are correct

#### Scenario: A subset grain computes at its own grain
- WHEN a mixed-grain transform combines an aggregate at the union grain with one at a strictly coarser grain (e.g. `rank(a:sum(partition_by=[region, city]) - a:sum(partition_by=region), direction='desc')`)
- THEN the union-grain aggregate is computed directly at the union grain while the coarser one is computed at its own grain and broadcast, and executed values are correct

#### Scenario: Nested transform evaluates at its own grain
- WHEN a mixed-grain transform contains a nested transform over a strictly coarser grain (e.g. `rank(cumsum(a:sum(partition_by=[region, ordered_at])) - b:sum(partition_by=city), direction='desc')`)
- THEN the inner transform evaluates over its own grain's rows (the cumulative sum accumulates across that grain's time buckets, not across union rows) before broadcasting into the union, and executed values are correct

#### Scenario: Temporal transform without its time axis in the grain fails cleanly
- WHEN a dimension expression applies a time-ordered transform over aggregates whose union grain lacks the transform's time-ordering key (e.g. `cumsum(amount:sum(partition_by=[region, city]))` in a query with a monthly time dimension)
- THEN the query fails with a clear error directing the author to include the time key in `partition_by`, and never returns duplicated rows

#### Scenario: Explicit transform partition over union rows
- WHEN a mixed-grain transform declares `partition_by=` naming a key of the union grain (e.g. `rank(a:sum(partition_by=region) - b:sum(partition_by=city), partition_by=region, direction='desc')`)
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
- WHEN a query groups by `rank(amount:sum(partition_by=city), direction='desc')` as a dimension and selects a transform measure
- THEN the producer-grain rank and the query-grain transform are both correct in one result

#### Scenario: Alongside a partitioned measure
- WHEN a computed dimension over a partitioned aggregate, a partitioned measure (`partition_by=`), and a transform measure appear in one query
- THEN all three are correct by executed values, each equal to its value when queried alone

#### Scenario: Adding a transform measure is cardinality-neutral
- WHEN a query with an aggregation-derived dimension (banded, bare, or transform-root) runs with and without an additional transform measure
- THEN both runs return the same rows and identical values in all shared columns

### Requirement: Aggregation-derived dimensions coexist with windowed and ranked measures
An aggregation-derived dimension (banded, bare partitioned aggregate, or transform-root) SHALL be legal in the same query as bare windowed (`window=` without `partition_by=`) and bare `first`/`last` measures, with correct executed values, unchanged result cardinality, and each measure equal to its value when queried alone.

#### Scenario: Banded dimension with a bare windowed measure
- WHEN a query groups by a dimension banding `amount:sum(partition_by=city)` and selects `amount:sum(window='1y')` over a month time dimension
- THEN both the band and the rolling total are correct by executed values in one result

#### Scenario: Bare partitioned aggregate as a dimension with a bare last measure
- WHEN a query groups directly by `amount:sum(partition_by=city)` as a dimension and selects `amount:last`
- THEN the query executes with correct values for both

#### Scenario: Transform-root dimension with a bare windowed or ranked measure
- WHEN a query groups by `rank(amount:sum(partition_by=city), direction='desc')` as a dimension and selects a bare windowed or bare `first`/`last` measure
- THEN the producer-grain rank and the measure are both correct in one result

#### Scenario: Adding a bare windowed or ranked measure is cardinality-neutral
- WHEN a query with an aggregation-derived dimension runs with and without an additional bare windowed or `first`/`last` measure
- THEN both runs return the same rows and identical values in all shared columns

#### Scenario: A dual-role aggregate coexists with a bare windowed measure
- WHEN the same partitioned aggregate appears inside a computed dimension and as a selected measure, alongside a bare windowed measure
- THEN all three values are correct and the dimension's grain treatment of the shared aggregate is unaffected by its measure role

### Requirement: An aggregate expression shared by a computed dimension and another position evaluates per position
When the same explicitly grained aggregate expression — a partitioned aggregate, a re-aggregation, a transform over them, or a cross-model re-aggregation — appears inside a computed dimension AND in another position of the same query (measure, measure-typed filter conjunct, order target), each occurrence SHALL evaluate as that position defines it: inside the dimension at row scope, broadcast onto the rows its grain determines; elsewhere at query grain (Axiom 13). The shared expression SHALL be computed once (one producer) and the query SHALL execute with correct values or fail with a typed query error — never an internal placeholder, materialisation, hidden-slot, name-collision or join-back error. Filtering or ordering by the computed dimension's NAME uses its banded output; filtering or ordering by the underlying expression uses the expression's value.

Oracles below use the DEV-1847 `sales` fixture with `R` = `avg(sum(amount, partition_by=[city, region]), partition_by=region)` (North 45, South 70, East 60, Gap 10, Void NULL), `rlevel` = `CASE WHEN R > 50 THEN 'hi' ELSE 'lo' END`, `tlevel` = `CASE WHEN rank(R, direction='desc') > 1 THEN 'top' ELSE 'rest' END`, and `tot` = `amount:sum`.

#### Scenario: Re-aggregation in a dimension and a measure-typed filter
- WHEN a query over dimensions `[region, rlevel]` selects `tot` and filters on `R < amount:sum`
- THEN the rows are East/hi 180, Gap/lo 20, North/lo 90, South/hi 140 (Void's NULL fails the predicate), and the filter `R > amount:sum` returns zero rows without error

#### Scenario: Re-aggregation in a dimension and an order target
- WHEN the same query is ordered by `R` descending
- THEN rows arrive South, East, North, Gap, then Void

#### Scenario: Re-aggregation in a dimension and a measure
- WHEN the same query also selects `R` as a measure
- THEN `R` per row equals the per-region values above and `rlevel` agrees with it

#### Scenario: Transform over a re-aggregation in a dimension
- WHEN a query over dimensions `[region, tlevel]` selects `tot`
- THEN South and Void (NULL rank) are `rest` and East, North and Gap are `top`

#### Scenario: Transform over a re-aggregation in a dimension and elsewhere
- WHEN the `tlevel` query also selects `rank(R, direction='desc')` as a measure, or filters on `rank(R, direction='desc') > 1` or `rank(R, direction='desc') < 4`, or orders by `rank(R, direction='desc')` ascending
- THEN the rank values are South 1, East 2, North 3, Gap 4, Void NULL on every supported dialect; `rank(R, direction='desc') > 1` keeps East, North, Gap; `rank(R, direction='desc') < 4` keeps South, East, North; and the ascending order is South, East, North, Gap, with Void's NULL rank sorting per the dialect's NULL ordering (last outside T-SQL)

#### Scenario: Two dimensions sharing a re-aggregation
- WHEN a query declares both `tlevel` and `rlevel` as dimensions and selects `tot` and `R`
- THEN the rows are South (rest, hi), East (top, hi), North (top, lo), Gap (top, lo), Void (rest, lo) with `tot` unchanged, and no internal name reaches the user

#### Scenario: Ordering by a transform shared with a dimension
- WHEN a computed dimension is `CASE WHEN rank(amount:sum(partition_by=region), direction='desc') > 1 THEN 'top' ELSE 'rest' END` and the query orders by `rank(amount:sum(partition_by=region), direction='desc')` ascending
- THEN rows arrive East, South, North, Gap, with Void's NULL rank sorting per the dialect's NULL ordering (last outside T-SQL), and ordering by the dimension's name instead sorts by its banded value

#### Scenario: Windowed transform over a re-aggregation in a dimension with a filter
- WHEN a monthly query declares the dimension `cumsum(min(<attached operand>, partition_by=region))` and filters on that dimension
- THEN it fails at plan time with the `TimeAxisError` naming `cumsum`, exactly as the same transform over `amount:sum(partition_by=region)` does

#### Scenario: Cross-model re-aggregation in a dimension and a measure-typed filter
- WHEN a `corders` query over dimensions `[customers.regions.name, cl]`, with `cl` banding `avg(sum(amount, partition_by=customer_id), partition_by=customers.regions.name)` at 40, filters on that re-aggregation `< amount:sum`
- THEN the only row is North/lo with `amount:sum` 70

### Requirement: Expressions over a computed dimension's whole aggregate evaluate as the dimension's value
When a computed dimension's whole expression is a partitioned aggregate, a re-aggregation or a transform, that expression — alone or inside arithmetic or a scalar call — in measure position (partitioned aggregate, re-aggregation) or order position (all three) SHALL evaluate as the dimension's value per result cell and execute on every supported dialect, never with an internal placeholder, render or partition-key error. The shared aggregate SHALL be computed once and attached once.

Oracles use the DEV-1847 `sales` fixture with `P` = `amount:sum(partition_by=region)` (North 90, South 140, East 180, Gap 20, Void NULL), `R` = `avg(sum(amount, partition_by=[city, region]), partition_by=region)` (North 45, South 70, East 60, Gap 10, Void NULL) and `tot` = `amount:sum`.

#### Scenario: Arithmetic over a partitioned-aggregate dimension as a measure
- **WHEN** a query over dimensions `[region, rd]`, with `rd` = `P`, declares the measures `P + 1` and `P * 2 + amount:sum`
- **THEN** `P + 1` is North 91, South 141, East 181, Gap 21, Void NULL and `P * 2 + amount:sum` is North 270, South 420, East 540, Gap 60, Void NULL

#### Scenario: Arithmetic over a re-aggregation dimension as a measure
- **WHEN** a query over dimensions `[region, rd]`, with `rd` = `R`, declares the measures `R + 1` and `R * 2 + amount:sum`
- **THEN** `R + 1` is North 46, South 71, East 61, Gap 11, Void NULL and `R * 2 + amount:sum` is North 180, South 280, East 300, Gap 40, Void NULL

#### Scenario: Arithmetic over an aggregate dimension as an order target
- **WHEN** the `rd` = `P` query orders by `P + 1` descending, and the `rd` = `R` query orders by `R + 1` descending
- **THEN** the first arrives East, South, North, Gap, Void and the second South, East, North, Gap, Void

#### Scenario: Arithmetic over a transform dimension as an order target
- **WHEN** a query over dimensions `[region, rk]`, with `rk` = `rank(P, direction='desc')`, orders by `rank(P, direction='desc') + 1` descending
- **THEN** rows arrive in descending `rk` order

#### Scenario: A finer-grained aggregate dimension read as a measure
- **WHEN** a query over dimensions `[region, x]`, with `x` = `amount:sum(partition_by=[city, region])`, declares the measures `amount:sum(partition_by=[city, region])` and `amount:sum(partition_by=[city, region]) + 1`
- **THEN** on every row the first equals `x` and the second equals `x` plus 1
