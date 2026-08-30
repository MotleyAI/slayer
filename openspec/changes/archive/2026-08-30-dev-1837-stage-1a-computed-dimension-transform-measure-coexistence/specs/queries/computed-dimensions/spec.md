## Purpose

Defines computed (expression) dimensions: which expressions are legal as dimensions, how expressions containing explicitly-grained aggregates and transforms over them behave, and the error surface for shapes that are not (yet) expressible.

## ADDED Requirements

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
