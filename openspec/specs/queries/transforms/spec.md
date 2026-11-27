# queries/transforms Specification

## Purpose
Composition rules for window and self-join transforms: which input shapes
`time_shift` and `consecutive_periods` accept, the values composite inputs
produce, the predicate typing contract, and the uniform fail-closed errors for
the remaining unsupported shapes.

## Requirements

### Requirement: Composite-input time_shift

`time_shift` (and therefore `change` / `change_pct`, which desugar onto it)
SHALL accept an input that is an arithmetic / scalar-call composite whose
slottable leaves are all aggregates (literals and arbitrary nesting allowed),
and an input containing a nested transform anywhere in its tree or a top-level
aggregate-typed predicate (`IN` / comparison over aggregates; `time_shift` only —
`change` / `change_pct` reject boolean-shaped inputs). Every input SHALL
evaluate by the one rule of "time_shift evaluates its operand in the moved
frame"; no input's evaluation SHALL depend on how it is built — a bare
aggregate, a local or cross-model composite, a predicate and a nested transform
are evaluated alike.

The result SHALL equal the input evaluated at the shifted time bucket within the
same partition — matching what the same expression as a measure would return
for that bucket in the query with its frame moved by the shift — and SHALL be
NULL when the input has no cell at the shifted bucket, including under
NULL-absorbing wrappers such as `coalesce`. Aggregation parameters, parameter
fragments, and column filters SHALL apply per leaf without leaking between
leaves. **Every constituent keeps its own grain** (Axioms 10 and 11.1): a
constituent whose grain — its explicit `partition_by=`, else the query grain —
contains the time axis, and any ranked or windowed leaf, is evaluated afresh in
the shifted evaluation at that grain; a constituent whose grain does not contain
the axis is constant along the axis and contributes its unshifted, in-frame
value. A partition key that is attributable across a join is honoured the same
way; a partition key whose dependency closure crosses a fanning or unproven hop
from the leaf's home resolves per the mode (`queries/attribution-modes` ›
Unattributable explicit partition keys resolve per the mode): under the default
and `"associate"` the leaf reads its associated cell at the shifted bucket, and
under `"broadcast"` and `"error"` the query fails with the typed partition-key
error, never a multiplied value. Adding the shifted measure SHALL NOT change the
row count or any other column's values.

**Frame rule.** A frame bound — `date_range`, or a relational bound with a
temporal literal on the raw column of one of the query's time dimensions —
selects the visible buckets. In the shifted evaluation every frame bound on the
time axis's column is moved so that it covers exactly the source buckets of the
visible buckets, so the earliest visible bucket is never short-changed. Every
other predicate, including a frame bound on another time dimension, a population
predicate conjoined with a frame bound and every model-level filter, applies to
the shifted evaluation unchanged, and a population restriction the query
resolves by association restricts the shifted evaluation the same way.

**Join-back.** A row reads the shifted relation at the bucket
containing its own bucket offset by the shift, matched on every other member of
the shifted relation's grain — every projected dimension and secondary time
dimension, except those an operand whose leaves are all coarser does not carry
(Axiom 11.1). For a shift whose granularity is a
multiple of the bucket this is the bucket `N` periods away; for a shift not
aligned to the bucket it is the bucket containing the offset instant.

#### Scenario: Ratio shifted one period back

- **WHEN** a query with a month time dimension and a dimension requests
  `time_shift(sum(revenue) / sum(qty), -1)`
- **THEN** each row carries the previous month's ratio for its dimension group,
  with executed values matching hand-computed expectations on SQLite and DuckDB

#### Scenario: change_pct over a ratio resets per partition

- **WHEN** a query grouped by store and month requests
  `change_pct(sum(revenue) / count(*))`
- **THEN** each store's first month yields NULL and later months yield that
  store's own month-over-month ratio growth, never another store's

#### Scenario: Missing shifted bucket yields NULL under a scalar wrap

- **WHEN** `time_shift(coalesce(sum(revenue), 0), -1)` is evaluated for the
  earliest bucket in the data
- **THEN** the shifted value is NULL (no shifted bucket exists), not 0

#### Scenario: Two differently-parameterized aggregate leaves

- **WHEN** the composite input combines two aggregates with distinct resolved
  parameters (for example a fragment-kwarg aggregation and a column-filtered
  aggregation)
- **THEN** each leaf re-aggregates with its own parameters and filter in the
  shifted period and the executed composite value matches hand-computed
  expectations

#### Scenario: Crossing aggregation parameter registers its join per leaf

- **WHEN** a composite leaf's aggregation parameter references a joined model's
  column
- **THEN** the shifted computation binds that column through the required join
  and executes correctly

#### Scenario: Nested transform input shifts the materialised series

- **WHEN** a query with a month time dimension requests
  `time_shift(cumsum(sum(revenue)), -1)`
- **THEN** each row carries the previous bucket's cumulative sum — with no frame
  bound, the value the inner transform holds at the shifted bucket — with NULL
  at the first bucket, correct by hand-computed executed values on SQLite and
  DuckDB

#### Scenario: Cross-model aggregate leaf inside a composite shifts

- **WHEN** a `time_shift` composite input contains an aggregate over another
  model's column (dotted path), alone or mixed with local aggregate leaves
- **THEN** the composite's shifted value is correct by executed values — never
  the former cross-model-leaf rejection — and equals the composite's value at
  the shifted bucket

#### Scenario: Aggregate-typed predicate input shifts as a boolean series

- **WHEN** a query requests `time_shift(sum(revenue) > 100, -1)` over a month
  time dimension
- **THEN** each row carries the previous bucket's boolean, NULL where the
  shifted bucket is absent, by executed values

#### Scenario: change over a predicate stays rejected by the typing contract

- **WHEN** a query requests `change(sum(revenue) > 100)`
- **THEN** the query fails with the existing boolean-in-arithmetic-context
  typing error (the desugared subtraction consumes a boolean), not an internal
  error

#### Scenario: Partitioned leaf keeps its grain inside a shifted composite

- **WHEN** a query over `monthly` by `region` and month requests
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)`
  (rows: North Jan 10 / Feb 20 / Mar 30, South Jan 5 / Feb 15, West Feb NULL)
- **THEN** each row carries the prior month's share of that month's
  cross-region total — Feb North ≈ 0.667, Feb South ≈ 0.333, Mar North ≈ 0.571,
  every Jan row and West Feb NULL — by executed values on SQLite and DuckDB,
  never 1.0

#### Scenario: change and change_pct over a partitioned-leaf composite

- **WHEN** the same query requests
  `change(sum(amount) / sum(amount, partition_by=[ordered_at]))` and
  `change_pct(sum(amount) / sum(amount, partition_by=[ordered_at]))`
- **THEN** the change is Feb North ≈ −0.095, Feb South ≈ +0.095, Mar North
  ≈ +0.429 and the percentage change Feb North ≈ −0.143, Feb South ≈ +0.286,
  Mar North ≈ +0.75, NULL elsewhere, by executed values

#### Scenario: Single partitioned leaf in a trivial composite

- **WHEN** the same query requests
  `time_shift(sum(amount, partition_by=[ordered_at]) / 2, -1)`
- **THEN** each row carries half the prior month's cross-region total — Feb
  North 7.5, Feb South 7.5, Mar North 17.5, West Feb 7.5 (the operand grain is
  the month alone, so a region absent from January still reads it), Jan rows
  NULL — by executed values

#### Scenario: Partitioned leaf at a non-time grain is constant along the axis

- **WHEN** the same query requests
  `time_shift(sum(amount) / sum(amount, partition_by=[region]), -1)`
- **THEN** each row carries the prior month's sum divided by the region's own
  total — Feb North ≈ 0.167, Mar North ≈ 0.333, Feb South 0.25, NULL elsewhere —
  by executed values

#### Scenario: Re-aggregation over a shifted partitioned composite

- **WHEN** a query over `monthly` with only a month time dimension requests
  `avg(time_shift(sum(amount, partition_by=[region, ordered_at]) / sum(amount, partition_by=[ordered_at]), -1))`
- **THEN** each month carries the mean of the prior month's regional shares —
  Feb 0.5, Mar ≈ 0.571, Jan NULL — by executed values

#### Scenario: Bare partitioned leaf reaches outside the date range

- **WHEN** the same query restricts the month time dimension with
  `"date_range": ["2024-02-01", "2024-03-31"]` and requests
  `time_shift(sum(amount, partition_by=[ordered_at]), -1)`
- **THEN** Feb North and Feb South carry January's total 15 and Mar North
  February's total 35, by executed values — never NULL at the first visible
  bucket

#### Scenario: Bare ranked leaf reaches outside the date range

- **WHEN** the same date-ranged query requests `time_shift(last(amount), -1)`
- **THEN** Feb North carries 10, Feb South 5 and Mar North 20 — the prior
  month's last value — by executed values

#### Scenario: Non-time partition under a date range keeps its in-frame value

- **WHEN** the same date-ranged query requests
  `time_shift(sum(amount) / sum(amount, partition_by=[region]), -1)`
- **THEN** the denominator is the region's total within the date range (North
  50, South 15): Feb North 0.2, Mar North 0.4, Feb South ≈ 0.333, by executed
  values

#### Scenario: Population predicate conjoined with a frame bound survives

- **WHEN** a query over `monthly` by `region` and month with the filter
  `ordered_at >= '2024-02-01' and region = 'North'` requests
  `time_shift(sum(amount) + sum(amount, partition_by=[ordered_at]), -1)`
- **THEN** Feb North carries 20 and Mar North 40 — the bound is moved in the
  shifted evaluation while the region predicate restricts it — by executed
  values

#### Scenario: Population restriction by association reaches the shifted evaluation

- **WHEN** a query rooted at `orders` by `status` and month carries a row
  filter across the fanning `regions → region_events` hop
  (`customers.regions.region_events.value > 40`) and requests
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)`
- **THEN** the shifted evaluation is restricted to the same associated orders:
  Feb ok ≈ 0.394, Feb new ≈ 0.606, Apr ok NULL (no associated March rows), by
  executed values

#### Scenario: Association-resolved leaf is re-evaluated by association

- **WHEN** a query rooted at `orders` in associate mode groups by the fanning
  dimension `customers.regions.region_events.value` and month and requests
  `time_shift(sum(amount), -1)`
- **THEN** each row carries the prior month's associated sum for its value —
  (50, Feb) 33, (30, Apr) 5, NULL elsewhere — by executed values, with the row
  count unchanged by the measure

#### Scenario: Partition key attributable across a join

- **WHEN** a query rooted at `orders` by `customers.tier` and month requests
  `time_shift(sum(amount) / sum(amount, partition_by=[customers.tier]), -1)`
- **THEN** the tier total is resolved through the to-one join and each row
  carries the prior month's share of it — gold Apr 0.1, NULL where the prior
  month has no rows for the tier — by executed values

#### Scenario: Shifted leaf with a fanning partition key associates per bucket

- **WHEN** a query rooted at `customers` by `orders.status` and signup month
  requests `time_shift(sum(spend, partition_by=[orders.status, signup_at]), -1)`,
  omitting the mode
- **THEN** each row carries the prior signup month's associated cell — ok Jan
  NULL, Feb 100, Mar 150, Apr 60; new Jan NULL, Feb 100, Mar 150; NULL Apr NULL
  — by executed values, never a multiplied value; under `"broadcast"` and
  `"error"` the query fails with the typed partition-key error naming
  `orders.status`

#### Scenario: Ranked leaf inside a shifted composite executes

- **WHEN** the `monthly` query requests `time_shift(last(amount) / 2, -1)`
- **THEN** each row carries half the prior month's last value — Feb North 5,
  Mar North 10, Feb South 2.5 — by executed values, never an internal error

#### Scenario: Windowed leaf inside a shifted composite keeps its window

- **WHEN** the `monthly` query requests
  `time_shift(sum(amount, window='90d') / 2, -1)`
- **THEN** each row carries half the prior month's trailing-window sum — Feb
  North 5, Mar North 15, Feb South 2.5, Jan rows and West Feb NULL — by
  executed values, never a value with the window silently dropped

#### Scenario: Bare windowed leaf reaches outside the date range

- **WHEN** the date-ranged `monthly` query requests
  `time_shift(sum(amount, window='90d'), -1)`
- **THEN** Feb North carries January's trailing-window sum 10 and Mar North
  February's 30, by executed values

#### Scenario: Two offsets share one shifted evaluation

- **WHEN** the `monthly` query requests both
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -1)` and
  `time_shift(sum(amount) / sum(amount, partition_by=[ordered_at]), -2)`
- **THEN** the generated SQL computes the shifted composite in exactly one
  relation read by both, and the two-period value at Mar North ≈ 0.667, by
  executed values

#### Scenario: Unaligned shift reads the bucket containing the offset instant

- **WHEN** the `monthly` query requests `time_shift(sum(amount), -1, 'day')`
  over month buckets
- **THEN** Feb North carries January's 10 and Mar North February's 20 — the
  bucket containing the day before each bucket start — by executed values

#### Scenario: Shifted composite over a stage time dimension

- **WHEN** an inner stage projects `region`, `ordered_at` at `month` and
  `sum(amount)` as `rev`, and the outer stage declares a month time dimension
  on that column with `"date_range": ["2024-02-01", "2024-03-31"]` and
  requests `time_shift(sum(rev) / sum(rev, partition_by=[ordered_at]), -1)` by
  `region`
- **THEN** Feb North ≈ 0.667, Feb South ≈ 0.333 and Mar North ≈ 0.571, by
  executed values on SQLite and DuckDB

#### Scenario: Shifted evaluation is closed and dependency-ordered

- **WHEN** SQL is generated for any shape above on the golden dialect set
- **THEN** every emitted CTE references only relations declared before it,
  the shifted relation and every producer it reads are scope-closed, and the
  generated SQL matches recorded golden baselines

### Requirement: Composite-input consecutive_periods

`consecutive_periods` SHALL accept any Mode-B value-key input tree — arithmetic
of any operator, scalar calls, `IN` / negated `IN`, null tests
(`IS NULL` / `IS NOT NULL`), boolean connectives, and nested transforms in any
position. A boolean-shaped input is used as the predicate directly with NULL
treated as false; a value-shaped input is true where its value is non-NULL and
non-zero. Streak semantics are unchanged: false or NULL breaks the run and
returns 0. Emitted SQL SHALL use a boolean-shaped predicate only in condition
positions — never wrapped as a scalar value — so generation is valid on
strictly-typed dialects (Postgres, T-SQL, BigQuery).

#### Scenario: Numeric delta truthiness

- **WHEN** a query requests `consecutive_periods(sum(revenue) - sum(cost))` over
  a month series
- **THEN** the streak counts consecutive months where the delta is non-NULL and
  non-zero, matching hand-computed values on SQLite and DuckDB

#### Scenario: Growth streak over a nested transform

- **WHEN** a query requests `consecutive_periods(change(sum(revenue)) > 0)`
- **THEN** the streak counts consecutive months of positive month-over-month
  growth

#### Scenario: Bare nested transform input

- **WHEN** a query requests `consecutive_periods(cumsum(sum(revenue)))`
- **THEN** the streak counts consecutive months where the running total is
  non-NULL and non-zero

#### Scenario: Scalar call inside a comparison

- **WHEN** a query requests `consecutive_periods(round(sum(revenue)) >= 10)`
- **THEN** the streak counts consecutive months where the rounded total reaches
  the threshold

#### Scenario: Newly lifted predicate families execute

- **WHEN** `consecutive_periods` receives a top-level `IN`, negated
  `IN`, `and`, `or`, or `not` predicate, including groups whose predicate
  evaluates to NULL
- **THEN** each executes on SQLite and DuckDB with NULL treated as false

#### Scenario: Nested IN materialises its column

- **WHEN** an `IN` predicate over a dimension column appears nested inside a
  boolean connective (for example `status in ('a','b') and sum(revenue) > 0`)
- **THEN** the referenced column materialises and the streak executes correctly

#### Scenario: Top-level null test drives the streak

- **WHEN** a query requests `consecutive_periods(sum(hi_rev) IS NOT NULL)`
  grouped by store, where one store's aggregate is NULL in the last month
- **THEN** the streak counts consecutive non-NULL months and the NULL month
  breaks the run (and the `IS NULL` form counts the complementary months)

#### Scenario: Null test under a boolean connective

- **WHEN** a query requests
  `consecutive_periods(sum(hi_rev) IS NOT NULL AND sum(cost) > 0)`
- **THEN** the query executes with both conjuncts applied, rather than failing
  with a boolean-shaped-operands `ValueError`

#### Scenario: Null test over a dimension column

- **WHEN** a query requests `consecutive_periods(store IS NOT NULL)` grouped by
  store
- **THEN** the referenced column materialises and the streak executes correctly

#### Scenario: Predicates emit as bare conditions on strict dialects

- **WHEN** SQL is generated for any boolean-shaped `consecutive_periods`
  predicate (a null test included) on Postgres, T-SQL, or BigQuery
- **THEN** the predicate appears directly as the `CASE WHEN` condition, with no
  `COALESCE(..., FALSE)` scalar wrapper and no `... IS NOT NULL AND ... <> 0`
  truthiness wrapper around a boolean

### Requirement: consecutive_periods predicate typing contract

Boolean-shaped SHALL be defined recursively as: a comparison; a null test
(`IS NULL` / `IS NOT NULL`); `IN`; or `and` / `or` / `not` whose
operands are themselves boolean-shaped. A boolean-shaped node SHALL be accepted
at the predicate top level and in a conditional's condition position (`iif`
first argument), and in a numeric position — an arithmetic operand, a numeric
scalar-call argument, an operand compared with numbers (an `IN` list included) —
it SHALL be its integer value (per `aggregations/boolean-inputs`), the input then
being value-shaped. `and` / `or` / `not` SHALL reject non-boolean-shaped operands
with a `ValueError` naming the shape. A top-level string-family scalar call SHALL
be rejected as a predicate (its truthiness is undefined).

#### Scenario: iif condition position accepts a predicate

- **WHEN** a query requests `consecutive_periods(iif(sum(revenue) > 0, 1, 0))`
- **THEN** the query executes, with the streak driven by the iif value's
  truthiness

#### Scenario: Boolean in arithmetic context rejected

- **WHEN** a query requests
  `consecutive_periods((sum(revenue) > 0) + (sum(cost) > 0))`
- **THEN** the query executes, each comparison read as its integer, and the
  streak counts consecutive months where neither comparison is NULL and at least
  one holds

#### Scenario: Boolean as scalar-call argument rejected

- **WHEN** a query requests `consecutive_periods(coalesce(sum(revenue) > 0, 0))`
- **THEN** the query executes, the comparison read as its integer, and the streak
  counts consecutive months with positive revenue

#### Scenario: Boolean in an IN operand rejected

- **WHEN** a query requests `consecutive_periods((sum(revenue) > 0) in (1, 0))`
- **THEN** the comparison is read as its integer inside the `IN` test — never
  passed through as a bare predicate into the emitted `IN` list — and the streak
  counts consecutive months whose revenue is non-NULL

#### Scenario: String-family scalar call rejected as predicate

- **WHEN** a query requests `consecutive_periods(lower(max(name)))`
- **THEN** the query fails with a `ValueError` explaining that a string-valued
  predicate has no truthiness

#### Scenario: Null test in a value position rejected

- **WHEN** a query requests `consecutive_periods((sum(hi_rev) IS NULL) + 1)`
- **THEN** the query executes with the null test read as its integer, so every
  month's value is 1 or 2 and the streak never breaks

### Requirement: Uniform fail-closed transform errors

Every render path SHALL raise the identical user-facing `ValueError` for an
unsupported transform-input shape, naming the transform, the shape, and the
remedy, with no internal stage markers in the message. The presence of a
cross-model measure elsewhere in the query SHALL NOT change which error a given
unsupported shape produces.

#### Scenario: Same error with and without a cross-model sibling

- **WHEN** an unsupported transform-input shape is queried once as a purely
  local query and once alongside a cross-model measure
- **THEN** both fail with the same error message

#### Scenario: SQL generation is pinned across dialects

- **WHEN** the lifted composite shapes are rendered for the golden dialect set
  (postgres, sqlite, duckdb, tsql, bigquery)
- **THEN** the generated SQL matches recorded golden baselines

### Requirement: time_shift row-level-leaf rejection stays fail-closed

`time_shift` (and `change` / `change_pct`) SHALL reject, with a `ValueError`
naming the operation, the offending input shape, and the remedies (aggregate
the leaf, project it as a query dimension, or compute the row-level value in
an earlier stage of a multi-stage `source_queries` model), any input
containing a row-level (non-aggregate) leaf that refines the query grain —
bare, mixed with aggregates, or a predicate over row-level columns. A bare
aggregate input SHALL keep its existing behavior; a bare row column or
derived column input is rejected by the same rule every other transform
obeys (the "Transforms reject grain-refining row-level leaves" requirement).
The rejection SHALL surface at plan time as a typed checker error, before any
SQL is generated.

#### Scenario: Mixed aggregate-and-row composite rejected

- **WHEN** a query requests `time_shift(sum(revenue) * weight, -1)` where
  `weight` is a plain column
- **THEN** the query fails with a `ValueError` naming the mixed shape

#### Scenario: Row-level predicate input rejected

- **WHEN** a query requests `time_shift(store in ('A', 'B'), -1)` where `store`
  is a plain column that is not a query dimension
- **THEN** the query fails with the row-level-leaf `ValueError` naming the shape
  and the remedy, rather than leaking an internal `RuntimeError`

#### Scenario: Row leaf hidden inside a nested transform rejected

- **WHEN** a query requests `time_shift(cumsum(weight), -1)` where `weight` is
  a plain column
- **THEN** the query fails with the row-level-leaf `ValueError` — a nested
  transform does not launder its row-level input into a series

### Requirement: Composites over transforms

A composite (arithmetic or scalar-call expression) whose operands include a transform
SHALL be legal in measure, filter and ORDER BY positions whatever its other operands are:
a local aggregate, a cross-model aggregate, another transform, or a literal, in any
nesting. Every operand the composite needs SHALL be materialised for it, including a local
aggregate that appears nowhere else in the query, and the composite SHALL evaluate to the
same value whether or not any operand is also selected on its own. The transform's value
SHALL be unchanged by the composite around it. No such
composite SHALL fail with an internal render error. The scenarios below run under
`to_many_handling: "broadcast"`, where a cross-model inner over an order month repeats
its total.

#### Scenario: Transform over a cross-model inner plus a hidden local aggregate

- **WHEN** a query rooted at `orders` with a month time dimension selects
  `change(sum(customers.spend)) + sum(amount)` and nothing else
- **THEN** each row carries the month-over-month change of the cross-model aggregate
  plus that month's local sum, by hand-computed executed values on SQLite and DuckDB,
  with NULL in the first month

#### Scenario: Hidden local operand of another aggregation kind

- **WHEN** the composite's hidden local operand is `count(*)` or `max(amount)` (for example
  `change(sum(customers.spend)) + count(*)`)
- **THEN** the query executes with the operand materialised, by executed values

#### Scenario: Transform over a crossing-fragment inner plus a hidden local aggregate

- **WHEN** a query selects `change(wscaled_sum(amount)) + sum(amount)`, where `wscaled_sum`
  is an aggregation whose default parameter crosses a join
- **THEN** each row carries the weighted-scaled delta plus the local sum, by executed
  values on SQLite and DuckDB

#### Scenario: Other transform families compose the same way

- **WHEN** the transform is `time_shift(sum(customers.spend), -1)` or
  `cumsum(sum(customers.spend))` combined with a hidden local aggregate
- **THEN** the composite executes with the transform's own semantics unchanged, by
  executed values

#### Scenario: Conditional over a transform and mixed aggregates

- **WHEN** a query selects
  `iif(change(sum(customers.spend)) > 0, sum(customers.spend), sum(amount))`
- **THEN** each row carries the cross-model total where the change is positive and the
  local sum otherwise, by executed values

#### Scenario: Selecting an operand on its own changes nothing

- **WHEN** the same query additionally selects `sum(amount)` as its own measure
- **THEN** the composite's values, the row count and every other column are identical to
  the query without it

#### Scenario: Composite in filter and order positions

- **WHEN** `change(sum(customers.spend)) + sum(amount)` appears only in a filter or only as
  an ORDER BY key, with a dimension present
- **THEN** rows are masked or sorted by the composite's value exactly as when it is
  selected, by executed values

#### Scenario: Inner varying along the time axis

- **WHEN** the cross-model inner is attributable to the query's time axis (for example
  `change(sum(orders.amount))` by customer signup month from `customers`, or
  `change(sum(customers.spend))` by `customers.signup_at` from `orders`)
- **THEN** the delta and `change_pct` percentages are correct per period by hand-computed
  executed values on SQLite and DuckDB, and adding the measure changes no row and no
  other column

#### Scenario: The issue's named shapes execute directly

- **WHEN** a query selects `change(wscaled_sum(amount))`, `change_pct(wscaled_sum(amount))`,
  `change(sum(customers.spend))` or `change_pct(sum(customers.spend))` alone
- **THEN** each executes with hand-computed values on SQLite and DuckDB, the broadcast
  cross-model inner yielding a zero delta and zero percentage after the first period

### Requirement: Time axis on stage datasets

A downstream stage's own time dimension SHALL be the time axis for every time-ordered transform in that stage (`time_shift`, `change`, `change_pct`, `cumsum`, `lag`, `lead`, `consecutive_periods`), for its windowed aggregates and for the default ranking key of a `first` / `last` over an aggregate in that stage, with values identical to the same shapes evaluated over a model-backed dataset holding the same rows. With two stage time dimensions and no `main_time_dimension`, a time-ordered transform SHALL fail with the existing unambiguous-time-dimension remedy; `main_time_dimension` SHALL select the axis. A stage has no model-level default time dimension, so no default is applied.

#### Scenario: time_shift over a stage time dimension

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` over an inner monthly stage and selects `time_shift(sum(rev), -1, 'month')`
- **THEN** each row carries the previous month's inner sum, NULL for the first month, by executed values on SQLite and DuckDB

#### Scenario: change and cumsum over a stage time dimension

- **WHEN** the same outer stage selects `change(sum(rev))` and `cumsum(sum(rev))`
- **THEN** each row carries the month-over-month delta (NULL first) and the running total, by executed values

#### Scenario: last over a stage time dimension

- **WHEN** the same outer stage selects `last(sum(rev))`
- **THEN** every row carries the value of the latest month — the second-order `last`
  ranked by the stage's month bucket — by executed values

#### Scenario: Windowed aggregate over a stage time dimension

- **WHEN** the outer stage selects `sum(rev, window='60d')` over the stage time dimension
- **THEN** each row carries the trailing-window sum keyed on the stage bucket, by executed values

#### Scenario: time_shift over a multi-hop flat time dimension

- **WHEN** an inner stage projects `customers.regions.last_activity_at` at `month` with `count(*)` named `n`, and the outer stage declares a time dimension on `customers__regions__last_activity_at` at `month` with `time_shift(sum(n), -1, 'month')`
- **THEN** the shifted relation references the inner stage's flat alias and the query executes with correct values

#### Scenario: Two stage time dimensions need main_time_dimension

- **WHEN** the outer stage declares time dimensions on two distinct temporal columns, `created_at` and `shipped_at`, each at `month`, and selects `change(sum(rev))` without `main_time_dimension`
- **THEN** planning fails with the existing error naming the `main_time_dimension` remedy, and setting `main_time_dimension` to `created_at` makes the query execute with that column's bucket as the axis

### Requirement: Transforms reject grain-refining row-level leaves

A transform, used in
measure, filter, or order position, or as a constituent of an aggregation
source, SHALL reject with a typed plan-time error — before any SQL is
generated — any row-level (non-aggregate) leaf in its input that refines the
consumer grain, that is, a leaf that is not itself a projected query
dimension. The error SHALL name the transform, the offending leaf, and the
remedies (aggregate the leaf, e.g. `cumsum(sum(weight))`; project it as a query
dimension; or compute it in an earlier `source_queries` stage), and cite no
tracking issue. The rule applies uniformly to every such transform op — the
rank family and the shift family (`time_shift`, `change`, `change_pct`)
included. Leaves that are projected grain keys — plain or computed dimensions
— remain legal, evaluated at the query grain. The raw source column of a
bucketed time dimension is not a projected grain key (it refines the bucket).
`first` / `last` are aggregations, never transforms, and the stricter
dimension-position rules are unchanged.

#### Scenario: Bare grain-refining leaf rejected
- **WHEN** a query with a month time dimension and no `weight` dimension selects
  the measure `cumsum(weight)`
- **THEN** it fails at plan time with the typed error naming the remedy — never
  SQL whose base grain is inflated to one row per (bucket, weight-value)

#### Scenario: Rank family is covered
- **WHEN** a query over `[store]` with a month time dimension selects the
  measure `rank(qty, direction='desc')`
- **THEN** it fails with the same typed error, never a result carrying one row
  per (store, month, qty-value)

#### Scenario: Shift family is covered
- **WHEN** a query with a month time dimension and no `weight` dimension
  selects `time_shift(weight, -1)`, `change(weight)` or `change_pct(weight)`,
  or `time_shift(hi_rev, -1)` over the unprojected derived column `hi_rev`
- **THEN** each fails at plan time with the same typed error naming the
  transform, the leaf and the remedies — never a result whose row count differs
  from the same query without the measure

#### Scenario: Predicate over an unprojected row column rejected
- **WHEN** a query selects the measure `consecutive_periods(weight > 0)` with
  `weight` not a query dimension
- **THEN** it fails with the same typed error naming the aggregate-the-leaf
  remedy

#### Scenario: A projected grain key stays legal
- **WHEN** a query projects `weight` as a dimension and selects the measure
  `rank(weight, direction='desc')`
- **THEN** it compiles at the query grain and executes with correct values —
  no error, no extra result rows

#### Scenario: A projected grain key stays legal under the shift family
- **WHEN** a query over `[store]` with a month time dimension selects
  `time_shift(store, -1)`
- **THEN** it compiles at the query grain and executes with the operand's own
  value per row — no error, no extra result rows

#### Scenario: Attached values do not launder a row leaf
- **WHEN** a query selects the measure
  `cumsum(weight * avg(unit_price, partition_by=product))` with `weight` not
  projected
- **THEN** it fails with the same typed error — a transform does not collapse
  row grain, unlike an aggregation

#### Scenario: Row leaf under a transform inside an aggregation source rejected
- **WHEN** a query selects the measure `sum(cumsum(weight) - 1)` with `weight` not a
  query dimension
- **THEN** it fails at plan time with the same typed error naming the transform and
  the aggregate-the-leaf remedy — an enclosing aggregation does not launder the
  transform's row leaf

#### Scenario: Projected grain key under a transform inside a source stays legal
- **WHEN** a query over `[region]` selects the measure `sum(rank(region, direction='desc'))`
- **THEN** it compiles: the transform types at the query grain and the aggregation is
  the degenerate identity with the degenerate-re-aggregation warning, never an error

### Requirement: Rank-family partition keys are operand-grain members
A rank-family transform (`rank`, `dense_rank`, `percent_rank`, `ntile`) partitions its
operand's cells, so every key in its own `partition_by=` SHALL be a member of the
transform's operand grain, in every position. The operand grain is the union over the
transform's input: an aggregate contributes its explicit `partition_by=` keys, else the
query grain (its dimensions and time buckets), plus the query's active time bucket when
it is windowed, a `first` / `last` contributing its own grain (`queries/semantics` › A ranked aggregation's grain drops its ranking key);
a nested transform contributes its own operand grain; a composite contributes the union of its operands,
with a projected row-level leaf contributing itself; an input with no aggregate (only
row-level leaves or literals) is the query grain, since it is evaluated once per
query-grain cell. A non-member key SHALL fail at plan time with an error naming
the transform, the key, the operand grain and the remedy (add the key to the inner
aggregate's `partition_by=`, or partition by a member). A key that is no query dimension
at all keeps the existing "not a query dimension" error, and an ungrained inner aggregate
in dimension position keeps the existing grain-self-containment error; both take
precedence over the membership error.

#### Scenario: Non-member query dimension as a measure
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=product, direction='desc')`
- **THEN** planning fails with an error naming `rank`, `product`, the operand grain `city, region` and the `partition_by=` remedy — it never executes by widening the grain

#### Scenario: Non-member query dimension in a filter
- **WHEN** the same query filters `rank(sum(amount, partition_by=[city, region]), partition_by=product, direction='desc') <= 2`
- **THEN** planning fails with the same error

#### Scenario: Non-member key in dimension position, plain column
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount, partition_by=[city, product]), partition_by=region, direction='desc')`
- **THEN** planning fails with the same error naming `region` and the grain `city, product`, never with an internal producer-slot error

#### Scenario: Non-member key in dimension position, computed-dimension name
- **WHEN** a query declares `ureg` = `upper(region)` and the dimension `rank(sum(amount, partition_by=[city, region]), partition_by=ureg, direction='desc')`
- **THEN** planning fails with the same error naming the key and the grain `city, region`, never with an internal error

#### Scenario: Member key executes
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=region, direction='desc')`
- **THEN** each row carries its (city, region) total's rank within the region: East Zeta 1, Delta 2, Epsilon 2; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap the NULL city 1, Kappa 2; Void Xi NULL (its total is NULL)

#### Scenario: Ungrained inner keeps the query-dimension rule
- **WHEN** a query over the banded dimension alone selects `rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** planning fails with the existing "partition_by column 'region' is not a query dimension" error listing the available dimensions; over `[region, band]` the same measure executes because the ungrained inner is grained at the query grain and `region` is a member

#### Scenario: Windowed inner admits the active bucket
- **WHEN** a monthly query selects `rank(sum(amount, window='1y', partition_by=customers.regions.name), partition_by=ordered_at, direction='desc')`
- **THEN** the partition key passes the operand-grain rule as the query's month bucket, a member contributed by the windowed inner

#### Scenario: Nested ranked aggregation drops its axis
- **WHEN** a monthly query selects `rank(last(sum(amount, partition_by=[customers.regions.name, ordered_at])), partition_by=ordered_at, direction='desc')`
- **THEN** planning fails with the operand-grain error naming `ordered_at` and the grain `customers.regions.name` — the `last` drops its ranking key, the month bucket; with `partition_by=customers.regions.name` the key passes the rule

#### Scenario: Aggregate-free input takes the query grain
- **WHEN** a query over `[city, region, product]` selects `rank(city, partition_by=region, direction='desc')`
- **THEN** it executes, ranking each row's city value descending within its region: East Zeta 1, Epsilon 2, Delta 3; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap Kappa 1, the NULL city NULL; Void Xi 1

#### Scenario: Residue error precedes the membership rule
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** planning fails with the existing grain-self-containment error ("must declare partition_by= explicitly"), not the operand-grain error

### Requirement: consecutive_periods counts calendar periods

`consecutive_periods(p)` SHALL count consecutive calendar buckets of the
query's active time bucket in which `p` holds. A bucket with no row in its
series is a gap and SHALL break the run exactly as a false or NULL predicate
does; the next bucket where `p` holds after a gap SHALL return 1. Runs SHALL be
counted per combination of the query's non-time dimensions. The series SHALL be
its operand's domain (`queries/transforms` › Transforms evaluate over their
operand's domain): the query's date range bounds it (no bucket before the
range contributes), a row-level filter that removes every row of a bucket makes
that bucket a gap, and a measure-typed filter SHALL NOT change any surviving
row's streak. A NULL time bucket SHALL be adjacent to no bucket: it returns 1
where `p` holds and 0 otherwise, and it never extends or starts another
bucket's run. Row-based `lag` / `lead` SHALL keep stepping over their
operand's present cells when nested with `consecutive_periods` in either direction.

Scenarios below use `orders` with one order per month for customer 1 in
2024-01/02/03/12 and 2025-01/02/03/05/06 with amounts 150, 500, 120, 220, 140,
200, 140, 225, 210 (April–November 2024 and April 2025 empty), and for
customer 2 in 2024-02/03/04/06/07/08 with amounts 300, 50, 300, 80, 300, 300
(May 2024 empty). "Month series of customer 1" means the query filters
`customer_id = 1` (or its data stands alone).

#### Scenario: Gaps in the middle of a series break the run

- **WHEN** a query over customer 1's month series requests
  `consecutive_periods(sum(amount) > 100)`
- **THEN** the values are 1, 2, 3, 1, 2, 3, 4, 1, 2 in month order — 2024-12
  and 2025-05 start new runs after the empty months — by executed values on
  SQLite and DuckDB

#### Scenario: Per-group series with different starts and ends

- **WHEN** a query grouped by `customer_id` and month requests
  `consecutive_periods(sum(amount) > 100)`
- **THEN** customer 1 reads 1, 2, 3, 1, 2, 3, 4, 1, 2 and customer 2 reads
  Feb 1, Mar 0, Apr 1, Jun 0, Jul 1, Aug 2 — customer 2's later start and
  earlier end never affect customer 1, and the false June after the empty May
  returns 0 — by executed values on SQLite and DuckDB

#### Scenario: Date range bounds the series

- **WHEN** the customer 1 query restricts the month time dimension with
  `"date_range": ["2025-02-01", "2025-06-30"]`
- **THEN** the values are Feb 1, Mar 2, May 1, Jun 2 — the true January
  outside the range does not extend the February run

#### Scenario: Row filter that empties a bucket makes a gap

- **WHEN** the customer 1 query adds the row filter `amount != 500`
- **THEN** February 2024 has no row and the values are 2024-01 1, 2024-03 1,
  2024-12 1, 2025-01 2, 2025-02 3, 2025-03 4, 2025-05 1, 2025-06 2

#### Scenario: Measure filter never changes a surviving streak

- **WHEN** the customer 1 query adds the measure-typed filter
  `sum(amount) > 200`
- **THEN** the surviving rows keep their unfiltered streaks: 2024-02 2,
  2024-12 1, 2025-05 1, 2025-06 2

#### Scenario: Predicate over a nested calendar transform

- **WHEN** the customer 1 query requests
  `consecutive_periods(change(sum(amount)) > 0)`
- **THEN** the values are 0, 1, 0, 0, 0, 1, 0, 0, 0 — `change` is NULL after
  each gap, so the gap-adjacent months are false

#### Scenario: Calendar transform over the streak

- **WHEN** the customer 1 query requests
  `change(consecutive_periods(sum(amount) > 100))`
- **THEN** the values are NULL, 1, 1, NULL, 1, 1, 1, NULL, 1 — NULL at the
  first month and after each gap

#### Scenario: Row-based lag over the streak

- **WHEN** the customer 1 query requests
  `lag(consecutive_periods(sum(amount) > 100))`
- **THEN** the values are NULL, 1, 2, 3, 1, 2, 3, 4, 1 — `lag` reads the
  previous present row's streak across each gap

#### Scenario: Streak over a row-based lag

- **WHEN** the customer 1 query requests
  `consecutive_periods(lag(sum(amount)) > 100)`
- **THEN** the values are 0, 1, 2, 1, 2, 3, 4, 1, 2 — the lagged predicate is
  row-based, the run it drives is calendar-based

#### Scenario: Downstream stage time dimension

- **WHEN** an inner stage aggregates customer 1's orders to month totals and an
  outer stage declares a month time dimension over the inner stage's month
  column and requests `consecutive_periods(sum(<total>) > 100)`
- **THEN** the values are 1, 2, 3, 1, 2, 3, 4, 1, 2 — identical to the
  model-backed query

#### Scenario: Every granularity uses its own calendar step

- **WHEN** the active time bucket is `second`, `minute`, `hour`, `day`,
  `week`, `week_sunday`, `month`, `quarter` or `year` and the series holds
  buckets 1, 2, 4 and 5 of that granularity (bucket 3 empty), each with a true
  predicate
- **THEN** the values are 1, 2, 1, 2 by executed values on SQLite and DuckDB

#### Scenario: NULL time bucket is adjacent to nothing

- **WHEN** customer 1's month series also holds an order with a NULL
  `order_date` and amount 300
- **THEN** the NULL bucket's streak is 1 and every dated month keeps the values
  1, 2, 3, 1, 2, 3, 4, 1, 2

#### Scenario: Server dialects execute the calendar streak

- **WHEN** the gapped-series query runs on PostgreSQL, MySQL, ClickHouse and
  SQL Server over a DATE-typed and a TIMESTAMP-typed time column, at `month`,
  `quarter` and `week_sunday`
- **THEN** each returns the same streaks as SQLite and DuckDB

#### Scenario: Generated SQL is pinned across dialects

- **WHEN** a gap-aware `consecutive_periods` query is rendered for the golden
  dialect set (postgres, sqlite, duckdb, tsql, bigquery)
- **THEN** the generated SQL matches recorded golden baselines, and every
  boolean-shaped predicate still appears only in condition positions

### Requirement: consecutive_periods rejects the period keyword

`consecutive_periods` SHALL reject a `period=` keyword argument with the
unsupported-keyword `ValueError` every other transform raises for a keyword it
does not accept; the counted period is always the query's active time bucket.

#### Scenario: period keyword rejected

- **WHEN** a query requests `consecutive_periods(sum(amount) > 100, period='year')`
- **THEN** the query fails with a `ValueError` naming the unsupported `period`
  keyword, instead of silently ignoring it

### Requirement: Sub-day time offsets on SQLite

On SQLite, a time offset at `hour`, `minute` or `second` granularity SHALL keep
the time of day, so `time_shift`, `change` and `change_pct` at sub-day buckets
read the calendar-shifted bucket exactly as on the other dialects. Offsets at
`day` and coarser, and `week_sunday` truncation, SHALL render as before.

#### Scenario: Hourly time_shift on SQLite

- **WHEN** a SQLite query over hourly buckets 01:00, 02:00, 03:00 and 05:00 of
  one day (sums 1, 2, 3, 5) requests `time_shift(sum(amount), -1)` and
  `change(sum(amount))`
- **THEN** `time_shift` reads NULL, 1, 2, NULL and `change` reads NULL, 1, 1,
  NULL — matching DuckDB

#### Scenario: Minute and second granularity on SQLite

- **WHEN** the same shape runs at `minute` and at `second` granularity on SQLite
- **THEN** each shifted value is the prior bucket's sum, NULL after a gap

#### Scenario: Day-and-coarser SQLite offsets are unchanged

- **WHEN** SQLite renders a time offset at `day`, `week`, `month`, `quarter` or
  `year`, or a `week_sunday` truncation
- **THEN** the rendered SQL is unchanged from before this change

### Requirement: time_shift offsets are exact on every dialect
`time_shift(x, periods, granularity)` SHALL look up the bucket whose start is the current bucket's
start moved by `periods` units with the same calendar rules as `date_add`: sub-day units keep the
time of day, and a `month` / `quarter` / `year` move landing past the target month's last day
clamps to that day. The looked-up bucket SHALL be identical on every Tier-1 dialect, SQLite
included.

#### Scenario: Hourly shift on SQLite
- **WHEN** a SQLite model `ev` has rows at 2024-01-01 10:15 (`v` 1), 11:20 (`v` 2) and 12:30 (`v` 4) and a query over an `hour` time dimension on `ts` selects `sum(v)` and `time_shift(sum(v), -1, 'hour')`
- **THEN** the shifted values are NULL at 10:00, 1 at 11:00 and 2 at 12:00

#### Scenario: Month shift of a month-end daily bucket
- **WHEN** a query over a `day` time dimension has rows dated 2024-02-29 (`v` 10), 2024-03-02 (`v` 5) and 2024-03-31 (`v` 1) and selects `time_shift(sum(v), -1, 'month')`
- **THEN** on every dialect the shifted value at 2024-03-31 is 10 (the 2024-02-29 bucket), never 5

### Requirement: Time-ordered transforms step by custom granularities

When the query's active time bucket is a custom granularity (`queries/custom-granularities`), one calendar step of `time_shift`, `change`, `change_pct` and `consecutive_periods` SHALL be `multiple` units of its base, so a bucket's neighbour is the adjacent custom bucket and an absent one is a gap. `time_shift(x, n, g)` SHALL accept a custom granularity `g` as its unit, moving the bucket start by `n × multiple` base units with the `date_add` calendar rules and reading the bucket containing the moved instant.

#### Scenario: Shift by the fiscal year

- **WHEN** a query over `orders` rows dated 2024-03-15 (10), 2024-04-02 (20), 2025-03-31 (30), 2025-04-01 (40) groups by `order_date` at `fiscal_year` (April origin) and selects `time_shift(sum(amount), -1)` and `change(sum(amount))`
- **THEN** the shifted values are FY2023 NULL, FY2024 10, FY2025 50 and the changes NULL, 40, -10, on SQLite and DuckDB

#### Scenario: Custom unit on a finer axis

- **WHEN** the same rows are grouped by month and the query selects `time_shift(sum(amount), -1, 'fiscal_year')`
- **THEN** 2024-03 and 2024-04 read NULL, 2025-03 reads 10 and 2025-04 reads 20 — one fiscal-year step is twelve months, so the result equals `time_shift(sum(amount), -1, 'year')`

#### Scenario: Streaks count custom buckets

- **WHEN** rows fall in sprints starting 2025-01-06, 2025-01-20 and 2025-02-17 (the 2025-02-03 sprint empty) and the query groups at `sprint` with `consecutive_periods(count(*) > 0)`
- **THEN** the streaks are 1, 2, 1

### Requirement: Rank-family ordering direction

`rank` and `dense_rank` SHALL take a required keyword argument `direction` whose
value is a string literal `asc`, `desc`, `ascending` or `descending` (any case,
surrounding whitespace ignored), normalised to `asc` / `desc`, and SHALL order
their window by the inner value ascending for `asc` and descending for `desc`.
`direction` SHALL combine with `partition_by=` and SHALL accept any orderable
inner, numeric or not. `ntile` and `percent_rank` SHALL reject `direction` and
SHALL always order ascending, so `ntile` bucket 1 holds the lowest values and a
higher value never gets a lower `percent_rank`. A missing, unrecognised,
non-literal or forbidden `direction` SHALL fail before any SQL runs with a
`TransformArgumentError` (a `QueryTypeError`), in every position (measure,
filter, order, computed dimension, aggregation parameter, saved `DatasetMetric`);
the missing-direction message SHALL show both
`direction='asc'` (lowest first) and `direction='desc'` (highest first). The
importer formula validator SHALL apply the identical rule with the identical
error. `rank(x, direction='asc')` and `rank(x, direction='desc')` SHALL be
distinct values that never deduplicate into one.

Values below use the DEV-1847 `sales` fixture, whose region totals are North
90, South 140, East 180, Gap 20 and Void NULL.

#### Scenario: Ascending rank puts the lowest value first

- **WHEN** a query over `[region]` selects `rank(sum(amount), direction='asc')`
- **THEN** the window orders the inner ascending and the ranks are Gap 1, North 2,
  South 3, East 4, Void NULL on SQLite and DuckDB

#### Scenario: Descending rank puts the highest value first

- **WHEN** a query over `[region]` selects `rank(sum(amount), direction='desc')`
- **THEN** the window orders the inner descending and the ranks are East 1, South 2,
  North 3, Gap 4, Void NULL

#### Scenario: dense_rank takes the same direction

- **WHEN** a query over `[region]` selects `dense_rank(sum(amount), direction='asc')`
- **THEN** the ranks are Gap 1, North 2, South 3, East 4, Void NULL

#### Scenario: Direction synonyms normalise

- **WHEN** a query selects `rank(sum(amount), direction=' Descending ')`
- **THEN** it binds to the identical value as `rank(sum(amount), direction='desc')`

#### Scenario: Non-numeric inner ranks ascending

- **WHEN** a query over `[region]` selects `rank(min(city), direction='asc')`
- **THEN** the ranks are North 1 and South 1 (both `Alpha`), East 3 (`Delta`), Gap 4
  (`Kappa`), Void 5 (`Xi`)

#### Scenario: Direction combines with partition_by

- **WHEN** a query over `[region, city]` selects
  `rank(sum(amount), partition_by=region, direction='asc')`
- **THEN** each city ranks lowest-first within its region: East Delta 1, Epsilon 1,
  Zeta 3; North Alpha 1, Beta 2; South Alpha 1, Gamma 2; Gap Kappa 1, the NULL city
  2; Void Xi NULL

#### Scenario: Both directions in one query stay distinct

- **WHEN** one query selects both `rank(sum(amount), direction='asc')` and
  `rank(sum(amount), direction='desc')` unnamed
- **THEN** the result carries two columns with the ascending and descending ranks
  above, never one deduplicated column

#### Scenario: Missing direction fails naming both spellings

- **WHEN** a query selects, filters on, or orders by `rank(sum(amount))` or
  `dense_rank(sum(amount), partition_by=region)`, or queries a saved
  `DatasetMetric` whose formula is `rank(sum(amount))`
- **THEN** it fails with a `TransformArgumentError` naming the transform and showing
  `direction='asc'` (lowest first) and `direction='desc'` (highest first), and no SQL
  runs

#### Scenario: Unrecognised or non-literal direction fails

- **WHEN** a query selects `rank(sum(amount), direction='up')` or
  `rank(sum(amount), direction=region)`
- **THEN** it fails with a `TransformArgumentError` listing the accepted values

#### Scenario: ntile and percent_rank reject direction

- **WHEN** a query selects `ntile(sum(amount), n=2, direction='desc')` or
  `percent_rank(sum(amount), direction='asc')`
- **THEN** it fails with a `TransformArgumentError` stating that the transform always
  orders ascending and takes no `direction`

#### Scenario: ntile and percent_rank order ascending

- **WHEN** a query over `[region]` selects `ntile(sum(amount), n=2)` and
  `percent_rank(sum(amount))`
- **THEN** `ntile` is Gap 1, North 1, South 2, East 2, Void NULL and `percent_rank` is
  Gap 0, North 1/3, South 2/3, East 1, Void NULL

#### Scenario: Importer validation shares the rule

- **WHEN** the importer formula validator parses `rank(sum(amount))`,
  `rank(sum(amount), direction='sideways')` or `ntile(sum(amount), n=4, direction='asc')`
- **THEN** each fails with the same `TransformArgumentError` the query binder raises
  for the same formula

#### Scenario: Window ordering is pinned across dialects

- **WHEN** `rank` with each direction, `dense_rank`, `ntile` and `percent_rank` are
  rendered for postgres, sqlite, duckdb, tsql and bigquery
- **THEN** each window orders the inner by the stated direction (`ASC` for `ntile` /
  `percent_rank`) and the generated SQL matches recorded golden baselines

### Requirement: Rank-family NULL inputs rank NULL

For `rank`, `dense_rank`, `ntile` and `percent_rank`, a row whose inner value is
NULL SHALL get a NULL result. NULL rows SHALL NOT take a rank position or an
`ntile` bucket, nor count in `percent_rank`'s denominator, within each partition;
the non-NULL rows SHALL rank exactly as if the NULL rows were absent. The result
SHALL be identical on every supported dialect, independent of the dialect's
native NULL ordering.

#### Scenario: Mixed NULL and non-NULL inners

- **WHEN** a query over `[region]` selects `rank(sum(amount), direction='desc')`,
  `percent_rank(sum(amount))` and `ntile(sum(amount), n=2)` on the `sales` fixture
- **THEN** Void (NULL total) gets NULL for all three, and the other regions get the
  values of the ordering-direction scenarios above, `percent_rank`'s denominator
  counting four rows, on SQLite and DuckDB

#### Scenario: An all-NULL partition ranks NULL without disturbing others

- **WHEN** a query over `[region, city]` selects
  `dense_rank(sum(amount), partition_by=region, direction='desc')`
- **THEN** Void Xi (NULL total) is NULL, and every other region's cities rank as they
  would without Void

#### Scenario: A NULL row inside a partition takes no position

- **WHEN** a query over `[region, city]` selects
  `rank(city, partition_by=region, direction='asc')`
- **THEN** Gap's NULL city is NULL and Kappa is 1; East Delta 1, Epsilon 2, Zeta 3

#### Scenario: A filter on rank drops NULL-ranked rows

- **WHEN** a query over `[region]` filters `rank(sum(amount), direction='asc') <= 5`
- **THEN** East, Gap, North and South survive and Void does not

#### Scenario: NULL handling does not depend on the dialect's NULL ordering

- **WHEN** the rank family is rendered for tsql, whose native ordering puts NULLs
  first on `ASC`
- **THEN** the emitted SQL nulls the result for a NULL inner and keeps NULL rows out of
  the non-NULL rows' window, exactly as on postgres

### Requirement: Stored rank calls without a direction load as descending

A persisted model, query or memory whose stored schema version predates this
change SHALL load with every `rank(` / `dense_rank(` call lacking a top-level
`direction=` in its Mode-B fields rewritten to carry `direction='desc'`,
preserving its pre-change ordering. The Mode-B fields are `DatasetMetric.expression`
and a query's `measures`, `filters`, `dimensions`, `time_dimensions`, `order` and
`main_time_dimension`, including queries nested in `source_queries`, the measures of a
stored query's extensions (in either stored spelling), and `Memory.query`. Mode-A SQL (`DatasetField.sql`, model
`filters`, `DatasetField.filter`, aggregation templates) and `ntile` / `percent_rank`
calls SHALL never be rewritten. The rewrite SHALL apply only to a payload read
from storage or one that declares an explicit schema version older than the
current one; a payload without a version, or at the current version, SHALL be
left as written, so a bare call in it fails with the missing-direction error. The
rewrite SHALL be idempotent, SHALL leave a formula it cannot tokenise
byte-identical, and a migrated model SHALL be persisted at the current
version only by a save or `slayer storage upgrade`.

#### Scenario: A stored model measure keeps its descending meaning

- **WHEN** a model stored at the previous version holds the measure
  `rank(sum(amount))` and is loaded
- **THEN** the measure reads `rank(sum(amount), direction='desc')`, the stored document
  is unchanged by the load, and querying it returns the descending ranks

#### Scenario: A stored query's every Mode-B field is rewritten

- **WHEN** a stored query-backed model's `source_queries` entry holds bare
  `rank(` / `dense_rank(` calls in a measure, a filter, an order item and a computed
  dimension expression, nested inside other calls and colon syntax
- **THEN** every call gains `direction='desc'` and nothing else in the formulas changes

#### Scenario: Unversioned legacy documents are rewritten

- **WHEN** a stored model with no `version`, whose nested source query also has no
  `version`, or a stored memory (YAML and SQLite) with no `version` or with an
  unversioned `query`, holds bare `rank(` calls
- **THEN** they load with `direction='desc'` filled in

#### Scenario: A fresh payload is never filled in

- **WHEN** a query or model with no `version`, or at the current version, is
  submitted through the API, MCP or Python with a bare `rank(sum(amount))`
- **THEN** it fails with the missing-direction `TransformArgumentError`

#### Scenario: A payload declaring an old version is treated as legacy

- **WHEN** a query submitted with an explicit older `version` holds `rank(sum(amount))`
- **THEN** it is filled in with `direction='desc'`, exactly as a stored document would be

#### Scenario: Calls that need no rewrite are untouched

- **WHEN** a stored document holds `rank(sum(amount), direction='asc')`, the text
  `rank(` inside a string literal, an attribute call `x.rank(`, `ntile(sum(amount), n=4)`,
  `percent_rank(sum(amount))`, or `dense_rank() over (order by id)` in a `DatasetField.sql`
- **THEN** each is left byte-identical

#### Scenario: The rewrite is idempotent

- **WHEN** an already-migrated document is migrated again
- **THEN** it is unchanged

#### Scenario: An untokenisable formula still loads

- **WHEN** a stored model's measure formula cannot be tokenised
- **THEN** the formula is left byte-identical and the model still loads

### Requirement: Transforms over a constant input

A window transform — `cumsum`, `lag`, `lead`, `change`, `change_pct`, `time_shift`,
`consecutive_periods`, `rank`, `dense_rank`, `percent_rank` and `ntile` — whose input is a
bare literal SHALL execute at the query grain, treating the literal as the same value in
every result row, and SHALL NOT fail with an internal error. `first` and `last` are
aggregations, not transforms, and are outside this requirement.

#### Scenario: Running and offset transforms over a literal
- **WHEN** a query rooted at `orders` with a month time dimension selects `sum(amount)`
  and, in turn, `cumsum(1)`, `lag(1)`, `lead(1)`, `change(1)`, `change_pct(1)` and
  `time_shift(1, -1)`
- **THEN** each executes on SQLite and DuckDB with hand-computed values: `cumsum(1)` is
  the running count of months within each partition, `lag(1)` is NULL in the first month
  and 1 after, `lead(1)` is 1 except NULL in the last month, `change(1)` is NULL in the
  first month and 0 after, `change_pct(1)` is NULL in the first month and 0 after, and
  `time_shift(1, -1)` is 1 wherever the previous month exists

#### Scenario: consecutive_periods over a literal predicate
- **WHEN** the same query selects `consecutive_periods(1 > 0)`
- **THEN** it executes on SQLite and DuckDB and each month carries its calendar-period
  run length, by hand-computed values

#### Scenario: Rank family over a literal
- **WHEN** a query grouped by a dimension selects `rank(1, direction='desc')`,
  `dense_rank(1, direction='desc')` and `percent_rank(1)`, with and without
  `partition_by=`
- **THEN** every row carries 1, 1 and 0 respectively on SQLite and DuckDB

#### Scenario: ntile over a literal
- **WHEN** a query grouped by a dimension selects `ntile(1, n=2)`
- **THEN** it executes on SQLite and DuckDB and the multiset of bucket numbers is the
  balanced split of the rows into two buckets; which row lands in which bucket is not
  specified, since every input value ties

### Requirement: A time-ordered transform over extraction dimensions names the date_trunc remedy

When a time-ordered transform (`time_shift`, `change`, `change_pct`, `cumsum`,
`lag`, `lead`, `consecutive_periods`), a `first` / `last` over an aggregate
defaulting its ranking key to the active time bucket, or a windowed aggregate fails with the existing typed no-time-dimension error because
the query projects no time dimension, and a dimension of the query is a
date-part extraction whose part is a built-in granularity (`year(x)`,
`quarter(x)`, `month(x)`, `week(x)`, `day(x)`, `hour(x)`, `minute(x)`,
`second(x)`, or `date_part('<part>', x)` with such a part, any case) of a DATE-
or TIMESTAMP-typed value, the error SHALL additionally name the remedy
`date_trunc('<part>', <expr>)` built from that extraction's part and argument,
one per such dimension. The failure SHALL otherwise be unchanged — same error
type, raised before any SQL is generated — and a query with no such dimension
SHALL keep the existing message. An extraction dimension SHALL never serve as a
transform's or window's time axis.

#### Scenario: Running total over a month-of-year dimension

- **WHEN** a query on `orders` has `dimensions=["month(created_at)"]`, no time
  dimension, and selects `cumsum(sum(amount))`
- **THEN** planning fails with the typed no-time-dimension error whose message
  names `date_trunc('month', created_at)`, and no SQL is generated

#### Scenario: Shift family and windowed aggregate name the remedy

- **WHEN** a query has `dimensions=["year(created_at)"]`, no time dimension, and
  selects `change(sum(amount))`, `last(sum(amount))`, or `sum(amount, window='3m')`
- **THEN** each fails with the typed no-time-dimension error naming
  `date_trunc('year', created_at)`

#### Scenario: The remedy executes

- **WHEN** the running-total query instead has
  `dimensions=["date_trunc('month', created_at)"]`
- **THEN** it executes, accumulating `sum(amount)` along the month buckets, by
  executed values

#### Scenario: No extraction keeps the existing message

- **WHEN** a query has `dimensions=["status"]`, no time dimension, and selects
  `cumsum(sum(amount))`
- **THEN** it fails with the existing no-time-dimension error, whose message
  names no `date_trunc` remedy

### Requirement: Transforms evaluate over their operand's domain
Every transform — the rank family (`rank`, `dense_rank`, `percent_rank`, `ntile`), `cumsum`, `lag`, `lead`, `time_shift`, `change`, `change_pct` and `consecutive_periods` — SHALL evaluate over its operand's domain in every position — dimension, measure, filter, order key, aggregation source and aggregation parameter, at any depth — and whether or not the query names `population`. In the first four its result attaches to the query's cells on its result grain; inside an aggregation, the aggregation reads the transform's cells at its result grain, never the rows of the aggregation's own operand, so the transform reads the same value in every position. The domain SHALL be the cells of the operand's constituents at the operand grain: an aggregate constituent's cells are the grain tuples its filtered home rows reach (rows hidden by a model filter or a row-level-security policy do not exist); a windowed aggregate constituent's cells are those of the same aggregate without `window=`; a nested transform constituent's cells are its own result cells; constituents at the full operand grain supply their cells, and a coarser constituent's cells extend to the agreeing cells of the finer constituents, else of the query's dimension anchor; an operand with no aggregate constituent takes the dimension anchor's cells. In a spine query the domain SHALL be the operand's non-time tuples that have a cell in a visible bucket, each crossed with every bucket in range. A query cell outside the domain SHALL read NULL for the transform. The query's rows, its other measures, its measure-typed filters, its order, and the dimensions, sibling constituents and parameters of an aggregation enclosing the transform SHALL NOT change a transform's value in any cell, and a transform used only in a filter or the order SHALL supply no rows. Ranking, row offsets (`lag`, `lead`) and running totals SHALL step over the domain's cells only; calendar-step transforms keep stepping by calendar buckets. A NULL time bucket SHALL have no position on the time axis on every dialect: it is adjacent to no bucket, so a preserving time-ordered transform evaluates it as a series of its own (`cumsum` reads its own value; `lag`, `lead`, `time_shift`, `change` and `change_pct` read NULL there), and it never contributes to or reads from another bucket's value; the rank family treats it as an ordinary cell. A `first` / `last` over an aggregate is an aggregation, not a transform, and ranks exactly this domain's cells (`queries/semantics` › first and last over an aggregate rank their operand's cells).

Scenarios marked "the Appendix B seed" use customers C1 EAST retail, C2 EAST corporate, C3 WEST retail, C4 NORTH retail; orders C1 100 (2025-01-10), C1 200 (2025-02-10), C2 50 (2025-01-20), C3 300 (2025-02-20); returns C1 25, C4 10. "Daily sales" uses `sales` 10 on 2025-01-05 and 30 on 2025-03-05, and `refunds` 5 on 2025-02-05, both joining a `days` dataset. "The city seed" uses one dataset `orders` with North/A 10 (2025-01-10) and 25 (2025-02-10), North/B 30 (2025-01-11) and 40 (2025-02-11), South/C 50 (2025-01-12), South/D 20 (2025-01-13) and 40 (2025-02-13): city totals A 35, B 70, C 50, D 60; region totals North 105, South 110; region-month totals North 40 and 65, South 70 and 40.

#### Scenario: A sibling that adds rows leaves rank-family values unchanged
- **WHEN** a query without `population` on the Appendix B seed groups by `customers.region` and selects `percent_rank(count(orders.*))` and `ntile(count(orders.*), n=2)`, once alone and once with `returns.total_returns` added
- **THEN** both runs read EAST (1, 2) and WEST (0, 1), and the NORTH row the sibling adds reads NULL for both transforms

#### Scenario: lag skips gaps without the spine
- **WHEN** a daily-sales query by month of `days.day` selects `lag(sum(sales.amount))`, once alone and once with `sum(refunds.amount)` added
- **THEN** January reads NULL and March reads 10 in both runs, and the February row the sibling adds reads NULL

#### Scenario: A nested transform is a constituent at its result grain
- **WHEN** the daily-sales query selects `lag(cumsum(sum(sales.amount)))`, once alone and once with `sum(refunds.amount)` added
- **THEN** January reads NULL and March reads 10 in both runs: `lag` steps over the running total's own cells

#### Scenario: A collapsing pick reads the operand's cells
- **WHEN** the daily-sales query selects `last(sum(sales.amount))`, once alone and once beside `sum(refunds.amount)` with a refund dated in April
- **THEN** every row of both runs reads 30, the latest month with sales — the second-order `last` ranks this domain's cells — never the April row the sibling adds

#### Scenario: A coarser operand ranks its own cells
- **WHEN** a query without `population` on the Appendix B seed groups by `customers.region` and `customers.segment` and selects only `rank(sum(orders.amount, partition_by=customers.region), direction='desc')`
- **THEN** the rows are (EAST, retail) 1, (EAST, corporate) 1 and (WEST, retail) 2: the two regions with orders are ranked and broadcast, and NORTH supplies no row

#### Scenario: A mixed-grain operand ranks the union-grain cells
- **WHEN** a query on the Appendix B seed groups by `customers.region` and `customers.segment` and selects `rank(sum(orders.amount, partition_by=customers.region) - sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc')`
- **THEN** exactly the cells (EAST, retail), (EAST, corporate) and (WEST, retail) are ranked, the region totals broadcast onto them, and the values are correct by executed values

#### Scenario: A named population does not widen the domain
- **WHEN** a query on the Appendix B seed names `population: customers`, groups by `customers.region` and selects `percent_rank(count(orders.*))`
- **THEN** EAST reads 1, WEST 0 and NORTH NULL

#### Scenario: A hand-made calendar population is not a dense series
- **WHEN** a query on the spine scenario datasource names `population: calendar` (one row per day, Jan–Jun 2025, joined from `return_date`), groups by month of `calendar.date` and selects `lag(sum(returns.amount))`
- **THEN** the months read NULL, NULL, 20, NULL, 10, NULL — `lag` steps over the months with returns only — while the same measure over the spine reads NULL, NULL, 20, 10, NULL, 5

#### Scenario: Filter and order positions read the same values
- **WHEN** the Appendix B query by `customers.region` with `orders.total_revenue` and `returns.total_returns` filters `percent_rank(count(orders.*)) = 1`, and another orders by `percent_rank(count(orders.*))` descending
- **THEN** the filter keeps exactly the EAST row, the order puts EAST before WEST, and neither changes any surviving value

#### Scenario: A filter-only transform supplies no rows
- **WHEN** a query without `population` on the Appendix B seed groups by `customers.region`, selects only `returns.total_returns` and filters `percent_rank(count(orders.*)) = 1`
- **THEN** the only row is EAST 25: WEST, which only the transform's domain holds, never becomes a row

#### Scenario: A NULL time bucket is adjacent to no bucket
- **WHEN** a query rooted at `customers` selects `cumsum(sum(customers.spend))` and `lag(sum(customers.spend))` by month of `orders.ordered_at`, a customer without orders (spend 55) falling in the NULL bucket beside monthly sums 100, 150, 100, 110 for 2024-01 to 2024-04
- **THEN** on SQLite and DuckDB alike the running total reads 100, 250, 350, 460 and 55 in the NULL bucket, and `lag` reads NULL, 100, 150, 100 and NULL in the NULL bucket

#### Scenario: A transform over a windowed aggregate reads the aggregate's own cells
- **WHEN** over the association seed of `queries/population` a query without `population` groups by `orders.status` and the month of `customers.signup_at` and selects `count(customers.*, window='60d')` as `w` with, in turn, `lag(w)`, `cumsum(w)`, `time_shift(w, -1)` and `change(w)` spelled over the same aggregate
- **THEN** on SQLite and DuckDB the cells (open, January), (open, March), (paid, January), (paid, February), (NULL, April) and (NULL, May) read `w` 1, 1, 1, 2, 1, 1; `lag` NULL, 1, NULL, 1, NULL, 1; `cumsum` 1, 2, 1, 3, 1, 2; `time_shift` NULL, NULL, NULL, 1, NULL, 1 — (open, February) is no cell of the aggregate, so (open, March) reads NULL —; and `change` NULL, NULL, NULL, 1, NULL, 0

#### Scenario: A sibling or a named population leaves a transform over a windowed aggregate unchanged
- **WHEN** each query of the previous scenario runs again beside `sum(orders.amount)`, and again naming `population: customers`; and a query by `products.category` and the month of `orders.ordered_at` selects `cumsum(count(customers.*, window='60d'))` alone, beside `sum(orders.amount)` and naming `population: customers`
- **THEN** every transform reads, cell by cell, its value in the query where it is the only metric; the second query's running total reads (food, March) 1, (toys, January) 1, (toys, February) 3, (NULL, April) 1 and (NULL, NULL) 0 in all three runs

#### Scenario: Over the spine, a transform over a windowed aggregate steps over its home's tuples
- **WHEN** over the association seed a query without `population` groups by `orders.status` and the month of `time_spine.timestamp` with `date_range` 2024-01-01 to 2024-06-30 and selects `time_shift(count(customers.*, window='60d'), -1)`, once alone, once beside `sum(orders.amount)` and once naming `population: customers`
- **THEN** on SQLite and DuckDB all three runs read, January to June, `open` 0, 1, 1, 1, 0, 0; `paid` 0, 1, 2, 1, 0, 0; and NULL 0, 0, 0, 0, 1, 1: the NULL tuple, which only `customers` reaches, is in the series, and every bucket the frame reaches carries its value

#### Scenario: Inside an aggregation, a coarser operand ranks its own cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `rank(sum(orders.amount, partition_by=orders.region), direction='desc')` as `r` and `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` as `m`
- **THEN** on SQLite and DuckDB every row reads `r` = `m`, North 2 and South 1, and `m` carries the `associated` warning naming `city`

#### Scenario: Inside an aggregation, a re-aggregated operand ranks its own cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `max(rank(max(sum(orders.amount, partition_by=[orders.region, orders.city]), partition_by=orders.region), direction='desc'))`
- **THEN** every North row reads 1 and every South row 2

#### Scenario: Inside an aggregation, a windowed operand collapsed by last ranks its own cells
- **WHEN** a query on the city seed groups by `orders.region` and the month of `orders.ordered_at` and selects `max(rank(last(sum(orders.amount, window='2m', partition_by=[orders.region, orders.ordered_at])), direction='desc'))`
- **THEN** both North months read 2 and both South months 1

#### Scenario: A finer sibling constituent does not widen a transform's domain
- **WHEN** a query on the city seed groups by `orders.region` and selects `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc') + sum(orders.amount, partition_by=[orders.region, orders.city]))`
- **THEN** North reads 72 and South 61

#### Scenario: Every aggregation and rank-family member reads the transform's cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `sum(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` and `max(percent_rank(sum(orders.amount, partition_by=orders.region)))`
- **THEN** the North rows read 2 and 0 and the South rows 1 and 1

#### Scenario: As an aggregation parameter a transform reads its own cells
- **WHEN** a query on the city seed with no dimensions selects `weighted_avg(sum(orders.amount, partition_by=[orders.region, orders.city]), weight=T)` for `T` in turn `rank(sum(orders.amount, partition_by=orders.region), direction='desc')`, `rank(max(sum(orders.amount, partition_by=[orders.region, orders.city]), partition_by=orders.region), direction='desc')` and `percent_rank(sum(orders.amount, partition_by=orders.region))`; and a query grouped by the month of `orders.ordered_at` selects `weighted_avg(sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at]), weight=rank(last(sum(orders.amount, window='2m', partition_by=[orders.region, orders.ordered_at])), direction='desc'))`
- **THEN** the first reads 320/6, the second 325/6 and the third 55; the monthly query reads January 25 and February 34

#### Scenario: Filter and order read a transform inside an aggregation at its metric-position value
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city`, selects `sum(orders.amount)` and filters `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc')) = 2`; and the same query without the filter orders by `max(rank(sum(orders.amount, partition_by=orders.region), direction='desc'))` ascending
- **THEN** the filter keeps exactly the rows North/A and North/B, and the order puts both South rows before both North rows

#### Scenario: Transforms already evaluated over their own cells keep their values
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `sum(orders.amount * rank(sum(orders.amount, partition_by=orders.region), direction='desc'))`; a query grouped by `orders.region` and the month of `orders.ordered_at` selects `max(cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at])) + 0 * sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at]))`; a query grouped by `orders.region`, `orders.city` and the month of `orders.ordered_at` selects `max(time_shift(cumsum(sum(orders.amount, partition_by=[orders.region, orders.ordered_at])), -1))` and `max(time_shift(rank(sum(orders.amount, partition_by=[orders.region, orders.ordered_at]), direction='desc'), -1))`; and a query without `population` on the Appendix B seed groups by `customers.region` and selects `sum(rank(sum(returns.amount), direction='desc'))`
- **THEN** the first reads A 70, B 140, C 50, D 60; the second North 40 and 105, South 70 and 110; the third reads NULL in January and, in February, 40 and 3 on the North rows and 70 and 1 on the South row; and the fourth reads EAST 1 and NORTH 2, the NORTH cell, which only `returns` reaches, present

### Requirement: An ungrained inner aggregate types at the query grain

An aggregate with no `partition_by=` SHALL have the query grain wherever it stands in a measure, filter or order key: directly under a transform, beside coarser or finer aggregates in a transform's operand, inside a second-order aggregation under a transform, inside a transform used as an aggregation source or aggregation parameter, and in the operand of a `first` / `last` over an aggregate — at any depth, and whether it reads the population's dataset, another dataset, or carries `window=` (a windowed one's grain includes the query's active time bucket). Every such query SHALL return the same values as the spelling that writes the query grain on that aggregate with `partition_by=`. A transform's operand grain SHALL be the union of its inner aggregates' grains so read, and the transform SHALL evaluate at that grain in every position.

A grain the aggregate has by default SHALL stay unwritten for the mode axis: an unattributable query dimension SHALL resolve per `to_many_handling` exactly as for the same aggregate standing alone — associated with the `associated` warning when the mode is omitted or `"associate"`, broadcast with its warning under the deprecated `"broadcast"`, refused under `"error"` — and no warning or error SHALL name a `partition_by=` the query did not write. When the query also writes the same aggregate with that grain, the two are one value and it SHALL resolve as written. Dimension position SHALL keep its existing rule (`queries/computed-dimensions`): an aggregate with no `partition_by=` there is refused.

Scenarios marked "the city seed" use one dataset `orders(id, customer_id, region, city, status, amount, ordered_at)` with rows North/A 10 (2025-01) and 25 (2025-02), North/B 30 (2025-01) and 40 (2025-02), South/C 50 (2025-01), South/D 20 (2025-01) and 40 (2025-02); `share` abbreviates `sum(orders.amount) / sum(orders.amount, partition_by=orders.region)`. Scenarios marked "the Appendix B seed" use the seed of "Transforms evaluate over their operand's domain". Every scenario holds by executed values on SQLite and DuckDB.

#### Scenario: The rank family over a mixed operand ranks the query's cells
- **WHEN** a query on the city seed groups by `orders.region` and `orders.city` and selects `rank(share, direction='desc')`, `dense_rank(share, direction='desc')`, `percent_rank(share)` and `ntile(share, n=2)`
- **THEN** for A, B, C, D the ranks and dense ranks are 4, 1, 3, 2, the percent ranks 0, 1, 1/3, 2/3 and the tiles 1, 2, 1, 2, each equal to the spelling with `sum(orders.amount, partition_by=[orders.region, orders.city])` as the numerator

#### Scenario: The same rank as a filter and as an order key
- **WHEN** the query selects `sum(orders.amount)` and filters `rank(share, direction='desc') = 1`, and another orders by `rank(share, direction='desc')` ascending
- **THEN** the filter keeps exactly North/B, and the order is B, D, C, A

#### Scenario: Other spellings of the mixed operand
- **WHEN** the query selects `rank(sum(orders.amount) - sum(orders.amount, partition_by=orders.region), direction='desc')`, the rank of a dataset metric defined as `share`, `rank(count(orders.*) / sum(orders.amount, partition_by=orders.region), direction='desc')`, and `rank(sum(orders.amount) / sum(orders.amount, partition_by=orders.region) + sum(orders.amount, partition_by=[]), direction='desc')`
- **THEN** each equals its explicit-grain spelling; the count form reads 1, 1, 4, 3 for A, B, C, D; the three-way form reads 4, 1, 3, 2

#### Scenario: A month bucket joins the query grain
- **WHEN** the city-seed query groups by `orders.region`, `orders.city` and month of `orders.ordered_at` and selects `rank(share, direction='desc')`, and again with `partition_by=orders.region` on the rank
- **THEN** in cell order (North A Jan, A Feb, B Jan, B Feb, South C Jan, D Jan, D Feb) the ranks are 7, 5, 4, 2, 1, 6, 3, and within regions 4, 3, 2, 1, 1, 3, 2

#### Scenario: Time-ordered transforms over a mixed operand
- **WHEN** a city-seed query groups by `orders.region` and month of `orders.ordered_at` and selects `cumsum(share)`, `lag(share)`, `lead(share)` and `consecutive_periods(share > 0.5)`
- **THEN** for North Jan, North Feb, South Jan, South Feb `cumsum` reads 0.381, 1.0, 0.636, 1.0; `lag` NULL, 0.381, NULL, 0.636; `lead` 0.619, NULL, 0.364, NULL; `consecutive_periods` 0, 1, 1, 0 — no internal planner error

#### Scenario: Nested transforms over a mixed operand
- **WHEN** the same query selects `rank(cumsum(sum(orders.amount)) / sum(orders.amount, partition_by=orders.region), direction='desc')` and `cumsum(rank(share, direction='desc'))`
- **THEN** they read 4, 1, 3, 1 and 3, 5, 1, 5

#### Scenario: Shapes that already work keep their values
- **WHEN** the same query selects `time_shift(share, -1)`, `change(share)`, `change_pct(share)`, `first(share)`, `last(share)`, a rank over `sum(orders.amount) / sum(orders.amount, partition_by=[])`, a mixed operand whose numerator is `sum(orders.amount, window='2m')`, and `time_shift` of an operand mixing an aggregate grained by region and month with the dimension `orders.region`
- **THEN** each returns the values it returns before this change, equal to its explicit-grain spelling

#### Scenario: A row-level leaf beside a coarser aggregate
- **WHEN** a city-seed query groups by `orders.region` and `orders.city` and selects `rank(sum(orders.amount, partition_by=orders.region) * iif(orders.city = 'A', 1, 2), direction='desc')`
- **THEN** A, B, C, D read 4, 3, 1, 1 — no internal planner error

#### Scenario: A second-order aggregation under a transform keeps its inner at the query grain
- **WHEN** a city-seed query groups by `orders.region` and `orders.city` and selects `rank(max(sum(orders.amount), partition_by=orders.region), direction='desc')`
- **THEN** North reads 1 and South 2 on every row (the largest city total is 70 in North and 60 in South), equal to the spelling with `sum(orders.amount, partition_by=[orders.region, orders.city])`

#### Scenario: A cross-dataset second-order aggregation under a transform
- **WHEN** a query on the Appendix B seed names `population: customers`, groups by `customers.region` and `customers.segment` and selects `rank(max(sum(orders.amount), partition_by=customers.region), direction='desc')`
- **THEN** the EAST rows and the WEST row read 1 and NORTH reads NULL, equal to the spelling with `sum(orders.amount, partition_by=[customers.region, customers.segment])`

#### Scenario: A finer operand with a windowed inner as an aggregation source
- **WHEN** a city-seed query groups by `orders.region` and month of `orders.ordered_at` and selects `max(rank(sum(orders.amount, window='2m') / sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at]), direction='desc'))`
- **THEN** North Jan, North Feb, South Jan, South Feb read 7, 5, 6, 4

#### Scenario: A finer operand with a windowed inner under last
- **WHEN** the same query selects `max(last(sum(orders.amount, window='2m') / sum(orders.amount, partition_by=[orders.region, orders.city, orders.ordered_at])))`
- **THEN** the four cells read 4.2, 4.2, 2.75, 2.75

#### Scenario: A finer operand with a cross-dataset inner
- **WHEN** a query on the Appendix B seed names `population: customers`, groups by `customers.region` and selects `max(rank(sum(orders.amount) / sum(orders.amount, partition_by=[customers.region, customers.segment]), direction='desc'))`, and another adds month of `orders.ordered_at` and selects `max(last(sum(orders.amount) / sum(orders.amount, partition_by=[customers.region, customers.segment, orders.ordered_at])))`
- **THEN** the first reads EAST 2, WEST 3 and NORTH NULL; in the second EAST January reads 3.0

#### Scenario: Windowed inners nested in transforms inside an aggregation
- **WHEN** a city-seed query groups by `orders.region` and month of `orders.ordered_at` and selects `max(rank(cumsum(sum(orders.amount, window='2m')) / sum(orders.amount, partition_by=orders.region), direction='desc'))`, and `max(last(sum(orders.amount, window='2m')))`
- **THEN** the first reads 4, 2, 3, 1; the second equals its explicit-grain spelling

#### Scenario: Two time buckets are both in the defaulted grain
- **WHEN** a query over a monthly dataset with totals 15, 35, 30 for January to March 2024 declares time dimensions at month and at year, in either order, and selects `sum(rank(sum(monthly.amount), direction='desc'))`
- **THEN** the months read 3, 1, 2, as before this change: the inner sum's grain holds both buckets

#### Scenario: Cross-dataset shares rank at the query grain
- **WHEN** a query on the Appendix B seed groups by `customers.region` and `customers.segment` and selects `rank(sum(orders.amount) / sum(orders.amount, partition_by=customers.region), direction='desc')`, once without `population` and once with `population: customers`; another, same grouping, selects `rank(sum(orders.amount) - sum(returns.amount, partition_by=customers.region), direction='desc')`; another selects `cumsum` of the cross-dataset share by `customers.region` and month of `orders.ordered_at`
- **THEN** the first reads (EAST, corporate) 3, (EAST, retail) 2, (WEST, retail) 1, with NORTH NULL under the named population; the second (EAST, corporate) 2, (EAST, retail) 1 and WEST NULL; the third EAST 0.429 then 1.0 and WEST 1.0

#### Scenario: A defaulted grain keeps the mode axis
- **WHEN** a query on the Appendix B seed names `population: orders`, groups by `orders.status` and selects `max(rank(sum(returns.amount), direction='desc'))`, under each `to_many_handling`; and likewise `max(rank(count(customers.*), direction='desc'))`, `weighted_avg(orders.amount, weight=rank(sum(returns.amount), direction='desc'))`, `max(rank(count(tickets.*), direction='desc'))` by `customers.region`, and `max(last(sum(returns.amount)))` by status and month
- **THEN** each returns the values and warnings it returns before this change: the `associated` warning when the mode is omitted, the value with the `broadcast` warning under `"broadcast"`, the error-mode refusal naming the metric as written under `"error"`; none raises an error about a `partition_by=`

#### Scenario: A finer operand does not make a sibling's grain written
- **WHEN** the same population and grouping select `max(rank(sum(returns.amount) / sum(returns.amount, partition_by=[orders.status, customers.region]), direction='desc'))` with the mode omitted
- **THEN** every row reads 1 and the `associated` warning is present

#### Scenario: A written and a defaulted spelling of one grain agree
- **WHEN** a city-seed query by `orders.region` and `orders.city` selects `max(rank(sum(orders.amount), direction='desc'))` beside `max(rank(sum(orders.amount, partition_by=[orders.region, orders.city]), direction='desc'))`
- **THEN** both read 4, 1, 3, 2 and the result has two distinct columns

#### Scenario: A sibling measure changes no value
- **WHEN** the mixed-operand rank query adds `sum(orders.amount)` and `rank(sum(orders.amount), direction='desc')`
- **THEN** every value of every measure is unchanged

#### Scenario: A mixed-operand measure beside a computed dimension
- **WHEN** a city-seed query's dimensions are `orders.region` and the computed dimension `sum(orders.amount, partition_by=orders.city)`, and it selects `rank(share, direction='desc')`
- **THEN** the four cells (North 35, North 70, South 50, South 60) read 4, 1, 3, 2, and a dimension `rank(sum(orders.amount), direction='desc')` is still refused with the existing computed-dimension error

### Requirement: time_shift evaluates its operand in the moved frame

`time_shift(E, k)` (and therefore `change` / `change_pct`) SHALL read, at each
visible cell, the value `E` takes at the shifted bucket in the same query with
its frame moved by `k`: every frame bound on the time axis's column moves so
that the frame covers exactly the source buckets of the visible buckets, in
whole buckets, and every other filter is unchanged. The rule SHALL hold for
every operand alike — a bare or composite aggregate, a cross-model leaf, a
predicate, a nested transform — so two operands equal at every cell of every
query shift to equal values. Consequently:

- an aggregate (plain, windowed, ranked or cross-model) and a nested `time_shift`
  / `change` / `change_pct` read their source bucket even when it lies outside
  the visible frame, and a forward shift reads after it;
- a nested frame-relative transform (`cumsum`, `lag`, `lead`, the rank family,
  `consecutive_periods`) evaluates over the moved frame — a running total
  restarts at the moved frame's first bucket, and `lag` steps over the moved
  frame's cells;
- a constituent whose grain does not contain the time axis keeps its in-frame
  value;
- a windowed constituent reads rows before the moved frame exactly as a windowed
  aggregate reads rows before the visible frame;
- shifts whose moved frames differ are evaluated separately.

Unshifted transforms are unchanged: `cumsum` accumulates from the first visible
bucket and `lag` / `lead` step over the visible cells.

Scenarios marked "the moved-frame seed" use `orders(region, amount, ordered_at)`
joined many-to-one to `customers(id, spend)` with North `sum(amount)` by month
2023-12 20, 2024-01 30 (dated 2024-01-10), 2024-02 40, 2024-04 30, 2024-05 5,
2024-06 7 and no March rows, queried by `orders.region` and the month of
`orders.ordered_at` with `"date_range": ["2024-02-01", "2024-05-31"]`; values
are read for North on SQLite and DuckDB.

#### Scenario: An equal rewrite shifts to the same value

- **WHEN** the moved-frame seed query requests `time_shift(sum(orders.amount), -1)`
  and `time_shift(sum(orders.amount) + 0 * cumsum(sum(orders.amount)), -1)`
- **THEN** both read Feb 30, Apr NULL and May 30, by executed values

#### Scenario: change and change_pct of an equal rewrite agree

- **WHEN** the moved-frame seed query requests `change` and `change_pct` of
  `sum(orders.amount)` and of `sum(orders.amount) + 0 * cumsum(sum(orders.amount))`
- **THEN** both `change` columns read Feb 10, Apr NULL, May −25 and both
  `change_pct` columns read Feb ⅓, Apr NULL, May −⅚, by executed values

#### Scenario: A NULL-absorbing equal rewrite agrees

- **WHEN** the moved-frame seed query requests
  `time_shift(coalesce(sum(orders.amount), 0), -1)` and
  `time_shift(coalesce(sum(orders.amount), 0) + 0 * cumsum(sum(orders.amount)), -1)`
- **THEN** the two columns are equal in every row, by executed values

#### Scenario: A shifted running total reads the moved frame

- **WHEN** the moved-frame seed query requests
  `time_shift(cumsum(sum(orders.amount)), -1)` and `cumsum(sum(orders.amount))`
- **THEN** the shift reads Feb 30, Apr NULL, May 100 — the running total from
  January, the moved frame's first bucket — while the unshifted running total
  reads Feb 40, Apr 70, May 75, by executed values

#### Scenario: Two offsets read their own moved frames

- **WHEN** the same query also requests `time_shift(cumsum(sum(orders.amount)), -2)`
- **THEN** it reads Feb 20, Apr 90, May NULL — the running total from December —
  while the `-1` shift keeps its values, by executed values

#### Scenario: A shifted lag steps over the moved frame

- **WHEN** the moved-frame seed query requests `lag(sum(orders.amount))` and
  `time_shift(lag(sum(orders.amount)), -1)`
- **THEN** `lag` reads Feb NULL, Apr 40, May 30 and the shift reads Feb NULL,
  Apr NULL, May 40, by executed values

#### Scenario: A nested shift reads outside the frame

- **WHEN** the moved-frame seed query requests
  `time_shift(time_shift(sum(orders.amount), -1), -1)`
- **THEN** it reads Feb 20 (December's sum), Apr NULL and May NULL, by executed
  values

#### Scenario: A shifted predicate reads outside the frame

- **WHEN** the moved-frame seed query requests `time_shift(sum(orders.amount) > 35, -1)`
- **THEN** it reads Feb false, Apr NULL and May false, by executed values

#### Scenario: A cross-model operand reads outside the frame

- **WHEN** the moved-frame seed query requests
  `time_shift(sum(customers.spend), -1)` and
  `time_shift(sum(orders.amount) + sum(customers.spend), -1)`
- **THEN** each row's values equal `sum(customers.spend)` and
  `sum(orders.amount) + sum(customers.spend)` at the shifted bucket of the same
  query with `"date_range": ["2024-01-01", "2024-04-30"]`, and Feb is non-NULL
  for both, by executed values

#### Scenario: A forward shift reads after the frame

- **WHEN** the moved-frame seed query requests `time_shift(sum(orders.amount), 1)`
- **THEN** it reads Feb NULL, Apr 5 and May 7, by executed values

#### Scenario: An unaligned shift reads the whole source bucket

- **WHEN** the moved-frame seed query requests
  `time_shift(sum(orders.amount), -1, 'day')`, over the plain month dimension
  and over a month dimension on `time_spine.timestamp` with the same range
- **THEN** Feb reads 30 — all of January, whose row is dated 2024-01-10 — on
  both, by executed values

#### Scenario: A shifted windowed operand reads before the moved frame

- **WHEN** the moved-frame seed query requests
  `time_shift(sum(orders.amount, window='60d'), -1)`
- **THEN** Feb equals `sum(orders.amount, window='60d')` at January in the same
  query without a date range — December's row included — by executed values

#### Scenario: A year-over-year running total

- **WHEN** a monthly query with `"date_range": "2025"` requests
  `time_shift(cumsum(sum(orders.amount)), -1, 'year')`
- **THEN** each 2025 month equals `cumsum(sum(orders.amount))` at the same month
  of 2024 in the query with `"date_range": "2024"`, by executed values

#### Scenario: Filter and order positions read the same rule

- **WHEN** the moved-frame seed query filters on
  `time_shift(sum(orders.amount) + 0 * cumsum(sum(orders.amount)), -1) > 25`,
  once as a filter and once as an order key over `sum(orders.amount)`
- **THEN** the filter keeps exactly the rows `time_shift(sum(orders.amount), -1) > 25`
  keeps (Feb and May), and the order equals ordering by that shift, by executed
  values

#### Scenario: Spine and non-spine agree

- **WHEN** each shape above, except the order position (spine queries refuse
  transform order keys until DEV-2506), is evaluated over a month dimension on
  `time_spine.timestamp` with the same range and `population: "orders"`
- **THEN** every bucket that has a row in the non-spine query carries the same
  value in the spine query wherever the shifted evaluation reads no bucket that
  only the spine query has (North's empty March holds a value for `coalesce`,
  running totals, `lag`, nested shifts and windowed operands), by executed values
