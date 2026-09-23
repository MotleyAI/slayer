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
slottable leaves are all aggregates (literals and arbitrary nesting allowed).
Two evaluation regimes apply, selected by the input's type, and each input
evaluates under exactly one:

- **Re-aggregation regime**: a bare aggregate or a composite whose aggregate
  leaves are all local. The result SHALL equal the composite evaluated over the
  shifted time bucket's aggregates within the same partition — matching what
  the same composite measure would return for that bucket — and SHALL be NULL
  when the shifted bucket has no rows, including under NULL-absorbing wrappers
  such as `coalesce`. Aggregation parameters, parameter fragments, and column
  filters SHALL apply per leaf without leaking between leaves. **Every leaf
  keeps its own grain** (Axioms 10 and 11.1): a leaf whose grain — its explicit
  `partition_by=`, else the query grain — contains the time axis, and any
  ranked or windowed leaf, is evaluated afresh over the shifted evaluation's
  rows at that grain; a leaf whose grain does not contain the axis is constant
  along the axis and contributes its unshifted value. A partition key that is
  attributable across a join is honoured the same way; a partition key whose
  dependency closure crosses a fanning hop fails with the existing
  input-safety error, never a value.
- **Series regime**: an input containing a nested transform anywhere in
  its tree, a cross-model aggregate leaf inside a composite (a bare cross-model
  aggregate stays re-aggregation), or a top-level aggregate-typed
  predicate (`IN` / comparison over aggregates; `time_shift` only —
  `change` / `change_pct` reject boolean-shaped inputs). The input's
  materialised result series is shifted: each row reads the series value at the
  shifted bucket within the same partition, and a shifted bucket absent from
  the series — filtered out, outside the query's date range, or before the
  series' first bucket — yields NULL. The series is computed exactly once (row
  filters and joins apply to it once, never re-applied by the shift), and
  adding the shifted measure SHALL NOT change the row count or any other
  column's values.

**Frame rule.** In the re-aggregation regime a frame bound — `date_range`, or
a relational bound with a temporal literal on the raw column of one of the
query's time dimensions — constrains the visible buckets only: the shifted
evaluation of every leaf whose grain contains the axis reaches outside it, so
the earliest visible bucket is never short-changed. Every other predicate,
including a population predicate conjoined with a frame bound and every
model-level filter, applies to the shifted evaluation unchanged, and a
population restriction the query resolves by association restricts the shifted
evaluation the same way.

**Join-back.** In both regimes a row reads the shifted relation at the bucket
containing its own bucket offset by the shift, matched on every other member of
the shifted relation's grain — every projected dimension and secondary time
dimension, except those an operand whose leaves are all coarser does not carry
(Axiom 11.1). For a shift whose granularity is a
multiple of the bucket this is the bucket `N` periods away; for a shift not
aligned to the bucket it is the bucket containing the offset instant.

#### Scenario: Ratio shifted one period back

- **WHEN** a query with a month time dimension and a dimension requests
  `time_shift(revenue:sum / qty:sum, -1)`
- **THEN** each row carries the previous month's ratio for its dimension group,
  with executed values matching hand-computed expectations on SQLite and DuckDB

#### Scenario: change_pct over a ratio resets per partition

- **WHEN** a query grouped by store and month requests
  `change_pct(revenue:sum / *:count)`
- **THEN** each store's first month yields NULL and later months yield that
  store's own month-over-month ratio growth, never another store's

#### Scenario: Missing shifted bucket yields NULL under a scalar wrap

- **WHEN** `time_shift(coalesce(revenue:sum, 0), -1)` is evaluated for the
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
  `time_shift(cumsum(revenue:sum), -1)`
- **THEN** each row carries the previous bucket's cumulative sum — the value the
  inner transform's own series holds at the shifted bucket — with NULL at the
  series' first bucket, correct by hand-computed executed values on SQLite and
  DuckDB, and the inner series is computed once

#### Scenario: Cross-model aggregate leaf inside a composite shifts

- **WHEN** a `time_shift` composite input contains an aggregate over another
  model's column (dotted path), alone or mixed with local aggregate leaves
- **THEN** the composed series' shifted value is correct by executed values —
  never the former cross-model-leaf rejection — and the cross-model operand is
  computed in its producer exactly once

#### Scenario: Aggregate-typed predicate input shifts as a boolean series

- **WHEN** a query requests `time_shift(revenue:sum > 100, -1)` over a month
  time dimension
- **THEN** each row carries the previous bucket's boolean, NULL where the
  shifted bucket is absent, by executed values

#### Scenario: change over a predicate stays rejected by the typing contract

- **WHEN** a query requests `change(revenue:sum > 100)`
- **THEN** the query fails with the existing boolean-in-arithmetic-context
  typing error (the desugared subtraction consumes a boolean), not an internal
  error

#### Scenario: Partitioned leaf keeps its grain inside a shifted composite

- **WHEN** a query over `monthly` by `region` and month requests
  `time_shift(amount:sum / amount:sum(partition_by=[ordered_at]), -1)`
  (rows: North Jan 10 / Feb 20 / Mar 30, South Jan 5 / Feb 15, West Feb NULL)
- **THEN** each row carries the prior month's share of that month's
  cross-region total — Feb North ≈ 0.667, Feb South ≈ 0.333, Mar North ≈ 0.571,
  every Jan row and West Feb NULL — by executed values on SQLite and DuckDB,
  never 1.0

#### Scenario: change and change_pct over a partitioned-leaf composite

- **WHEN** the same query requests
  `change(amount:sum / amount:sum(partition_by=[ordered_at]))` and
  `change_pct(amount:sum / amount:sum(partition_by=[ordered_at]))`
- **THEN** the change is Feb North ≈ −0.095, Feb South ≈ +0.095, Mar North
  ≈ +0.429 and the percentage change Feb North ≈ −0.143, Feb South ≈ +0.286,
  Mar North ≈ +0.75, NULL elsewhere, by executed values

#### Scenario: Single partitioned leaf in a trivial composite

- **WHEN** the same query requests
  `time_shift(amount:sum(partition_by=[ordered_at]) / 2, -1)`
- **THEN** each row carries half the prior month's cross-region total — Feb
  North 7.5, Feb South 7.5, Mar North 17.5, West Feb 7.5 (the operand grain is
  the month alone, so a region absent from January still reads it), Jan rows
  NULL — by executed values

#### Scenario: Partitioned leaf at a non-time grain is constant along the axis

- **WHEN** the same query requests
  `time_shift(amount:sum / amount:sum(partition_by=[region]), -1)`
- **THEN** each row carries the prior month's sum divided by the region's own
  total — Feb North ≈ 0.167, Mar North ≈ 0.333, Feb South 0.25, NULL elsewhere —
  by executed values

#### Scenario: Re-aggregation over a shifted partitioned composite

- **WHEN** a query over `monthly` with only a month time dimension requests
  `avg(time_shift(amount:sum(partition_by=[region, ordered_at]) / amount:sum(partition_by=[ordered_at]), -1))`
- **THEN** each month carries the mean of the prior month's regional shares —
  Feb 0.5, Mar ≈ 0.571, Jan NULL — by executed values

#### Scenario: Bare partitioned leaf reaches outside the date range

- **WHEN** the same query restricts the month time dimension with
  `"date_range": ["2024-02-01", "2024-03-31"]` and requests
  `time_shift(amount:sum(partition_by=[ordered_at]), -1)`
- **THEN** Feb North and Feb South carry January's total 15 and Mar North
  February's total 35, by executed values — never NULL at the first visible
  bucket

#### Scenario: Bare ranked leaf reaches outside the date range

- **WHEN** the same date-ranged query requests `time_shift(amount:last, -1)`
- **THEN** Feb North carries 10, Feb South 5 and Mar North 20 — the prior
  month's last value — by executed values

#### Scenario: Non-time partition under a date range keeps its in-frame value

- **WHEN** the same date-ranged query requests
  `time_shift(amount:sum / amount:sum(partition_by=[region]), -1)`
- **THEN** the denominator is the region's total within the date range (North
  50, South 15): Feb North 0.2, Mar North 0.4, Feb South ≈ 0.333, by executed
  values

#### Scenario: Population predicate conjoined with a frame bound survives

- **WHEN** a query over `monthly` by `region` and month with the filter
  `ordered_at >= '2024-02-01' and region = 'North'` requests
  `time_shift(amount:sum + amount:sum(partition_by=[ordered_at]), -1)`
- **THEN** Feb North carries 20 and Mar North 40 — the bound is stripped from
  the shifted evaluation while the region predicate restricts it — by executed
  values

#### Scenario: Population restriction by association reaches the shifted evaluation

- **WHEN** a query rooted at `orders` by `status` and month carries a row
  filter across the fanning `regions → region_events` hop
  (`customers.regions.region_events.value > 40`) and requests
  `time_shift(amount:sum / amount:sum(partition_by=[ordered_at]), -1)`
- **THEN** the shifted evaluation is restricted to the same associated orders:
  Feb ok ≈ 0.394, Feb new ≈ 0.606, Apr ok NULL (no associated March rows), by
  executed values

#### Scenario: Association-resolved leaf is re-evaluated by association

- **WHEN** a query rooted at `orders` in associate mode groups by the fanning
  dimension `customers.regions.region_events.value` and month and requests
  `time_shift(amount:sum, -1)`
- **THEN** each row carries the prior month's associated sum for its value —
  (50, Feb) 33, (30, Apr) 5, NULL elsewhere — by executed values, with the row
  count unchanged by the measure

#### Scenario: Partition key attributable across a join

- **WHEN** a query rooted at `orders` by `customers.tier` and month requests
  `time_shift(amount:sum / amount:sum(partition_by=[customers.tier]), -1)`
- **THEN** the tier total is resolved through the to-one join and each row
  carries the prior month's share of it — gold Apr 0.1, NULL where the prior
  month has no rows for the tier — by executed values

#### Scenario: Partition key across a fanning hop stays a typed error

- **WHEN** a shifted composite's leaf declares a `partition_by=` key whose
  dependency closure crosses a fanning hop from the host
- **THEN** the query fails with the existing input-safety error naming the
  leaf and the hop, never a value

#### Scenario: Ranked leaf inside a shifted composite executes

- **WHEN** the `monthly` query requests `time_shift(amount:last / 2, -1)`
- **THEN** each row carries half the prior month's last value — Feb North 5,
  Mar North 10, Feb South 2.5 — by executed values, never an internal error

#### Scenario: Windowed leaf inside a shifted composite keeps its window

- **WHEN** the `monthly` query requests
  `time_shift(amount:sum(window='90d') / 2, -1)`
- **THEN** each row carries half the prior month's trailing-window sum — Feb
  North 5, Mar North 15, Feb South 2.5, Jan rows and West Feb NULL — by
  executed values, never a value with the window silently dropped

#### Scenario: Bare windowed leaf reaches outside the date range

- **WHEN** the date-ranged `monthly` query requests
  `time_shift(amount:sum(window='90d'), -1)`
- **THEN** Feb North carries January's trailing-window sum 10 and Mar North
  February's 30, by executed values

#### Scenario: Two offsets share one shifted evaluation

- **WHEN** the `monthly` query requests both
  `time_shift(amount:sum / amount:sum(partition_by=[ordered_at]), -1)` and
  `time_shift(amount:sum / amount:sum(partition_by=[ordered_at]), -2)`
- **THEN** the generated SQL computes the shifted composite in exactly one
  relation read by both, and the two-period value at Mar North ≈ 0.667, by
  executed values

#### Scenario: Unaligned shift reads the bucket containing the offset instant

- **WHEN** the `monthly` query requests `time_shift(amount:sum, -1, 'day')`
  over month buckets
- **THEN** Feb North carries January's 10 and Mar North February's 20 — the
  bucket containing the day before each bucket start — by executed values

#### Scenario: Shifted composite over a stage time dimension

- **WHEN** an inner stage projects `region`, `ordered_at` at `month` and
  `amount:sum` as `rev`, and the outer stage declares a month time dimension
  on that column with `"date_range": ["2024-02-01", "2024-03-31"]` and
  requests `time_shift(rev:sum / rev:sum(partition_by=[ordered_at]), -1)` by
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
of any operator, scalar calls, `BETWEEN`, `IN` / negated `IN`, null tests
(`is None` / `is not None`), boolean connectives, and nested transforms in any
position. A boolean-shaped input is used as the predicate directly with NULL
treated as false; a value-shaped input is true where its value is non-NULL and
non-zero. Streak semantics are unchanged: false or NULL breaks the run and
returns 0. Emitted SQL SHALL use a boolean-shaped predicate only in condition
positions — never wrapped as a scalar value — so generation is valid on
strictly-typed dialects (Postgres, T-SQL, BigQuery).

#### Scenario: Numeric delta truthiness

- **WHEN** a query requests `consecutive_periods(revenue:sum - cost:sum)` over
  a month series
- **THEN** the streak counts consecutive months where the delta is non-NULL and
  non-zero, matching hand-computed values on SQLite and DuckDB

#### Scenario: Growth streak over a nested transform

- **WHEN** a query requests `consecutive_periods(change(revenue:sum) > 0)`
- **THEN** the streak counts consecutive months of positive month-over-month
  growth

#### Scenario: Bare nested transform input

- **WHEN** a query requests `consecutive_periods(cumsum(revenue:sum))`
- **THEN** the streak counts consecutive months where the running total is
  non-NULL and non-zero

#### Scenario: Scalar call inside a comparison

- **WHEN** a query requests `consecutive_periods(round(revenue:sum) >= 10)`
- **THEN** the streak counts consecutive months where the rounded total reaches
  the threshold

#### Scenario: Newly lifted predicate families execute

- **WHEN** `consecutive_periods` receives a top-level `BETWEEN`, `IN`, negated
  `IN`, `and`, `or`, or `not` predicate, including groups whose predicate
  evaluates to NULL
- **THEN** each executes on SQLite and DuckDB with NULL treated as false

#### Scenario: Nested IN materialises its column

- **WHEN** an `IN` predicate over a dimension column appears nested inside a
  boolean connective (for example `status in ('a','b') and revenue:sum > 0`)
- **THEN** the referenced column materialises and the streak executes correctly

#### Scenario: Top-level null test drives the streak

- **WHEN** a query requests `consecutive_periods(hi_rev:sum is not None)`
  grouped by store, where one store's aggregate is NULL in the last month
- **THEN** the streak counts consecutive non-NULL months and the NULL month
  breaks the run (and the `is None` form counts the complementary months)

#### Scenario: Null test under a boolean connective

- **WHEN** a query requests
  `consecutive_periods(hi_rev:sum is not None and cost:sum > 0)`
- **THEN** the query executes with both conjuncts applied, rather than failing
  with a boolean-shaped-operands `ValueError`

#### Scenario: Null test over a dimension column

- **WHEN** a query requests `consecutive_periods(store is not None)` grouped by
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
(`is None` / `is not None`); `BETWEEN`; `IN`; or `and` / `or` / `not` whose
operands are themselves boolean-shaped. A boolean-shaped node SHALL be accepted
at the predicate top level and in a conditional's condition position (`iif`
first argument), and SHALL be rejected with a `ValueError` naming the shape when
it appears in any value position — an arithmetic operand, an argument of any
other scalar call, or an operand of an `IN` / `BETWEEN` predicate. `and` / `or`
/ `not` SHALL reject non-boolean-shaped operands the same way. A top-level
string-family scalar call SHALL be rejected as a predicate (its truthiness is
undefined).

#### Scenario: iif condition position accepts a predicate

- **WHEN** a query requests `consecutive_periods(iif(revenue:sum > 0, 1, 0))`
- **THEN** the query executes, with the streak driven by the iif value's
  truthiness

#### Scenario: Boolean in arithmetic context rejected

- **WHEN** a query requests
  `consecutive_periods((revenue:sum > 0) + (cost:sum > 0))`
- **THEN** the query fails with a `ValueError` naming the boolean-in-numeric
  shape

#### Scenario: Boolean as scalar-call argument rejected

- **WHEN** a query requests `consecutive_periods(coalesce(revenue:sum > 0, 0))`
- **THEN** the query fails with a `ValueError` naming the shape

#### Scenario: Boolean in an IN operand rejected

- **WHEN** a query requests `consecutive_periods((revenue:sum > 0) in (1, 0))`
- **THEN** the query fails with a `ValueError` naming the boolean shape, rather
  than passing the predicate through into the emitted `IN` list

#### Scenario: String-family scalar call rejected as predicate

- **WHEN** a query requests `consecutive_periods(lower(name:max))`
- **THEN** the query fails with a `ValueError` explaining that a string-valued
  predicate has no truthiness

#### Scenario: Null test in a value position rejected

- **WHEN** a query requests `consecutive_periods((hi_rev:sum is None) + 1)`
- **THEN** the query fails with a `ValueError` naming the boolean-in-numeric
  shape, rather than rendering the null test as an arithmetic operand

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

- **WHEN** a query requests `time_shift(revenue:sum * weight, -1)` where
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
same value whether or not any operand is also selected on its own. The transform's own
regime (re-aggregation or series) SHALL be unchanged by the composite around it. No such
composite SHALL fail with an internal render error.

#### Scenario: Transform over a cross-model inner plus a hidden local aggregate

- **WHEN** a query rooted at `orders` with a month time dimension selects
  `change(customers.spend:sum) + amount:sum` and nothing else
- **THEN** each row carries the month-over-month change of the cross-model aggregate
  plus that month's local sum, by hand-computed executed values on SQLite and DuckDB,
  with NULL in the first month

#### Scenario: Hidden local operand of another aggregation kind

- **WHEN** the composite's hidden local operand is `*:count` or `amount:max` (for example
  `change(customers.spend:sum) + *:count`)
- **THEN** the query executes with the operand materialised, by executed values

#### Scenario: Transform over a crossing-fragment inner plus a hidden local aggregate

- **WHEN** a query selects `change(amount:wscaled_sum) + amount:sum`, where `wscaled_sum`
  is an aggregation whose default parameter crosses a join
- **THEN** each row carries the weighted-scaled delta plus the local sum, by executed
  values on SQLite and DuckDB

#### Scenario: Other transform families compose the same way

- **WHEN** the transform is `time_shift(customers.spend:sum, -1)` or
  `cumsum(customers.spend:sum)` combined with a hidden local aggregate
- **THEN** the composite executes with the transform's own semantics unchanged, by
  executed values

#### Scenario: Conditional over a transform and mixed aggregates

- **WHEN** a query selects
  `iif(change(customers.spend:sum) > 0, customers.spend:sum, amount:sum)`
- **THEN** each row carries the cross-model total where the change is positive and the
  local sum otherwise, by executed values

#### Scenario: Selecting an operand on its own changes nothing

- **WHEN** the same query additionally selects `amount:sum` as its own measure
- **THEN** the composite's values, the row count and every other column are identical to
  the query without it

#### Scenario: Composite in filter and order positions

- **WHEN** `change(customers.spend:sum) + amount:sum` appears only in a filter or only as
  an ORDER BY key, with a dimension present
- **THEN** rows are masked or sorted by the composite's value exactly as when it is
  selected, by executed values

#### Scenario: Inner varying along the time axis

- **WHEN** the cross-model inner is attributable to the query's time axis (for example
  `change(orders.amount:sum)` by customer signup month from `customers`, or
  `change(customers.spend:sum)` by `customers.signup_at` from `orders`)
- **THEN** the delta and `change_pct` percentages are correct per period by hand-computed
  executed values on SQLite and DuckDB, and adding the measure changes no row and no
  other column

#### Scenario: The issue's named shapes execute directly

- **WHEN** a query selects `change(amount:wscaled_sum)`, `change_pct(amount:wscaled_sum)`,
  `change(customers.spend:sum)` or `change_pct(customers.spend:sum)` alone
- **THEN** each executes with hand-computed values on SQLite and DuckDB, the broadcast
  cross-model inner yielding a zero delta and zero percentage after the first period

### Requirement: Time axis on stage datasets

A downstream stage's own time dimension SHALL be the time axis for every time-ordered transform in that stage (`time_shift`, `change`, `change_pct`, `cumsum`, `lag`, `lead`, `first`, `last`, `consecutive_periods`) and for its windowed aggregates, with values identical to the same shapes evaluated over a model-backed dataset holding the same rows. With two stage time dimensions and no `main_time_dimension`, a time-ordered transform SHALL fail with the existing unambiguous-time-dimension remedy; `main_time_dimension` SHALL select the axis. A stage has no model-level default time dimension, so no default is applied.

#### Scenario: time_shift over a stage time dimension

- **WHEN** an outer stage declares a time dimension on `created_at` at `month` over an inner monthly stage and selects `time_shift(rev:sum, -1, 'month')`
- **THEN** each row carries the previous month's inner sum, NULL for the first month, by executed values on SQLite and DuckDB

#### Scenario: change and cumsum over a stage time dimension

- **WHEN** the same outer stage selects `change(rev:sum)` and `cumsum(rev:sum)`
- **THEN** each row carries the month-over-month delta (NULL first) and the running total, by executed values

#### Scenario: last over a stage time dimension

- **WHEN** the same outer stage selects `last(rev:sum)`
- **THEN** every row carries the value of the latest month, by executed values

#### Scenario: Windowed aggregate over a stage time dimension

- **WHEN** the outer stage selects `rev:sum(window='60d')` over the stage time dimension
- **THEN** each row carries the trailing-window sum keyed on the stage bucket, by executed values

#### Scenario: time_shift over a multi-hop flat time dimension

- **WHEN** an inner stage projects `customers.regions.last_activity_at` at `month` with `*:count` named `n`, and the outer stage declares a time dimension on `customers__regions__last_activity_at` at `month` with `time_shift(n:sum, -1, 'month')`
- **THEN** the shifted relation references the inner stage's flat alias and the query executes with correct values

#### Scenario: Two stage time dimensions need main_time_dimension

- **WHEN** the outer stage declares time dimensions on two distinct temporal columns, `created_at` and `shipped_at`, each at `month`, and selects `change(rev:sum)` without `main_time_dimension`
- **THEN** planning fails with the existing error naming the `main_time_dimension` remedy, and setting `main_time_dimension` to `created_at` makes the query execute with that column's bucket as the axis

### Requirement: Transforms reject grain-refining row-level leaves

A transform other than the aggregation-dispatched `first` / `last`, used in
measure, filter, or order position, or as a constituent of an aggregation
source, SHALL reject with a typed plan-time error — before any SQL is
generated — any row-level (non-aggregate) leaf in its input that refines the
consumer grain, that is, a leaf that is not itself a projected query
dimension. The error SHALL name the transform, the offending leaf, and the
remedies (aggregate the leaf, e.g. `cumsum(weight:sum)`; project it as a query
dimension; or compute it in an earlier `source_queries` stage), and cite no
tracking issue. The rule applies uniformly to every such transform op — the
rank family and the shift family (`time_shift`, `change`, `change_pct`)
included. Leaves that are projected grain keys — plain or computed dimensions
— remain legal, evaluated at the query grain. The raw source column of a
bucketed time dimension is not a projected grain key (it refines the bucket).
`first` / `last` keep their aggregation dispatch, and the stricter
dimension-position rules are unchanged.

#### Scenario: Bare grain-refining leaf rejected
- **WHEN** a query with a month time dimension and no `weight` dimension selects
  the measure `cumsum(weight)`
- **THEN** it fails at plan time with the typed error naming the remedy — never
  SQL whose base grain is inflated to one row per (bucket, weight-value)

#### Scenario: Rank family is covered
- **WHEN** a query over `[store]` with a month time dimension selects the
  measure `rank(qty)`
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
  `rank(weight)`
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
- **WHEN** a query over `[region]` selects the measure `sum(rank(region))`
- **THEN** it compiles: the transform types at the query grain and the aggregation is
  the degenerate identity with the degenerate-re-aggregation warning, never an error

### Requirement: Rank-family partition keys are operand-grain members
A rank-family transform (`rank`, `dense_rank`, `percent_rank`, `ntile`) partitions its
operand's cells, so every key in its own `partition_by=` SHALL be a member of the
transform's operand grain, in every position. The operand grain is the union over the
transform's input: an aggregate contributes its explicit `partition_by=` keys, else the
query grain (its dimensions and time buckets), plus the query's active time bucket when
it is windowed; a nested transform contributes its own operand grain, minus its time
axis when it is `first` or `last`; a composite contributes the union of its operands,
with a projected row-level leaf contributing itself; an input with no aggregate (only
row-level leaves or literals) is the query grain, since it is evaluated once per
query-grain cell. A non-member key SHALL fail at plan time with an error naming
the transform, the key, the operand grain and the remedy (add the key to the inner
aggregate's `partition_by=`, or partition by a member). A key that is no query dimension
at all keeps the existing "not a query dimension" error, and an ungrained inner aggregate
in dimension position keeps the existing grain-self-containment error; both take
precedence over the membership error.

#### Scenario: Non-member query dimension as a measure
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=product)`
- **THEN** planning fails with an error naming `rank`, `product`, the operand grain `city, region` and the `partition_by=` remedy — it never executes by widening the grain

#### Scenario: Non-member query dimension in a filter
- **WHEN** the same query filters `rank(sum(amount, partition_by=[city, region]), partition_by=product) <= 2`
- **THEN** planning fails with the same error

#### Scenario: Non-member key in dimension position, plain column
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount, partition_by=[city, product]), partition_by=region)`
- **THEN** planning fails with the same error naming `region` and the grain `city, product`, never with an internal producer-slot error

#### Scenario: Non-member key in dimension position, computed-dimension name
- **WHEN** a query declares `ureg` = `upper(region)` and the dimension `rank(sum(amount, partition_by=[city, region]), partition_by=ureg)`
- **THEN** planning fails with the same error naming the key and the grain `city, region`, never with an internal error

#### Scenario: Member key executes
- **WHEN** a query over `[city, region, product]` selects `rank(sum(amount, partition_by=[city, region]), partition_by=region)`
- **THEN** each row carries its (city, region) total's rank within the region: East Zeta 1, Delta 2, Epsilon 2; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap the NULL city 1, Kappa 2; Void Xi 1

#### Scenario: Ungrained inner keeps the query-dimension rule
- **WHEN** a query over the banded dimension alone selects `rank(sum(amount), partition_by=region)`
- **THEN** planning fails with the existing "partition_by column 'region' is not a query dimension" error listing the available dimensions; over `[region, band]` the same measure executes because the ungrained inner is grained at the query grain and `region` is a member

#### Scenario: Windowed inner admits the active bucket
- **WHEN** a monthly query selects `rank(sum(amount, window='1y', partition_by=customers.regions.name), partition_by=ordered_at)`
- **THEN** the partition key passes the operand-grain rule as the query's month bucket, a member contributed by the windowed inner

#### Scenario: Nested collapsing transform drops its axis
- **WHEN** a monthly query selects `rank(last(sum(amount, partition_by=[customers.regions.name, ordered_at])), partition_by=ordered_at)`
- **THEN** planning fails with the operand-grain error naming `ordered_at` and the grain `customers.regions.name`; with `partition_by=customers.regions.name` the key passes the rule

#### Scenario: Aggregate-free input takes the query grain
- **WHEN** a query over `[city, region, product]` selects `rank(city, partition_by=region)`
- **THEN** it executes, ranking each row's city value descending within its region: East Zeta 1, Epsilon 2, Delta 3; North Beta 1, Alpha 2; South Gamma 1, Alpha 2; Gap Kappa 1, the NULL city 2; Void Xi 1

#### Scenario: Residue error precedes the membership rule
- **WHEN** a query over `[region]` declares the dimension `rank(sum(amount), partition_by=region)`
- **THEN** planning fails with the existing grain-self-containment error ("must declare partition_by= explicitly"), not the operand-grain error
