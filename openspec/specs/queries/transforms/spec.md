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

- **Re-aggregation regime** (existing behavior, unchanged): a bare single leaf
  (aggregate, column, derived column) or a composite whose aggregate leaves are
  all local. The result SHALL equal the composite evaluated over the shifted
  time bucket's aggregates within the same partition — matching what the same
  composite measure would return for that bucket — and SHALL be NULL when the
  shifted bucket has no rows, including under NULL-absorbing wrappers such as
  `coalesce`. Aggregation parameters, parameter fragments, and column filters
  SHALL apply per leaf without leaking between leaves.
- **Series regime** (new): an input containing a nested transform anywhere in
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
naming the operation, the offending input shape, and the multi-stage
`source_queries` remedy, any input containing a row-level (non-aggregate) leaf
— pure-row, mixed with aggregates, or a predicate over row-level columns. Bare
single-leaf inputs (aggregate, column, derived column) SHALL keep their
existing behavior. The rejection SHALL surface at plan time as a typed checker
error, before any SQL is generated.

#### Scenario: Mixed aggregate-and-row composite rejected

- **WHEN** a query requests `time_shift(revenue:sum * weight, -1)` where
  `weight` is a plain column
- **THEN** the query fails with a `ValueError` naming the mixed shape

#### Scenario: Row-level predicate input rejected

- **WHEN** a query requests `time_shift(store in ('A', 'B'), -1)` where `store`
  is a plain column
- **THEN** the query fails with the row-level-leaf `ValueError` naming the shape
  and the remedy, rather than leaking an internal `RuntimeError`

#### Scenario: Row leaf hidden inside a nested transform rejected

- **WHEN** a query requests `time_shift(cumsum(weight), -1)` where `weight` is
  a plain column
- **THEN** the query fails with the row-level-leaf `ValueError` — a nested
  transform does not launder its row-level input into a series

### Requirement: Non-shift transforms reject grain-refining row-level leaves
A transform other than `time_shift`, `change`, and `change_pct`, used in
measure, filter, or order position, SHALL reject with a typed plan-time error —
before any SQL is generated — any row-level (non-aggregate) leaf in its input
that refines the consumer grain, that is, a leaf that is not itself a projected
query dimension. The error SHALL name the transform, the offending leaf kind,
and the remedy (aggregate the leaf, e.g. `cumsum(weight:sum)`), and cite no
tracking issue. The rule applies uniformly to every non-shift transform
operation, the rank family included. Leaves that are projected grain keys —
plain or computed dimensions — remain legal, evaluated at the query grain. The
raw source column of a bucketed time dimension is not a projected grain key
(it refines the bucket). `first`/`last` keep their aggregation dispatch, the
shift family keeps its bare-leaf regime and its existing composite row-leaf
rejection, and the stricter dimension-position rules are unchanged.

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

#### Scenario: Attached values do not launder a row leaf
- **WHEN** a query selects the measure
  `cumsum(weight * avg(unit_price, partition_by=product))` with `weight` not
  projected
- **THEN** it fails with the same typed error — a transform does not collapse
  row grain, unlike an aggregation

#### Scenario: Shift family keeps its bare-leaf regime
- **WHEN** a query selects `time_shift(weight, -1)` or `change(weight)` over a
  month time dimension
- **THEN** the established read-and-rebucket behavior is unchanged and the base
  grain is not inflated
