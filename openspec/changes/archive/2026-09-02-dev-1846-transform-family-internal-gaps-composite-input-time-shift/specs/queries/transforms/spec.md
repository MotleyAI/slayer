# queries/transforms — delta

## Purpose

Composition rules for window and self-join transforms: which input shapes
`time_shift` and `consecutive_periods` accept, the values composite inputs
produce, the predicate typing contract, and the uniform fail-closed errors for
the remaining unsupported shapes.

## ADDED Requirements

### Requirement: Composite-input time_shift

`time_shift` (and therefore `change` / `change_pct`, which desugar onto it)
SHALL accept an input that is an arithmetic / scalar-call composite whose
slottable leaves are all aggregates (literals and arbitrary nesting allowed).
The result SHALL equal the composite evaluated over the shifted time bucket's
aggregates within the same partition — matching what the same composite measure
would return for that bucket — and SHALL be NULL when the shifted bucket has no
rows, including under NULL-absorbing wrappers such as `coalesce`. Aggregation
parameters, parameter fragments, and column filters SHALL apply per leaf
without leaking between leaves.

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

### Requirement: time_shift composite rejection stays fail-closed

`time_shift` SHALL reject, with a `ValueError` naming the operation, the
offending input shape, and the multi-stage `source_queries` remedy: a nested
transform anywhere in the input tree, a composite with any row-level leaf
(pure-row or mixed with aggregates), a composite containing a cross-model
aggregate leaf, and a top-level predicate (`IN` / `BETWEEN`) that has no
materialised value slot. Bare single-leaf inputs (aggregate, column, derived
column) SHALL keep their existing behavior.

#### Scenario: Nested transform inside time_shift rejected

- **WHEN** a query requests `time_shift(cumsum(revenue:sum), -1)`
- **THEN** the query fails with a `ValueError` naming the nested-transform
  shape and the multi-stage remedy

#### Scenario: Mixed aggregate-and-row composite rejected

- **WHEN** a query requests `time_shift(revenue:sum * weight, -1)` where
  `weight` is a plain column
- **THEN** the query fails with a `ValueError` naming the mixed shape

#### Scenario: Cross-model aggregate leaf inside a composite rejected

- **WHEN** a `time_shift` composite input contains an aggregate over another
  model's column (dotted path)
- **THEN** the query fails with a `ValueError` naming the cross-model leaf and
  the remedy

#### Scenario: Top-level predicate input rejected

- **WHEN** a query requests `time_shift(store in ('A', 'B'), -1)`
- **THEN** the query fails with a `ValueError` naming the shape and the remedy,
  rather than leaking an internal `RuntimeError`

### Requirement: Composite-input consecutive_periods

`consecutive_periods` SHALL accept any Mode-B value-key input tree — arithmetic
of any operator, scalar calls, `BETWEEN`, `IN` / negated `IN`, boolean
connectives, and nested transforms in any position. A boolean-shaped input is
used as the predicate directly with NULL treated as false; a value-shaped input
is true where its value is non-NULL and non-zero. Streak semantics are
unchanged: false or NULL breaks the run and returns 0.

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

### Requirement: consecutive_periods predicate typing contract

Boolean-shaped SHALL be defined recursively as: a comparison; `BETWEEN`; `IN`;
or `and` / `or` / `not` whose operands are themselves boolean-shaped. A
boolean-shaped node SHALL be accepted at the predicate top level and in a
conditional's condition position (`iif` first argument), and SHALL be rejected
with a `ValueError` naming the shape when it appears in any value position — an
arithmetic operand, an argument of any other scalar call, or an operand of an
`IN` / `BETWEEN` predicate. `and` / `or` / `not` SHALL reject non-boolean-shaped
operands the same way. A top-level string-family scalar call SHALL be rejected
as a predicate (its truthiness is undefined).

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
