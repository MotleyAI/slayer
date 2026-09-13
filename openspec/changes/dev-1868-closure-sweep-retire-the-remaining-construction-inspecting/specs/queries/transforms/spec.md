# queries/transforms — delta

## MODIFIED Requirements

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
  predicate (`IN` / comparison over aggregates). The input's
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

## REMOVED Requirements

### Requirement: time_shift composite rejection stays fail-closed

**Reason**: Three of its four rejection arms are closure violations now lifted —
nested-transform inputs, cross-model composite leaves, and aggregate-typed
predicate inputs execute under the series regime of the Composite-input
time_shift requirement. Only the row-level-leaf arm survives, re-specified as
its own requirement below.

**Migration**: Formerly rejected nested-transform, cross-model-leaf, and
aggregate-typed-predicate inputs now execute (series semantics; see the
Composite-input time_shift requirement). Row-level leaves keep failing closed
under the new row-level-leaf requirement, pending DEV-1859.

## ADDED Requirements

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
