## ADDED Requirements

### Requirement: date_range bounds are time points

A time dimension's `date_range` SHALL be a single time point (a string, or a one-element list) or a two-element list `[lower, upper]` whose elements are time points or `null`. A single period `P` SHALL restrict the time dimension's raw column to `[start(P), next_start(P))`. A two-element range SHALL restrict it to `x >= start(lower)` and, for the upper bound, `x < next_start(upper)` when it is a period or `x <= upper` when it is an instant; a `null` bound is open and adds no restriction on that side. A lower bound after the upper bound SHALL yield an empty result. The restriction SHALL apply identically on model and stage time dimensions and SHALL be a frame bound (it does not clip trailing windows or `time_shift`).

#### Scenario: Date-only upper bound covers its whole day

- **WHEN** a TIMESTAMP column holds rows at `2024-06-01 00:00`, `2024-12-31 00:00` and `2024-12-31 10:00`, and the time dimension has `date_range: ['2024-01-01', '2024-12-31']`
- **THEN** all three rows are counted, identically on SQLite and DuckDB

#### Scenario: Single period

- **WHEN** a time dimension has `date_range: "2025-Q1"` or `date_range: ["last month"]`
- **THEN** rows are restricted to that quarter, respectively to last month

#### Scenario: Period bounds

- **WHEN** a time dimension has `date_range: ["2025-01-15", "2025-03"]`
- **THEN** rows are restricted to `[2025-01-15, 2025-04-01)`

#### Scenario: Relative bounds

- **WHEN** the clock reads `2026-09-29 12:00:00` and a time dimension has `date_range: ["12 months ago", "last month"]`
- **THEN** rows are restricted to `[2025-09-01, 2026-09-01)`

#### Scenario: Instant upper bound is inclusive

- **WHEN** a time dimension has `date_range: ["2024-01-01", "2024-01-10 00:00:00"]` and a row sits at `2024-01-10 00:00:00`
- **THEN** that row is included and a row at `2024-01-10 00:00:01` is not

#### Scenario: Open upper bound

- **WHEN** a time dimension has `date_range: ["2024-01-01", null]`
- **THEN** rows from `2024-01-01` onward are included with no upper restriction, by executed values on SQLite and DuckDB

#### Scenario: Open lower bound

- **WHEN** a time dimension has `date_range: [null, "2024-12-31"]`
- **THEN** every row before `2025-01-01` is included

#### Scenario: Reversed bounds

- **WHEN** a time dimension has `date_range: ["2025-06", "2025-01"]`
- **THEN** the query executes and returns no rows

#### Scenario: Multi-stage models accept one-sided ranges

- **WHEN** a query against a multi-stage (`source_queries`) model has a time dimension with `date_range: ["2024-01-01", null]`
- **THEN** the range restricts the rows rather than failing or being ignored

### Requirement: Malformed date_range shapes are rejected at construction

Constructing a query SHALL fail with a typed error naming the time dimension and the received value when its `date_range` is an empty list, has three or more elements, is `[null, null]`, or has an element that is not a time point by syntax (neither an instant, a period literal nor a relative token). Checks that depend on the column's type (such as a sub-day bound on a DATE column) SHALL fail at planning with the typed time-literal error.

#### Scenario: Empty and over-long ranges

- **WHEN** a query is constructed with `date_range: []` or `date_range: ['2024-01-01', '2024-02-01', '2024-03-01']`
- **THEN** construction fails with the typed error naming the time dimension

#### Scenario: Both bounds missing

- **WHEN** a query is constructed with `date_range: [None, None]`
- **THEN** construction fails with the typed error

#### Scenario: Unparseable bound

- **WHEN** a query is constructed with `date_range: ['last fortnight', None]`
- **THEN** construction fails with the typed error listing the accepted time-point forms

#### Scenario: Sub-day bound on a DATE column

- **WHEN** a query on a DATE time dimension has `date_range: ["2025-01-01 10:00:00", null]`
- **THEN** planning fails with the typed time-literal error

## MODIFIED Requirements

### Requirement: A date_range on a stage time dimension filters the stage's rows

A `date_range` on a downstream stage's time dimension SHALL filter that stage's input rows on the stage column — with the same time-point meaning as a model-scope `date_range` — before the stage's own aggregation, exactly as a model-scope `date_range` filters a model's rows. The range MUST NOT be silently dropped, and it MUST NOT alter the upstream stage's own computation.

#### Scenario: Stage date_range restricts the outer stage

- **WHEN** an inner stage buckets `created_at` at `month` with a revenue sum over four months, and the outer stage declares a time dimension on `created_at` at `month` with `date_range` covering the middle two months
- **THEN** the outer stage returns only those two months with the inner sums, by executed values on SQLite and DuckDB, and the generated SQL applies the range in the outer stage's WHERE on the stage column

#### Scenario: Stage date_range on a coarser re-bucketing

- **WHEN** the outer stage re-buckets a monthly column at `year` with a `date_range` inside one year
- **THEN** the result is that year's total over the months inside the range only

## REMOVED Requirements

### Requirement: A two-bound date_range renders an inclusive range filter

**Reason**: An inclusive `[start, end]` comparison against a date-only end bound dropped rows after midnight on the end day (and, on SQLite, the whole end day), differently per dialect.
**Migration**: Covered by "date_range bounds are time points": a date-only upper bound now covers its whole day; write an instant (`'2024-12-31 00:00:00'`) to keep a midnight-inclusive bound.

### Requirement: A null date_range bound is a hard error

**Reason**: One-sided ranges are now supported.
**Migration**: A `null` bound is an open side (see "date_range bounds are time points"); `[null, null]` is rejected by "Malformed date_range shapes are rejected at construction".

### Requirement: A wrong-length date_range warns and emits no filter

**Reason**: Silently ignoring a stated restriction is replaced by fail-closed typing; a one-element range is now a single period.
**Migration**: `[x]` means the period `x`; `[]` and three or more elements are construction errors (see "Malformed date_range shapes are rejected at construction").
