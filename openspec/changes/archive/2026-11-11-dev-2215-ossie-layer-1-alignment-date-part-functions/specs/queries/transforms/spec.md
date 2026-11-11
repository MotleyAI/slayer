## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: A time-ordered transform over extraction dimensions names the date_trunc remedy

When a time-ordered transform (`time_shift`, `change`, `change_pct`, `cumsum`,
`lag`, `lead`, `first` / `last` as transforms, `consecutive_periods`) or a
windowed aggregate fails with the existing typed no-time-dimension error because
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
  selects `change(sum(amount))`, or `sum(amount, window='3m')`
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
