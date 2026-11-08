# Delta: queries/transforms — null-test predicates in consecutive_periods

## MODIFIED Requirements

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
