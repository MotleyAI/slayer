# facade/time-filter-translation Specification

## Purpose
Defines how the SQL facade translates WHERE-clause predicates on time dimensions
into a `SlayerQuery`: what lifts into `TimeDimension.date_range` and what passes
through verbatim as query filters.

## Requirements

### Requirement: Only a source-SQL BETWEEN lifts to date_range

The facade SHALL lift a WHERE conjunct of the form `<time-dim> BETWEEN <literal> AND <literal>`
into the matching `TimeDimension.date_range` as a two-element `[low, high]` range.
No other predicate shape SHALL populate `date_range`.

#### Scenario: Two-literal BETWEEN lifts

- **WHEN** the facade translates `WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31'`
  and `ordered_at` is a projected time dimension
- **THEN** that time dimension's `date_range` is `['2024-01-01', '2024-12-31']` and no
  filter string is emitted for the conjunct

#### Scenario: BETWEEN plus a comparator on the same dimension

- **WHEN** the facade translates `WHERE ordered_at BETWEEN '2024-01-01' AND '2024-12-31' AND ordered_at >= '2024-06-01'`
- **THEN** the BETWEEN lifts to `date_range` and the comparator is emitted verbatim as a
  query filter, preserving AND semantics

### Requirement: Relational time comparators translate verbatim

The facade SHALL translate every relational comparator (`>=`, `>`, `<=`, `<`) on a time
dimension verbatim into `SlayerQuery.filters` — whether one-sided or part of a pair —
preserving the operator's strictness exactly. It MUST NOT merge comparators into
`date_range`, and MUST NOT produce a `date_range` containing a null bound.

#### Scenario: One-sided lower bound

- **WHEN** the facade translates `WHERE ordered_at >= '2024-01-01'` with no upper bound
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at >= '2024-01-01'`

#### Scenario: One-sided upper bound

- **WHEN** the facade translates `WHERE ordered_at <= '2024-12-31'` with no lower bound
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at <= '2024-12-31'`

#### Scenario: Strict lower bound alone

- **WHEN** the facade translates `WHERE ordered_at > '2024-01-01'`
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at > '2024-01-01'`

#### Scenario: Strict upper bound alone

- **WHEN** the facade translates `WHERE ordered_at < '2025-01-01'`
- **THEN** the time dimension's `date_range` is unset and the filters contain
  `ordered_at < '2025-01-01'`

#### Scenario: Paired inclusive bounds do not lift

- **WHEN** the facade translates `WHERE ordered_at >= '2024-01-01' AND ordered_at <= '2024-12-31'`
- **THEN** the time dimension's `date_range` is unset and both comparators appear
  verbatim in the filters

#### Scenario: Paired mixed-strictness bounds preserve strictness

- **WHEN** the facade translates `WHERE ordered_at >= '2024-01-01' AND ordered_at < '2025-01-01'`
- **THEN** the time dimension's `date_range` is unset and both comparators appear
  verbatim in the filters, so a row at exactly `2025-01-01 00:00:00` is excluded

#### Scenario: Reversed operand order stays verbatim

- **WHEN** the facade translates `WHERE '2024-01-01' <= ordered_at`
- **THEN** the conjunct is emitted as a query filter (no `date_range`) and the resulting
  query is executable

### Requirement: One-sided time filters return correct results end-to-end

A facade-translated query with a one-sided time filter SHALL return exactly the rows
satisfying the filter when executed.

#### Scenario: Open-ended date filter returns the matching rows

- **WHEN** a table holds rows on both sides of `2024-01-01` and the facade-translated
  query `SELECT month(ordered_at), revenue_sum FROM orders WHERE ordered_at >= '2024-01-01'`
  is executed
- **THEN** the result contains exactly the rows with `ordered_at >= '2024-01-01'` and is
  non-empty
