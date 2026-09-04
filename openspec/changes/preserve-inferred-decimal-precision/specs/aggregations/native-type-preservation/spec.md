# aggregations/native-type-preservation — delta

## Purpose

Aggregates over exact-numeric database columns (NUMERIC/DECIMAL families) keep
the database's native exact type in results instead of being cast to a lossy
inferred logical type, unless the query explicitly requests a type.

## ADDED Requirements

### Requirement: Inferred aggregates over exact-numeric columns preserve native precision

When a column's physical database type is an exact numeric (its pre-parenthesis
type token contains `DECIMAL` or `NUMERIC`, case-insensitive — covering
`DECIMAL`, `NUMERIC`, ClickHouse `Decimal32/64/128/256`, BigQuery
`BIGNUMERIC`), an aggregate over it whose result type was inferred (not
explicitly declared in the query or model) SHALL NOT be cast to the inferred
logical type. The generated SQL SHALL carry no lossy cast around the aggregate
and result values SHALL retain the database's exact-numeric representation.
Database-specific nullability/cardinality wrappers around the physical type
(e.g. ClickHouse `Nullable(...)`, `LowCardinality(...)`, arbitrarily nested)
SHALL be transparent to this behaviour. An explicitly declared result type
SHALL still be cast as requested.

#### Scenario: Plain decimal column aggregates without a lossy cast

- WHEN a model column has physical type `DECIMAL(18,2)` (logical type inferred
  as DOUBLE) and a query requests `amount:sum` with no explicit type
- THEN the generated SQL contains no `CAST(... AS DOUBLE)` around the
  aggregate, and the result value is the exact decimal sum

#### Scenario: Wrapped ClickHouse decimal is preserved

- WHEN a ClickHouse column has physical type `Nullable(Decimal(18, 2))` (or
  the same nested under `LowCardinality`) and a query aggregates it without an
  explicit type
- THEN the column's retained raw type is the bare inner type (no wrapper text)
  and the aggregate is emitted without a lossy float cast

#### Scenario: Short and widened decimal variants are preserved

- WHEN a column's raw database type string is a variant such as
  `Decimal64(4)` or `BIGNUMERIC` and a query aggregates it without an explicit
  type
- THEN the column is detected as exact-numeric, its raw type is retained, and
  the aggregate is emitted without a lossy float cast

#### Scenario: Explicit type still casts

- WHEN a query declares an explicit result type (e.g. `type: DOUBLE`) on a
  measure over a `DECIMAL(18,2)` column
- THEN the generated SQL casts the aggregate to the declared type and the
  result value has that type

#### Scenario: Non-exact-numeric columns are unaffected

- WHEN a column's physical type is not an exact numeric (e.g. `DOUBLE`, `INT`,
  `MONEY`)
- THEN aggregate cast behaviour is unchanged from the inferred logical type's
  rules
