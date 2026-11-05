## MODIFIED Requirements

### Requirement: Inferred aggregates over exact-numeric columns preserve native precision

When a column's physical database type is an exact numeric (its pre-parenthesis
type token contains `DECIMAL` or `NUMERIC`, or is `MONEY` or `SMALLMONEY`,
case-insensitive — covering `DECIMAL`, `NUMERIC`, ClickHouse
`Decimal32/64/128/256`, BigQuery `BIGNUMERIC`, Postgres `money` and SQL Server
`MONEY` / `SMALLMONEY`), an aggregate over it whose result type was inferred (not
explicitly declared in the query or model) SHALL NOT be cast to the inferred
logical type, and neither SHALL a row-level derived column whose own physical
database type is an exact numeric. The generated SQL SHALL carry no lossy cast
around the aggregate or derived column, and result values SHALL retain the
database's exact-numeric representation. Reflection and the column-type probe
SHALL NOT flag an exact numeric as a float.
Database-specific nullability/cardinality wrappers around the physical type
(e.g. ClickHouse `Nullable(...)`, `LowCardinality(...)`, arbitrarily nested)
SHALL be transparent to this behaviour. An explicitly declared result type
SHALL still be cast as requested. On dialects without native exact decimal
storage (SQLite's numeric affinity), the inferred cast SHALL be kept — there is
no exact value to preserve, and an un-cast aggregate would have an unstable
result type.

#### Scenario: Plain decimal column aggregates without a lossy cast

- WHEN a model column has physical type `DECIMAL(18,2)` (logical type inferred
  as DOUBLE) and a query requests `sum(amount)` with no explicit type
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

#### Scenario: Money columns are exact

- WHEN a Postgres model column is reflected or probed as `money`, and a query
  requests `sum(amount)` and the derived column `amount * 2` with no explicit
  type
- THEN the column is not flagged as a float, its raw type `money` is retained,
  neither expression is cast to a float, the statement executes, and both
  values are exact

#### Scenario: SQL Server money variants are exact

- WHEN a SQL Server column's type is `MONEY` or `SMALLMONEY`, as a SQLAlchemy
  type, an information-schema string or a cursor description
- THEN it is detected as exact-numeric, not a float, and an aggregate over it
  is emitted without a lossy float cast

#### Scenario: Explicit type still casts

- WHEN a query declares an explicit result type (e.g. `type: DOUBLE`) on a
  measure over a `DECIMAL(18,2)` column
- THEN the generated SQL casts the aggregate to the declared type and the
  result value has that type

#### Scenario: Non-exact-numeric columns are unaffected

- WHEN a column's physical type is not an exact numeric (e.g. `DOUBLE`, `INT`,
  `REAL`)
- THEN aggregate cast behaviour is unchanged from the inferred logical type's
  rules

#### Scenario: Dialects without exact decimal storage keep the inferred cast

- WHEN a SQLite model column is declared `DECIMAL(18,2)` (numeric affinity) and
  a query requests `sum(amount)` with no explicit type
- THEN the generated SQL keeps the inferred float cast and the result value is
  a float even when every stored value is integral

#### Scenario: A wide exact integer derived column keeps its value

- WHEN a Postgres model's derived column of type INT with physical type
  `NUMERIC(38,0)` computes a value above 2⁶³ and a query selects it with no
  explicit type
- THEN the generated SQL carries no cast around the column and the exact
  value is returned
