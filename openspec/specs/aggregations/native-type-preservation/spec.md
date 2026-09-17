# aggregations/native-type-preservation Specification

## Purpose
Aggregates over exact-numeric database columns (NUMERIC/DECIMAL families) keep
the database's native exact type in results instead of being cast to a lossy
inferred logical type, unless the query explicitly requests a type.

## Requirements

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
SHALL still be cast as requested. On dialects without native exact decimal
storage (SQLite's numeric affinity), the inferred cast SHALL be kept — there is
no exact value to preserve, and an un-cast aggregate would have an unstable
result type.

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

#### Scenario: Dialects without exact decimal storage keep the inferred cast

- WHEN a SQLite model column is declared `DECIMAL(18,2)` (numeric affinity) and
  a query requests `amount:sum` with no explicit type
- THEN the generated SQL keeps the inferred float cast and the result value is
  a float even when every stored value is integral

### Requirement: Declared temporal casts are suppressed on dialects without native temporal storage

On a dialect whose temporal values are stored as text under numeric affinity (SQLite), a declared or inferred DATE / TIMESTAMP result type SHALL NOT be rendered as `CAST(... AS DATE)` / `CAST(... AS TIMESTAMP)` around an aggregate, a composite, a windowed or ranked value, or a derived column expression, because that cast collapses a text date to its leading year. Executed temporal values on such a dialect SHALL be the full date. Every other dialect SHALL render its declared-type casts unchanged.

#### Scenario: Temporal max returns the full date on SQLite

- **WHEN** a query against a SQLite datasource selects `created_at:max` grouped by `customer_id`
- **THEN** the generated SQL carries no `CAST(... AS TIMESTAMP)` around the aggregate and each value is the customer's full latest date; the same query on DuckDB returns the same dates

#### Scenario: Partitioned temporal aggregate returns the full date on SQLite

- **WHEN** a query against a SQLite datasource selects `created_at:max(partition_by=customer_id)`
- **THEN** the attached value is the full latest date per customer

#### Scenario: Derived temporal column returns the full date on SQLite

- **WHEN** a model declares a TIMESTAMP column whose `sql` is a non-identifier expression (for example `coalesce(shipped_at, created_at)`) and a SQLite query projects it as a dimension and aggregates it with `max`
- **THEN** both values are full dates, by executed values on SQLite and DuckDB

#### Scenario: Other dialects keep their casts

- **WHEN** the same temporal aggregate is rendered for Postgres, DuckDB, T-SQL or BigQuery
- **THEN** the generated SQL is byte-identical to the SQL rendered before this change
