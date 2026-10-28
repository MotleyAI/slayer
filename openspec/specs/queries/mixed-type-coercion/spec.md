# queries/mixed-type-coercion Specification

## Purpose
Defines what a Mode-B expression means when a TEXT value meets a number, so comparisons and arithmetic over numbers stored as text behave the same on every Tier-1 dialect.

## Requirements

### Requirement: TEXT against a number is numeric

When a TEXT operand meets an INT or DOUBLE operand in a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN`, `BETWEEN`, `IS [NOT] DISTINCT FROM`, arithmetic (`+`, `-`, `*`, `/`, `%`) or `greatest` / `least`, the TEXT side SHALL be read as `TRY_CAST(text AS DOUBLE)` with the meaning of `queries/casts` — invalid text yields NULL — and the operation SHALL be numeric on every dialect. Arithmetic with a TEXT operand SHALL type as DOUBLE. On Postgres older than 16, or of unknown version, the conversion SHALL follow `TRY_CAST`'s contract there (a plain cast).

#### Scenario: Comparison against an integer literal

- **WHEN** a query's filter is `amount_txt > 100` over rows holding `'120.5'`, `'99'`, `'abc'` and NULL
- **THEN** it keeps `'120.5'` only, on every Tier-1 dialect (Postgres 16+)

#### Scenario: Comparison against a numeric column

- **WHEN** a filter is `amount_txt = qty` over a row where `amount_txt` is `'3.0'` and `qty` is 3
- **THEN** it keeps the row

#### Scenario: Numeric, not lexicographic, on SQLite

- **WHEN** a filter is `amount_txt < 9` over a row holding `'10'` on SQLite
- **THEN** it drops the row

#### Scenario: Arithmetic

- **WHEN** a dimension is `amount_txt * 2` over a row holding `'1.5'`
- **THEN** it yields `3.0` with type DOUBLE

#### Scenario: IN list

- **WHEN** a filter is `code_txt IN (1, 2)` over rows holding `'1'`, `'02'` and `'x'`
- **THEN** it keeps `'1'` and `'02'`

### Requirement: TEXT literals convert at plan time

A TEXT literal meeting a number SHALL be converted to a number at plan time; a literal that is not a number SHALL be rejected at plan time with a typed error naming the literal.

#### Scenario: Numeric string literal

- **WHEN** a filter is `amount > '100'` over an INT column `amount`
- **THEN** it keeps the rows with `amount > 100`

#### Scenario: Non-numeric literal

- **WHEN** a filter is `amount > 'abc'`
- **THEN** the query fails at plan time naming `'abc'`

### Requirement: Coercion is limited to mixed operands

Coercion SHALL apply only where TEXT meets a number in the positions above. A numeric function or numeric aggregation over a TEXT argument (`abs(amount_txt)`, `sum(amount_txt)`) SHALL keep its type error, whose remedy SHALL name `TRY_CAST`. An author-written `CAST` / `::` SHALL never be rewritten. `||` with a number SHALL convert the number to TEXT with the database's own number-to-text cast. A BOOLEAN operand of `||`, `concat` or `concat_ws` SHALL be rejected at plan time with a typed error whose remedy names `CASE`. Existing rules for TEXT against a DATE / TIMESTAMP (a time point) and booleans in a numeric context (1 / 0) SHALL be unchanged.

#### Scenario: Boolean in text concatenation

- **WHEN** a dimension is `name || (amount > 100)`
- **THEN** the query fails at plan time with a type error whose remedy names `CASE`

#### Scenario: Numeric aggregation over TEXT

- **WHEN** a measure is `sum(amount_txt)`
- **THEN** it fails with a type error whose remedy names `TRY_CAST`

#### Scenario: Author cast kept

- **WHEN** a filter is `CAST(amount_txt AS INT) > 100` on Postgres over a row holding `'abc'`
- **THEN** the query fails with Postgres's own cast error, as the author's `CAST` does

### Requirement: Large integers stored as text

The documentation SHALL state that the DOUBLE reading loses precision for integers beyond 2^53 and that `TRY_CAST(x AS BIGINT)` is the exact form.

#### Scenario: Exact form

- **WHEN** a filter is `TRY_CAST(id_txt AS BIGINT) = 12345678901234567`
- **THEN** it compares exactly

### Requirement: A naive timestamp meets a zoned column in the column's type

SLayer's TIMESTAMP SHALL be a naive wall-clock value on every dialect (BigQuery `DATETIME`) — the result of `now()`, timestamp literals, `TRY_CAST(… AS TIMESTAMP)`, `json_value(… RETURNING TIMESTAMP)` and the date functions. A timestamp literal against such a column SHALL compare as ISO text in the column's type, read as UTC. Where a column's `db_type` is the dialect's zoned instant type (BigQuery `TIMESTAMP`), a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IS [NOT] DISTINCT FROM`) between that column and a non-literal TIMESTAMP value SHALL convert the value to the column's type, reading it as UTC, and SHALL leave the column unconverted. Everywhere else — buckets, date functions, windows, the time spine, scalar functions and returned values — such a column SHALL read as its UTC wall clock, a naive TIMESTAMP.

#### Scenario: now() against a BigQuery TIMESTAMP column

- **WHEN** a filter is `created_at < now()` over a BigQuery `TIMESTAMP` column ingested with `db_type` `TIMESTAMP`
- **THEN** it keeps the rows before the current UTC instant, comparing the column itself

#### Scenario: now() against a BigQuery DATETIME column

- **WHEN** a filter is `created_at < now()` over a BigQuery `DATETIME` column
- **THEN** it keeps the rows before the current UTC wall clock

#### Scenario: Timestamp literal against a BigQuery TIMESTAMP column

- **WHEN** a filter is `created_at < TIMESTAMP '2025-01-01 00:00:00'` over a BigQuery `TIMESTAMP` column ingested with `db_type` `TIMESTAMP`
- **THEN** it keeps the rows before the UTC instant 2025-01-01 00:00:00, comparing the column itself

#### Scenario: Monthly bucket of a BigQuery TIMESTAMP column

- **WHEN** a query has the time dimension `month(created_at)` over a BigQuery `TIMESTAMP` column ingested with `db_type` `TIMESTAMP`, on the time spine
- **THEN** each bucket is the naive month start of the UTC wall clock and joins the spine
