## Purpose

Defines casts in Mode-B expressions: `CAST` / `::` as the database's own cast of the author's type, and `TRY_CAST` as the one portable cast that turns an invalid input into NULL on every dialect.

## ADDED Requirements

### Requirement: CAST is the database's cast of the author's type

`CAST(x AS <type>)` and `x::<type>` SHALL be accepted in every Mode-B position, chains included (`x::int::text`). The cast SHALL render the author's type, parameters included (`DECIMAL(10,2)`, `VARCHAR(3)`), changing only its spelling to the target dialect's; its value semantics — rounding, parsing, invalid input — SHALL be the database's own. A type the target dialect cannot express SHALL be rejected at plan time with a typed query error naming the type and the dialect.

#### Scenario: Author's parameterised type is kept

- **WHEN** a measure on Postgres is `sum(CAST(amount AS DECIMAL(10,2)))`
- **THEN** the generated SQL casts to `DECIMAL(10, 2)` and the value is Postgres's own

#### Scenario: Spelling transpiled to the dialect

- **WHEN** a dimension is `amount::text` on MySQL
- **THEN** the generated SQL is valid MySQL casting to its character type

#### Scenario: Chained casts

- **WHEN** a dimension is `amount::int::text`
- **THEN** it casts to INT, then to TEXT

#### Scenario: Invalid input follows the database

- **WHEN** a dimension is `CAST(status AS INT)` over a row holding `'abc'`
- **THEN** Postgres fails the query with its own error and SQLite returns 0, as each database's `CAST` does

### Requirement: A cast's result type is inferred

A cast SHALL type as SLayer's data type for its target: the integer family as INT; floating-point, `REAL`, `DECIMAL` and `NUMERIC` as DOUBLE; character types as TEXT; `BOOLEAN` as BOOLEAN; `DATE` as DATE; `TIMESTAMP` and `DATETIME` as TIMESTAMP; any other type as UNKNOWN. The inferred type SHALL drive checking and response types exactly as a column's declared type does.

#### Scenario: Cast to DATE is a date operand

- **WHEN** a dimension is `date_diff('day', CAST(created_txt AS DATE), DATE '2025-01-01')` over a TEXT column `created_txt`
- **THEN** it binds without a date-operand type error and the response reports INT

#### Scenario: Unknown target type

- **WHEN** a dimension is `CAST(payload AS JSON)` on Postgres
- **THEN** it renders and its response type is UNKNOWN

### Requirement: TRY_CAST turns invalid input into NULL on every dialect

`TRY_CAST(x AS <type>)` SHALL render the author's type like `CAST` and SHALL yield NULL, never an error or a substitute value, for an input the cast cannot convert, on every Tier-1 dialect. Where the database cannot itself tell a valid input from an invalid one (SQLite, MySQL numeric targets), a number SHALL be valid when it is an optionally signed decimal with an optional fraction and exponent, surrounding spaces ignored; a boolean when it is one of `true`, `false`, `t`, `f`, `yes`, `no`, `1`, `0` in any case; a date or timestamp when it is an ISO time-point shape naming a real calendar value. On Postgres, a server older than 16 — or one whose version cannot be determined, including a dry run without a connection — SHALL get a plain `CAST`, so an invalid input fails the query there; this is documented.

#### Scenario: Invalid text becomes NULL

- **WHEN** a dimension is `TRY_CAST(status AS INT)` over rows holding `'12'` and `'abc'`
- **THEN** they yield 12 and NULL on SQLite, DuckDB, MySQL and Postgres 16

#### Scenario: SQLite does not truncate to a prefix

- **WHEN** a dimension is `TRY_CAST(status AS INT)` over a row holding `'12abc'` on SQLite
- **THEN** it yields NULL, not 12

#### Scenario: Postgres before 16

- **WHEN** the same query runs against a Postgres server reporting version 15
- **THEN** the generated SQL is a plain `CAST`, and a row holding `'abc'` fails the query with Postgres's error

#### Scenario: Postgres 16 uses the native validity check

- **WHEN** the query runs against a Postgres server reporting version 16 or later
- **THEN** the generated SQL guards the cast with Postgres's own input-validity check and `'abc'` yields NULL
