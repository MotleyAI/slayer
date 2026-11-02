## MODIFIED Requirements

### Requirement: CAST is the database's cast of the author's type

`CAST(x AS <type>)` and `x::<type>` SHALL be accepted in every Mode-B position, chains included (`x::int::text`). A type SHALL be written as type words with an optional parenthesised integer parameter list that may sit between words (`TIMESTAMP(6) WITH TIME ZONE`, `CHARACTER VARYING(10)`, `DECIMAL(10,2) UNSIGNED`), followed by an optional `[]` array suffix; a malformed type SHALL fail at parse time with an error about the type. The cast SHALL render the author's type, parameters included (`DECIMAL(10,2)`, `VARCHAR(3)`), changing only its spelling to the target dialect's; its value semantics — rounding, parsing, invalid input — SHALL be the database's own. A type the target dialect cannot express SHALL be rejected at plan time with a typed query error naming the type and the dialect; a type is not expressed when the target's rendering would drop or change any part of it — its family, precision or scale, time-zone modifier, signedness, or array suffix. An array of arrays (`T[][]`) SHALL be rejected.

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

#### Scenario: Precision before the time-zone words

- **WHEN** a dimension on Postgres is `CAST(created_at AS TIMESTAMP(6) WITH TIME ZONE)`
- **THEN** it parses and the generated SQL casts to a time-zoned timestamp with precision 6

#### Scenario: Precision that the rendering would drop

- **WHEN** a dimension on Postgres is `CAST(created_at AS TIMESTAMP WITH TIME ZONE(6))`
- **THEN** the query is rejected at plan time with the typed cast error naming the type and the dialect

#### Scenario: Unsigned type only where the dialect has one

- **WHEN** a dimension is `CAST(amount AS DECIMAL(10,2) UNSIGNED)` on MySQL and the same dimension on Postgres
- **THEN** MySQL casts to `DECIMAL(10, 2) UNSIGNED`, and Postgres rejects it at plan time with the typed cast error

#### Scenario: Malformed type

- **WHEN** a dimension is `CAST(amount AS DECIMAL(10,))`
- **THEN** it fails at parse time with an error about the type, not about a missing `)`

#### Scenario: Array cast on a dialect with arrays

- **WHEN** a dimension on Postgres is `CAST(codes AS INT[])` and on DuckDB `codes::INT[]`
- **THEN** each renders the database's array cast

#### Scenario: Array cast on a dialect without arrays

- **WHEN** a dimension on SQLite is `CAST(codes AS INT[])`
- **THEN** the query is rejected at plan time with the typed cast error naming the type and the dialect

#### Scenario: Array of arrays

- **WHEN** a dimension on Postgres is `CAST(codes AS INT[][])`
- **THEN** it is rejected with a typed error

### Requirement: A cast's result type is inferred

A cast SHALL type as SLayer's data type for its target: the integer family as INT; floating-point, `REAL`, `DECIMAL` and `NUMERIC` as DOUBLE; character types as TEXT; `BOOLEAN` as BOOLEAN; `DATE` as DATE; `TIMESTAMP` and `DATETIME` as TIMESTAMP; any other type as UNKNOWN. A cast to `T[]` SHALL type as a list whose elements have `T`'s type (UNKNOWN elements when `T` maps to UNKNOWN), usable wherever a list column is. The inferred type SHALL drive checking and the query plan's types exactly as a column's declared type does.

#### Scenario: Cast to DATE is a date operand

- **WHEN** a dimension is `date_diff('day', CAST(created_txt AS DATE), DATE '2025-01-01')` over a TEXT column `created_txt`
- **THEN** it binds without a date-operand type error and the query plan reports INT

#### Scenario: Unknown target type

- **WHEN** a dimension is `CAST(payload AS JSON)` on Postgres
- **THEN** it renders and its planned type is UNKNOWN

#### Scenario: Array cast is a list of its element type

- **WHEN** a dimension on Postgres is `contains(CAST(codes AS INT[]), 3)`
- **THEN** it binds, the cast is planned as a list of INT, and the dimension is true for rows whose array holds 3

#### Scenario: Unsigned integer is an integer

- **WHEN** a dimension on MySQL is `CAST(amount AS INT UNSIGNED)`
- **THEN** its planned type is INT

### Requirement: TRY_CAST turns invalid input into NULL on every dialect

`TRY_CAST(x AS <type>)` SHALL render the author's type like `CAST` and SHALL yield NULL, never an error or a substitute value, for an input the cast cannot convert, on every Tier-1 dialect. For an integer-family target, on every Tier-1 dialect, a text input SHALL be valid exactly when it is an optionally signed run of decimal digits, surrounding spaces ignored, a numeric input exactly when it is integral, and either only when the integer is within the target's range. Where the database cannot itself tell a valid input from an invalid one (SQLite, MySQL numeric targets): any other number target SHALL accept an optionally signed decimal with an optional fraction and exponent, surrounding spaces ignored; a boolean SHALL be one of `true`, `false`, `t`, `f`, `yes`, `no`, `1`, `0` in any case; a date or timestamp SHALL be an ISO time-point shape naming a real calendar value. `TRY_CAST` to an array type SHALL be rejected with a typed query error on every dialect. On Postgres, a server older than 16 — or one whose version cannot be determined, including a dry run without a connection — SHALL get a plain `CAST`, so an invalid input fails the query there; this is documented.

#### Scenario: Invalid text becomes NULL

- **WHEN** a dimension is `TRY_CAST(status AS INT)` over rows holding `'12'` and `'abc'`
- **THEN** they yield 12 and NULL on SQLite, DuckDB, MySQL and Postgres 16

#### Scenario: SQLite does not truncate to a prefix

- **WHEN** a dimension is `TRY_CAST(status AS INT)` over a row holding `'12abc'` on SQLite
- **THEN** it yields NULL, not 12

#### Scenario: Fractional text is not an integer

- **WHEN** a dimension is `TRY_CAST(code AS INT)` over rows holding `'1.5'`, `'1e3'`, `'-12'` and `' 7 '`
- **THEN** they yield NULL, NULL, -12 and 7 on every Tier-1 dialect

#### Scenario: A fractional number is not an integer

- **WHEN** a dimension is `TRY_CAST(ratio AS INT)` over a DOUBLE column holding 1.5 and 2.0
- **THEN** they yield NULL and 2 on every Tier-1 dialect

#### Scenario: Out-of-range integer

- **WHEN** a dimension is `TRY_CAST(code AS SMALLINT)` over a row holding `'99999'`
- **THEN** it yields NULL on every Tier-1 dialect

#### Scenario: Fractional text stays valid for a decimal target

- **WHEN** a dimension is `TRY_CAST(code AS DOUBLE)` over a row holding `'1.5'`
- **THEN** it yields 1.5

#### Scenario: TRY_CAST to an array type

- **WHEN** a dimension on Postgres is `TRY_CAST(codes AS INT[])`
- **THEN** it is rejected with a typed query error, while `CAST(codes AS INT[])` is accepted

#### Scenario: Postgres before 16

- **WHEN** the same query runs against a Postgres server reporting version 15
- **THEN** the generated SQL is a plain `CAST`, and a row holding `'abc'` fails the query with Postgres's error

#### Scenario: Postgres 16 uses the native validity check

- **WHEN** the query runs against a Postgres server reporting version 16 or later
- **THEN** the generated SQL guards the cast with Postgres's own input-validity check and `'abc'` yields NULL
