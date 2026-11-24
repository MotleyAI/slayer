# queries/casts Specification

## Purpose
Defines casts in Mode-B expressions: `CAST` / `::` as the database's own cast of the author's type, and `TRY_CAST` as the one portable cast that turns an invalid input into NULL on every dialect.

## Requirements

### Requirement: CAST has one meaning on every dialect

`CAST(x AS <type>)` and `x::<type>` SHALL be accepted in every Mode-B position, chains included (`x::int::text`). A type SHALL be written as type words with an optional parenthesised integer parameter list that may sit between words (`TIMESTAMP(6) WITH TIME ZONE`, `CHARACTER VARYING(10)`, `DECIMAL(10,2) UNSIGNED`), followed by an optional `[]` array suffix; a malformed type SHALL fail at parse time with an error about the type. The cast SHALL render the author's type, parameters included (`DECIMAL(10,2)`, `VARCHAR(3)`), changing only its spelling to the target dialect's. For every valid input its value SHALL be the same on every Tier-1 dialect:

- To `BOOLEAN`: a number SHALL be true exactly when it is not zero, a boolean SHALL be itself, and NULL SHALL stay NULL. The result SHALL be a nullable boolean in the dialect's own representation (1 and 0 where booleans are integers).
- To an integer type, a non-integral number SHALL round half away from zero. An exact-numeric tie SHALL round exactly so; a floating-point tie SHALL be the database's own, since a binary value is rarely an exact tie; this is documented.
- To `DECIMAL(p, s)` or `NUMERIC(p, s)`, a number or numeric text (an optionally signed decimal with an optional fraction and exponent, surrounding spaces ignored) SHALL round to `s` fractional digits half away from zero. On Trino and SQL Server, whose own cast reads no exponent, exponent-form text is read through a double, so its digits past a double's precision are not kept; this is documented. An omitted scale SHALL be 0 and an omitted precision 38: `DECIMAL` and `NUMERIC` mean `DECIMAL(38, 0)`, and `DECIMAL(p)` means `DECIMAL(p, 0)`.
- To `VARCHAR(n)` or `STRING(n)`, the value SHALL be the first `n` characters (Unicode code points) of its text, never an error for a longer value.

Invalid input — text that is not a literal of the target type, or a value outside the target's range or precision — SHALL follow the database's own `CAST` unless a requirement below says otherwise. A type Ossie does not define SHALL keep the database's own conversion. These rules govern the casts an author writes; the casts SLayer emits itself to read or type a value SHALL keep their values. A type the target dialect cannot express SHALL be rejected at plan time with a typed query error naming the type and the dialect; a type is not expressed when the target's rendering would drop or change any part of it — its family, precision or scale, time-zone modifier, signedness, or array suffix — save a `VARCHAR` length, and on BigQuery a `DECIMAL` precision and scale, which the cast applies itself (`TRY_CAST` yields NULL past the precision). An array of arrays (`T[][]`) SHALL be rejected.

#### Scenario: Author's parameterised type is kept

- **WHEN** a measure on Postgres is `sum(CAST(amount AS DECIMAL(10,2)))`
- **THEN** the generated SQL casts to `DECIMAL(10, 2)` and a row holding `12.345` contributes `12.35`

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

#### Scenario: Wide integer type only where the database has one

- **WHEN** a dimension is `CAST(amount AS HUGEINT)` on DuckDB and the same dimension on Postgres
- **THEN** DuckDB casts to its 128-bit integer, and Postgres rejects it at plan time with the typed cast error

#### Scenario: Malformed type

- **WHEN** a dimension is `CAST(amount AS DECIMAL(10,))`
- **THEN** it fails at parse time with an error about the type, not about a missing `)`

#### Scenario: Array cast on a dialect with arrays

- **WHEN** a dimension on Postgres is `length(CAST(codes AS INT[]))` and on DuckDB `length(codes::INT[])`
- **THEN** each renders the database's array cast

#### Scenario: Array cast on a dialect without arrays

- **WHEN** a dimension on SQLite is `CAST(codes AS INT[])`
- **THEN** the query is rejected at plan time with the typed cast error naming the type and the dialect

#### Scenario: Array of arrays

- **WHEN** a dimension on Postgres is `CAST(codes AS INT[][])`
- **THEN** it is rejected with a typed error

#### Scenario: A number cast to BOOLEAN is true when it is not zero

- **WHEN** a dimension is `CAST(qty AS BOOLEAN)` over INT rows holding `0`, `5`, `-1` and NULL, and a filter is `CAST(qty AS BOOLEAN) = TRUE`
- **THEN** the dimension yields false, true, true and NULL, and the filter keeps exactly the rows holding `5` and `-1`, on every Tier-1 dialect

#### Scenario: A fractional number cast to an integer rounds half away from zero

- **WHEN** a dimension is `CAST(d AS INT)` over DECIMAL rows holding `2.5`, `-2.5`, `2.4` and `2.675`
- **THEN** it yields `3`, `-3`, `2` and `3` on every Tier-1 dialect

#### Scenario: A decimal cast rounds to its scale

- **WHEN** dimensions are `CAST(12.345 AS DECIMAL(10,2))`, `CAST('12.345' AS DECIMAL(10,2))` and `CAST(-12.345 AS NUMERIC(10,2))`
- **THEN** they yield `12.35`, `12.35` and `-12.35` on every Tier-1 dialect

#### Scenario: An omitted scale is zero

- **WHEN** dimensions are `CAST(12.5 AS DECIMAL)`, `CAST(12.4 AS NUMERIC)` and `CAST(12.5 AS DECIMAL(5))`
- **THEN** they yield `13`, `12` and `13` on every Tier-1 dialect

#### Scenario: A VARCHAR length keeps the first characters

- **WHEN** dimensions are `CAST('abcdef' AS VARCHAR(3))`, `CAST('ñandú' AS VARCHAR(2))` and `CAST('ab' AS VARCHAR(3))`
- **THEN** they yield `'abc'`, `'ña'` and `'ab'` on every Tier-1 dialect, DuckDB included

### Requirement: A cast's result type is inferred

A cast SHALL type as SLayer's data type for its target: the integer family as INT; floating-point, `REAL`, `DECIMAL` and `NUMERIC` as DOUBLE; character types as TEXT; `BOOLEAN` as BOOLEAN; `DATE` as DATE; `TIMESTAMP` and `DATETIME` as TIMESTAMP; any other type as UNKNOWN. The target SHALL be read as Postgres spells it, and as the target dialect spells it only when Postgres does not know the type (`UBIGINT`, `UInt32`), so a type word means the same on every dialect. A cast to `T[]` SHALL type as a list whose elements have `T`'s type (UNKNOWN elements when `T` maps to UNKNOWN), usable wherever a list column is. The inferred type SHALL drive checking and the query plan's types exactly as a column's declared type does.

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

#### Scenario: Vendor type name

- **WHEN** a dimension is `CAST(amount AS UBIGINT)` on DuckDB and `CAST(amount AS UInt32)` on ClickHouse
- **THEN** each one's planned type is INT

#### Scenario: A type word means the same on every dialect

- **WHEN** a dimension on SQL Server is `TRY_CAST(created_txt AS TIMESTAMP)`
- **THEN** its planned type is TIMESTAMP and it converts to SQL Server's timestamp type, not its `ROWVERSION` synonym

### Requirement: TRY_CAST turns invalid input into NULL on every dialect

`TRY_CAST(x AS <type>)` SHALL render the author's type like `CAST`, SHALL yield the value `CAST(x AS <type>)` yields per "CAST has one meaning on every dialect" for every input that cast converts, and SHALL yield NULL, never an error or a substitute value, for an input the cast cannot convert, on every Tier-1 dialect. For an integer-family target, on every Tier-1 dialect, a text input SHALL be valid exactly when it is an optionally signed run of decimal digits, surrounding spaces ignored; a numeric input SHALL round half away from zero as `CAST` does; and either SHALL be valid only when the resulting integer is within the target's range. A boolean input to a number target SHALL read as 1 or 0, and a number input to a `BOOLEAN` target SHALL be true exactly when it is not zero. For a `DECIMAL(p, s)` target a valid input SHALL round to `s` fractional digits as `CAST` does and SHALL be NULL when the rounded value has more than `p` digits. Where the database cannot itself tell a valid input from an invalid one (SQLite, MySQL numeric targets): any other number target SHALL accept an optionally signed decimal with an optional fraction and exponent, surrounding spaces ignored; a boolean SHALL be one of `true`, `false`, `t`, `f`, `yes`, `no`, `1`, `0` in any case; a date or timestamp SHALL be an ISO time-point shape naming a real calendar value. `TRY_CAST` to an array type SHALL be rejected with a typed query error on every dialect. On Postgres, a server older than 16 — or one whose version cannot be determined, including a dry run without a connection — SHALL get a plain `CAST`, so an invalid input fails the query there; this is documented.

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
- **THEN** they yield 2 and 2 on every Tier-1 dialect, the fractional number rounding half away from zero as `CAST` does

#### Scenario: Out-of-range integer

- **WHEN** a dimension is `TRY_CAST(code AS SMALLINT)` over a row holding `'99999'`
- **THEN** it yields NULL on every Tier-1 dialect

#### Scenario: A boolean reads as 1 or 0

- **WHEN** dimensions are `TRY_CAST(amount > 15 AS INT)` and `TRY_CAST(amount > 15 AS DOUBLE)` over rows whose amount is 10, 20 and NULL
- **THEN** they yield 0, 1 and NULL on every Tier-1 dialect

#### Scenario: Integer at the ends of its range

- **WHEN** a dimension is `TRY_CAST(big AS BIGINT)` over a BIGINT column holding 9223372036854775807 and -9223372036854775808
- **THEN** each yields itself on every Tier-1 dialect

#### Scenario: Out-of-range number is NULL, not an error

- **WHEN** a dimension is `TRY_CAST(CAST(1e19 AS DOUBLE) AS BIGINT)`
- **THEN** it yields NULL on every Tier-1 dialect, including a database that evaluates constant casts at plan time

#### Scenario: Integer type wider than 64 bits

- **WHEN** a dimension on DuckDB is `TRY_CAST(ratio AS UBIGINT)` over a DOUBLE column holding 1e19 and 2e19
- **THEN** they yield 10000000000000000000 and NULL

#### Scenario: Fractional text stays valid for a decimal target

- **WHEN** a dimension is `TRY_CAST(code AS DOUBLE)` over a row holding `'1.5'`
- **THEN** it yields 1.5

#### Scenario: TRY_CAST to an array type

- **WHEN** a dimension on Postgres is `TRY_CAST(codes AS INT[])`
- **THEN** it is rejected with a typed query error, while `length(CAST(codes AS INT[]))` is accepted

#### Scenario: Postgres before 16

- **WHEN** the same query runs against a Postgres server reporting version 15
- **THEN** the generated SQL is a plain `CAST`, and a row holding `'abc'` fails the query with Postgres's error

#### Scenario: Postgres 16 uses the native validity check

- **WHEN** the query runs against a Postgres server reporting version 16 or later
- **THEN** the generated SQL guards the cast with Postgres's own input-validity check and `'abc'` yields NULL

#### Scenario: Rounding decides the range

- **WHEN** dimensions are `TRY_CAST(CAST(32766.5 AS DECIMAL(7,1)) AS SMALLINT)` and `TRY_CAST(CAST(32767.5 AS DECIMAL(7,1)) AS SMALLINT)`
- **THEN** they yield 32767 and NULL on every Tier-1 dialect, the second rounding to 32768, outside the range

#### Scenario: A number is a boolean

- **WHEN** a dimension is `TRY_CAST(qty AS BOOLEAN)` over INT rows holding `0`, `5` and NULL
- **THEN** it yields false, true and NULL on every Tier-1 dialect

#### Scenario: A decimal target rounds, and overflow is NULL

- **WHEN** dimensions are `TRY_CAST(12.345 AS DECIMAL(10,2))`, `TRY_CAST('12.345' AS DECIMAL(10,2))` and `TRY_CAST(12345.6 AS DECIMAL(3,1))`
- **THEN** they yield `12.35`, `12.35` and NULL on every Tier-1 dialect

### Requirement: SQLite casts to timestamp and time types

SQLite has no timestamp type, so on SQLite `CAST(x AS T)` and `x::T` for a timestamp type `T` without a time-zone modifier or precision parameter (`TIMESTAMP`, `DATETIME`, `TIMESTAMP_NTZ`, `TIMESTAMP WITHOUT TIME ZONE`) SHALL yield the value `TRY_CAST(x AS T)` yields there: the ISO timestamp of ISO date or date-time input, a date at its midnight, and NULL for any other input. It SHALL never yield the numeric prefix of the input. A cast on SQLite to `TIME`, to a timestamp type with a time-zone modifier, or to a timestamp type with a precision parameter SHALL be rejected at plan time with the typed query error naming the type and the dialect, as for any type the dialect cannot express; `TRY_CAST` to those types SHALL be rejected the same way.

#### Scenario: Timestamp cast of a date column

- **WHEN** a dimension on SQLite is `CAST(order_date AS TIMESTAMP)` over a row dated 2024-12-30
- **THEN** it yields `2024-12-30 00:00:00`, not `2024`

#### Scenario: Timestamp spellings agree

- **WHEN** dimensions on SQLite are `CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)`, `'2024-01-15 10:30:00'::DATETIME` and `TRY_CAST('2024-01-15 10:30:00' AS TIMESTAMP)`
- **THEN** each yields `2024-01-15 10:30:00`

#### Scenario: Timestamp cast in a filter

- **WHEN** a filter on SQLite is `created_at <= CAST('2024-04-10' AS TIMESTAMP)` over rows at `2024-03-05 08:00` and `2024-04-10 09:30`
- **THEN** it matches the first row, as on DuckDB

#### Scenario: Inexpressible types are refused

- **WHEN** a dimension on SQLite is `CAST(created_at AS TIME)`, `CAST(created_at AS TIMESTAMP(3))`, `CAST(created_at AS TIMESTAMP WITH TIME ZONE)` or `TRY_CAST(created_at AS TIME)`
- **THEN** the query fails at plan time with the typed error naming the type and SQLite, and no SQL is executed

### Requirement: SQLite casts to boolean, decimal and varchar types

SQLite has no boolean or fixed-point decimal type and ignores a character type's length, so on SQLite `CAST(x AS T)` and `x::T` SHALL yield, for `T` a `BOOLEAN`, a `DECIMAL` / `NUMERIC` type or a `VARCHAR(n)` / `STRING(n)` type, the value "CAST has one meaning on every dialect" defines, and for an input that rule does not convert the value `TRY_CAST(x AS T)` yields there: NULL for text that is not a boolean word (`true`, `false`, `t`, `f`, `yes`, `no`, `1`, `0`, any case, surrounding spaces ignored) or not numeric text, and NULL for a decimal value with more than `p` digits after rounding. It SHALL never yield the numeric prefix of the input.

#### Scenario: Boolean words and other text

- **WHEN** dimensions on SQLite are `CAST('true' AS BOOLEAN)`, `CAST(' No ' AS BOOLEAN)` and `CAST('abc' AS BOOLEAN)`
- **THEN** they yield 1, 0 and NULL

#### Scenario: Decimal text that is not a number

- **WHEN** a dimension on SQLite is `CAST(code AS DECIMAL(10,2))` over rows holding `'12.345'` and `'12abc'`
- **THEN** they yield `12.35` and NULL, not `12`

#### Scenario: Decimal overflow

- **WHEN** a dimension on SQLite is `CAST(12345.6 AS DECIMAL(3,1))`
- **THEN** it yields NULL
