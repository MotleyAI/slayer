## MODIFIED Requirements

### Requirement: CAST has one meaning on every dialect

`CAST(x AS <type>)` and `x::<type>` SHALL be accepted in every Mode-B position, chains included (`x::int::text`). A type SHALL be written as type words with an optional parenthesised integer parameter list that may sit between words (`TIMESTAMP(6) WITH TIME ZONE`, `CHARACTER VARYING(10)`, `DECIMAL(10,2) UNSIGNED`), followed by an optional `[]` array suffix; a malformed type SHALL fail at parse time with an error about the type. The cast SHALL render the author's type, parameters included (`DECIMAL(10,2)`, `VARCHAR(3)`), changing only its spelling to the target dialect's. For every valid input its value SHALL be the same on every Tier-1 dialect, save the documented exceptions below:

- To `BOOLEAN`: a number SHALL be true exactly when it is not zero, a boolean SHALL be itself, and NULL SHALL stay NULL. The result SHALL be a nullable boolean in the dialect's own representation (1 and 0 where booleans are integers).
- To an integer type, a non-integral number SHALL round half away from zero. An exact-numeric tie SHALL round exactly so; a floating-point tie SHALL be the database's own, since a binary value is rarely an exact tie; this is documented.
- To `DECIMAL(p, s)` or `NUMERIC(p, s)`, a number or numeric text (an optionally signed decimal with an optional fraction and exponent, surrounding spaces ignored) SHALL keep every digit and round to `s` fractional digits half away from zero, exponent-form text included. An omitted scale SHALL be 0 and an omitted precision 38: `DECIMAL` and `NUMERIC` mean `DECIMAL(38, 0)`, and `DECIMAL(p)` means `DECIMAL(p, 0)`.
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

#### Scenario: Exponent-form text casts to a decimal exactly

- **WHEN** dimensions are `CAST('9007199254740993e0' AS DECIMAL(20,0))`, `CAST('12345e-2' AS DECIMAL(10,2))`, `CAST('1.2345e1' AS DECIMAL(10,3))` and `CAST('1.25e0' AS DECIMAL(5,1))`
- **THEN** they yield `9007199254740993`, `123.45`, `12.345` and `1.3` on every Tier-1 dialect, Trino and SQL Server included

#### Scenario: An omitted scale is zero

- **WHEN** dimensions are `CAST(12.5 AS DECIMAL)`, `CAST(12.4 AS NUMERIC)` and `CAST(12.5 AS DECIMAL(5))`
- **THEN** they yield `13`, `12` and `13` on every Tier-1 dialect

#### Scenario: A VARCHAR length keeps the first characters

- **WHEN** dimensions are `CAST('abcdef' AS VARCHAR(3))`, `CAST('ñandú' AS VARCHAR(2))` and `CAST('ab' AS VARCHAR(3))`
- **THEN** they yield `'abc'`, `'ña'` and `'ab'` on every Tier-1 dialect, DuckDB included

