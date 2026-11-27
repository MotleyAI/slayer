## MODIFIED Requirements

### Requirement: A cast's result type is inferred

A cast SHALL type as SLayer's data type for its target: the integer family as INT; floating-point, `REAL`, `DECIMAL` and `NUMERIC` as DOUBLE; character types as TEXT; `BOOLEAN` as BOOLEAN; `DATE` as DATE; `TIMESTAMP` and `DATETIME` as TIMESTAMP; `TIME`, with or without a precision and without a time zone, as the time of day (`queries/time-of-day`); any other type, a time type with a time zone included, as UNKNOWN. The target SHALL be read as Postgres spells it, and as the target dialect spells it only when Postgres does not know the type (`UBIGINT`, `UInt32`), so a type word means the same on every dialect. A cast to `T[]` SHALL type as a list whose elements have `T`'s type (UNKNOWN elements when `T` maps to UNKNOWN), usable wherever a list column is. The inferred type SHALL drive checking and the query plan's types exactly as a column's declared type does.

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

#### Scenario: Cast to TIME is a time of day

- **WHEN** dimensions on Postgres are `CAST(opened_txt AS TIME)`, `CAST(opened_txt AS TIME(3))` and `CAST(opened_txt AS TIME WITH TIME ZONE)`
- **THEN** the first two are planned as `Time` and the third as UNKNOWN

### Requirement: SQLite casts to timestamp and time types

SQLite has no timestamp type, so on SQLite `CAST(x AS T)` and `x::T` for a timestamp type `T` without a time-zone modifier or precision parameter (`TIMESTAMP`, `DATETIME`, `TIMESTAMP_NTZ`, `TIMESTAMP WITHOUT TIME ZONE`) SHALL yield the value `TRY_CAST(x AS T)` yields there: the ISO timestamp of ISO date or date-time input, a date at its midnight, and NULL for any other input. It SHALL never yield the numeric prefix of the input. SQLite has no time-of-day type either, so `CAST(x AS TIME)` and `x::TIME` SHALL likewise yield the value `TRY_CAST(x AS TIME)` yields there: the time of day of ISO time text or of a timestamp (`queries/time-of-day`), and NULL for any other input. A cast on SQLite to a time or timestamp type with a time-zone modifier, or to a timestamp type with a precision parameter, SHALL be rejected at plan time with the typed query error naming the type and the dialect, as for any type the dialect cannot express; `TRY_CAST` to those types SHALL be rejected the same way.

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

- **WHEN** a dimension on SQLite is `CAST(created_at AS TIMESTAMP(3))`, `CAST(created_at AS TIMESTAMP WITH TIME ZONE)`, `CAST(created_at AS TIME WITH TIME ZONE)` or `TRY_CAST(created_at AS TIMESTAMP(3))`
- **THEN** the query fails at plan time with the typed error naming the type and SQLite, and no SQL is executed

#### Scenario: Time cast on SQLite

- **WHEN** dimensions on SQLite are `CAST(created_at AS TIME)` over a row at `2024-01-15 10:30:45`, `'10:30:00'::TIME` and `CAST('bogus' AS TIME)`
- **THEN** they yield 10:30:45, 10:30:00 and NULL
