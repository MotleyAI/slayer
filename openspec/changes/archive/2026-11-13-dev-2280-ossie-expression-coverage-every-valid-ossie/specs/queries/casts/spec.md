## ADDED Requirements

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
