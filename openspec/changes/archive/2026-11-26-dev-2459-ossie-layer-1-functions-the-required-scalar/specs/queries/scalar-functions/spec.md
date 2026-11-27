## MODIFIED Requirements

### Requirement: Version-gated functions fail closed

A function that needs a newer server than the connected one SHALL be rejected at plan time with a typed error naming the function and the required version, when the server version is known. When the version is unknown, the SQL SHALL be emitted and the database decides. `greatest` / `least` require SQL Server 2022 or later; regex functions require SQL Server 2025 or later. On MySQL, the regex functions and operators — and `TRY_CAST` to a number, with the text-to-number coercion that reads through it — require 8.0 or later, and `json_value` requires 8.0.21 or later. On PostgreSQL, `regexp_count` requires 15 or later.

#### Scenario: Old SQL Server

- **WHEN** a dimension is `greatest(a, b)` against a SQL Server reporting version 2019
- **THEN** the query fails at plan time naming `greatest` and SQL Server 2022

#### Scenario: Unknown version

- **WHEN** the same query is rendered without a server profile
- **THEN** the SQL is emitted

#### Scenario: Old MySQL

- **WHEN** a dimension is `json_value(payload, '$.a')` against a MySQL server reporting version 8.0.20
- **THEN** the query fails at plan time naming `json_value` and MySQL 8.0.21, and on 8.0.21 it runs

#### Scenario: Old PostgreSQL

- **WHEN** a dimension is `regexp_count(code, 'a')` against a PostgreSQL server reporting version 14, on execution and on dry run
- **THEN** the query fails at plan time naming `regexp_count` and PostgreSQL 15, and on 15 it runs

#### Scenario: regexp_count on SQL Server

- **WHEN** a dimension is `regexp_count(code, 'a')` against a SQL Server reporting version 2022, and against one reporting 2025
- **THEN** the first fails at plan time naming `regexp_count` and SQL Server 2025, and the second runs

### Requirement: Math functions

The library SHALL provide `div(a, b)` — the quotient truncated toward zero, INT when both operands are INT, else DOUBLE; `pi()`; `sin`, `cos`, `tan`, `asin`, `acos`, `atan` (radians); `atan2(y, x)`; `cbrt(x)`, the real cube root keeping the sign of `x`; `radians(x)` and `degrees(x)`, DOUBLE, converting degrees to radians and radians to degrees; and `trunc(x)` and `trunc(x, d)`, `x` truncated toward zero to `d` fractional digits (0 when omitted), a negative `d` truncating to the left of the decimal point. `truncate` SHALL be a spelling of `trunc` at both argument counts. `trunc(x, d)` SHALL have the result type `round(x, d)` has.

#### Scenario: div truncates toward zero

- **WHEN** dimensions are `div(7, 2)`, `div(-7, 2)`, `div(7, -2)` and `div(-7, -2)`
- **THEN** they yield `3`, `-3`, `-3` and `3` on every Tier-1 dialect

#### Scenario: cbrt of a negative number

- **WHEN** a dimension is `cbrt(x)` over a row holding `-8`
- **THEN** it yields `-2`

#### Scenario: The floor-division hint names a working function

- **WHEN** a query uses `a // b` and then follows the error's hint with `div(a, b)`
- **THEN** the second query runs

#### Scenario: trunc to digits

- **WHEN** dimensions are `trunc(x, 1)`, `TRUNCATE(x, 1)`, `trunc(x, 0)` and `trunc(x, -1)` over rows holding `2.789`, `-2.789` and `1234.5`
- **THEN** they yield `2.7`, `2.7`, `2` and `0` for `2.789`, `-2.7`, `-2.7`, `-2` and `0` for `-2.789`, and `1234.5`, `1234.5`, `1234` and `1230` for `1234.5`, on every Tier-1 dialect, SQLite included

#### Scenario: radians and degrees

- **WHEN** dimensions are `radians(180)` and `degrees(pi())`
- **THEN** they yield π and 180 within floating-point tolerance on every Tier-1 dialect

### Requirement: Division and rounding follow Ossie's numeric semantics

On every Tier-1 dialect, `/` SHALL be true division: the quotient keeps its fractional part whatever the operand types, two INT operands included. Two-argument `round(x, d)` over an exact-numeric (DECIMAL) `x` SHALL break an exact tie away from zero. For a floating-point `x`, ties SHALL be the database's own, since a binary value is rarely an exact tie; this is documented. A negative `d` SHALL round to the left of the decimal point on every Tier-1 dialect: `round(x, -k)` is the multiple of 10 to the power `k` nearest `x`, with the same tie rule. The digits argument `d` of `round(x, d)` and `trunc(x, d)` SHALL be an integer: a literal that is not a whole number SHALL be rejected at plan time with a typed error naming the function and the argument, and a non-literal `d` SHALL be of an integer type, else rejected the same way.

#### Scenario: Integer operands divide exactly

- **WHEN** a dimension is `i / 2` over INT rows holding `5`, `-5` and `4`
- **THEN** it yields `2.5`, `-2.5` and `2` on every Tier-1 dialect

#### Scenario: Decimal ties round away from zero

- **WHEN** dimensions are `round(d, 0)` and `round(e, 2)` over DECIMAL columns, `d` holding `2.5` and `-2.5` and `e` holding `0.125` and `-0.125`
- **THEN** they yield `3`, `-3`, `0.13` and `-0.13` on every Tier-1 dialect

#### Scenario: Negative digits round left of the decimal point

- **WHEN** dimensions are `round(i, -2)` over INT rows holding `1250`, `-1250` and `1249`, and `round(d, -1)` over a DECIMAL row holding `1234.5678`
- **THEN** they yield `1300`, `-1300`, `1200` and `1230` on every Tier-1 dialect, SQLite included

#### Scenario: Digits must be whole

- **WHEN** a dimension is `round(x, 1.5)`, `trunc(x, 1.5)`, `round(x, ratio)` or `trunc(x, ratio)` with `ratio` a DOUBLE column
- **THEN** each query fails at plan time with a typed error naming the function and its digits argument

#### Scenario: Integer column digits

- **WHEN** a dimension is `trunc(x, n)` with `n` an INT column, over a row holding `x` = `2.789` and `n` = `2`
- **THEN** it yields `2.78` on every Tier-1 dialect

### Requirement: Functions outside the library are rejected

`initcap`, `random`, format-string parsers (`to_date(x, fmt)`, `to_timestamp(x, fmt)`, `strftime`), JSON builders and JSON table functions SHALL NOT be Mode-B functions; a call SHALL fail with the existing unknown-function error.

#### Scenario: initcap

- **WHEN** a dimension is `initcap(name)`
- **THEN** the query fails as an unknown function

### Requirement: Every dialect translates a library function or refuses it

Each dialect SHALL run a portable library function — the string, math, `greatest` / `least`, `split_part`, `LIKE` / `ILIKE`, `json_value` and regex functions, their operator and alias spellings, and `TRY_CAST` with the text-to-number coercion that reads through it — only where SLayer holds a translation written for that dialect. On any other dialect the query SHALL fail at plan time, before SQL reaches the database, with a typed error naming the function and the database, on execution and dry run alike; SQL written for another database SHALL never be emitted. Tier-1 dialects translate the whole library (subject to version gates, the MariaDB refusals, and MySQL's refusal of `regexp_count`, which it has no translation for). Presto and Athena translate what Trino does except functions Presto lacks. Redshift, Databricks, Spark and Oracle translate `like`, `ilike`, `trim`, `ltrim`, `rtrim`, `replace`, `instr` / `strpos`, `greatest` and `least`, and refuse every other library function and `TRY_CAST`. `docs/database-support.md` SHALL list, per dialect, the functions it translates, and SHALL agree with the code.

#### Scenario: Refused on a Tier-2 database

- **WHEN** a query on a Redshift datasource has the dimension `json_value(payload, '$.a')`
- **THEN** it fails at plan time with a typed error naming `json_value` and Redshift, and no SQL is emitted

#### Scenario: Translated on a Tier-2 database

- **WHEN** a query on a Databricks datasource filters `code LIKE 'a\_%'`
- **THEN** the SQL is Databricks SQL and keeps `'a_1'` but not `'ab1'`

#### Scenario: Spellings share the decision

- **WHEN** a query on an Oracle datasource filters `notes ~ 'paid'`, or has the dimension `payload ->> 'a'`
- **THEN** it fails at plan time naming the regex or JSON function and Oracle, as `regexp_like` and `json_value` do

#### Scenario: Coercion needs TRY_CAST

- **WHEN** a query on a Spark datasource filters `amount_txt > 100` with `amount_txt` a TEXT column
- **THEN** it fails at plan time with a typed error naming the text-to-number comparison and Spark

#### Scenario: Presto gets Presto SQL

- **WHEN** a query on a Presto or Athena datasource has the dimension `regexp_extract(notes, 'paid(.*)late', 1)` and the dimension `json_value(payload, '$.a.b')`
- **THEN** the SQL uses Presto's own regex and JSON functions, contains no Postgres-only function, and its pattern makes `.` match newlines

#### Scenario: Every dialect and function is decided

- **WHEN** any registered dialect meets any library function
- **THEN** the dialect either emits its own SQL for it or refuses it with the typed error, and the test suite fails on any other outcome

#### Scenario: regexp_count on MySQL

- **WHEN** a query on a MySQL datasource has the dimension `regexp_count(code, 'a')`
- **THEN** it fails at plan time with a typed error naming `regexp_count` and MySQL, on execution and dry run alike

## ADDED Requirements

### Requirement: Ossie conditional spellings

`IF(c, a, b)` SHALL mean `iif(c, a, b)`, as `IFF` does; `NVL(a, b)` SHALL mean `ifnull(a, b)`; and `NVL2(e, a, b)` SHALL mean `iif(e IS NOT NULL, a, b)` — each matched case-insensitively, with its twin's value, result key and SQL. `zeroifnull(x)` SHALL yield `x`, or 0 when `x` is NULL, and `nullifzero(x)` SHALL yield `x`, or NULL when `x` is 0; both read their argument as a number, so a TEXT argument SHALL keep the numeric-function type error whose remedy names `TRY_CAST`, raised when the definition is saved where the argument's type is known. A call with another argument count SHALL be rejected naming the function. Inside a model that declares a custom aggregation of one of these names, a call SHALL be that aggregation.

#### Scenario: Conditionals evaluate

- **WHEN** dimensions are `IF(amount > 20, 1, 0)`, `NVL(shipped_at, created_at)`, `NVL2(shipped_at, 'y', 'n')`, `ZEROIFNULL(sla_days)` and `NULLIFZERO(sla_days)` over rows holding `amount` 30, `shipped_at` NULL and `sla_days` NULL, and over rows holding `amount` 10, `shipped_at` set and `sla_days` 0
- **THEN** the first rows yield `1`, `created_at`, `'n'`, `0` and NULL, and the second `0`, `shipped_at`, `'y'`, `0` and NULL, on every Tier-1 dialect

#### Scenario: Twins share the key

- **WHEN** one query has the dimension `IF(amount > 20, 1, 0)` and another `iif(amount > 20, 1, 0)`
- **THEN** both have the same result key and SQL

#### Scenario: ZEROIFNULL over TEXT

- **WHEN** a dataset field's expression is `ZEROIFNULL(status)` with `status` a TEXT column, and a query's dimension is `NULLIFZERO(status)`
- **THEN** saving the field fails with the numeric-function type error naming `TRY_CAST`, and the query fails at plan time with the same error

#### Scenario: Custom aggregation named nvl

- **WHEN** a model declares a custom aggregation `nvl` and a measure uses `nvl(amount)`, and another model declares a custom aggregation `if`
- **THEN** the measure renders the model's `nvl` aggregation, and `if(amount)` in the second model renders its `if` aggregation

### Requirement: Ossie string-search spellings

`STARTSWITH(s, p)` and `ENDSWITH(s, p)` SHALL mean `starts_with(s, p)` and `ends_with(s, p)`, and `CHARINDEX(sub, s)` SHALL mean `instr(s, sub)`, the 1-based position of `sub` in `s` or 0 — each matched case-insensitively, with its twin's value, result key and SQL. `CHARINDEX` with three arguments (a start position) SHALL be rejected naming the function and its argument count.

#### Scenario: Search spellings evaluate

- **WHEN** dimensions are `STARTSWITH(status, 'pe')`, `ENDSWITH(status, 'ing')`, `CHARINDEX('d', status)` and `CHARINDEX('z', status)` over a row holding `'pending'`
- **THEN** they yield true, true, `4` and `0` on every Tier-1 dialect

#### Scenario: CHARINDEX with a start position

- **WHEN** a dimension is `CHARINDEX('d', status, 2)`
- **THEN** the query is rejected naming `CHARINDEX` and its argument count

### Requirement: The strict binding admits exactly the Ossie functions that run

Under `conformance="ossie-l3"`, a scalar function call SHALL be admitted exactly when Ossie Layer 1 defines that spelling at that argument count and SLayer evaluates it with Ossie's meaning. Among the functions this capability adds, that is `IF`, `IFF`, `NVL`, `NVL2`, `ZEROIFNULL`, `NULLIFZERO`, `STARTSWITH`, `ENDSWITH`, `CHARINDEX` (2), `TRUNC` and `TRUNCATE` (2), `TO_DATE` and `TO_TIMESTAMP` (1), `RADIANS`, `DEGREES`, `REGEXP_COUNT` (2), `EXTRACT` / `DATE_PART` with `DAYOFWEEK` and `MILLISECOND`, and `TIMESTAMP_NTZ` literals. Every other call SHALL raise `E_INVALID_QUERY` and answer under `slayer` where it is SLayer syntax.

#### Scenario: New Ossie functions are admitted

- **WHEN** strict queries' filters use each of `IF(orders.amount > 20, 1, 0) = 1`, `NVL(orders.note, 'x') = 'x'`, `TRUNC(orders.amount, 1) > 0`, `TO_DATE('2024-01-15') < orders.order_date`, `EXTRACT(MILLISECOND FROM orders.created_at) = 0` and `orders.created_at > TIMESTAMP_NTZ '2024-01-01 00:00:00'`
- **THEN** each answers with the rows Ossie defines

#### Scenario: Non-Ossie arities and spellings are refused

- **WHEN** strict queries' filters use `TRUNC(orders.amount) = 0`, `TRUNCATE(orders.amount) = 0`, `TRUNCATE(orders.amount, 1, 2) = 0`, `CHARINDEX('d', orders.status, 2) = 0`, `REGEXP_COUNT(orders.status, 'n', 'i') = 2`, `starts_with(orders.status, 'p')` and `DAYOFWEEK(orders.created_at) = 1`
- **THEN** each fails with `E_INVALID_QUERY`
