## ADDED Requirements

### Requirement: Every dialect translates a library function or refuses it

Each dialect SHALL run a portable library function — the string, math, `greatest` / `least`, `split_part`, `LIKE` / `ILIKE`, `json_value` and regex functions, their operator and alias spellings, and `TRY_CAST` with the text-to-number coercion that reads through it — only where SLayer holds a translation written for that dialect. On any other dialect the query SHALL fail at plan time, before SQL reaches the database, with a typed error naming the function and the database, on execution and dry run alike; SQL written for another database SHALL never be emitted. Tier-1 dialects translate the whole library (subject to version gates and the MariaDB refusals). Presto and Athena translate what Trino does except functions Presto lacks. Redshift, Databricks, Spark and Oracle translate `like`, `ilike`, `trim`, `ltrim`, `rtrim`, `replace`, `instr` / `strpos`, `greatest` and `least`, and refuse every other library function and `TRY_CAST`. `docs/database-support.md` SHALL list, per dialect, the functions it translates, and SHALL agree with the code.

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

- **WHEN** a query on a Presto or Athena datasource has the dimension `regexp_extract(notes, 'a(b)', 1)` and the dimension `json_value(payload, '$.a.b')`
- **THEN** the SQL uses Presto's own regex and JSON functions, contains no Postgres-only function, and `.` matches newlines

#### Scenario: Every dialect and function is decided

- **WHEN** any registered dialect meets any library function
- **THEN** the dialect either emits its own SQL for it or refuses it with the typed error, and the test suite fails on any other outcome

## MODIFIED Requirements

### Requirement: Out-of-domain arguments yield NULL

A function argument outside the function's domain — `asin` / `acos` beyond ±1, `div` by zero, a `chr` code point that is a surrogate or above `0x10FFFF`, `ln` / `log10` / `log2` / one-argument `log` of a value ≤ 0, two-argument `log(b, x)` with `x` ≤ 0 or a base `b` ≤ 0 or equal to 1, `sqrt` of a negative value, `pow` / `power` of a negative base to a non-integer exponent or of 0 to a negative exponent — SHALL yield NULL on every dialect, as `/` by zero does. When the out-of-domain arguments are literals, the query SHALL be rejected at plan time with a typed error naming the function and the argument. A result too large to represent (overflow, e.g. `exp(1000)`) is outside this rule: it SHALL be the database's own behaviour, and that is documented.

#### Scenario: Column argument out of domain

- **WHEN** a dimension is `asin(ratio)` over rows holding `0.5` and `2`
- **THEN** they yield `asin(0.5)` and NULL on every Tier-1 dialect

#### Scenario: Literal argument out of domain

- **WHEN** a dimension is `asin(2)`, `ln(0)`, `sqrt(-1)`, `log(1, x)` or `pow(-8, 0.5)`
- **THEN** the query fails at plan time with a typed error naming the function and its argument

#### Scenario: Division by zero

- **WHEN** a dimension is `div(amount, qty)` over a row where `qty` is 0
- **THEN** it yields NULL on every Tier-1 dialect

#### Scenario: Logarithm and square root of a column

- **WHEN** dimensions are `ln(x)`, `log10(x)`, `log2(x)`, `log(x)`, `log(2, x)` and `sqrt(x)` over rows holding `1`, `0` and `-1`
- **THEN** the row holding `1` yields `0`, `0`, `0`, `0`, `0` and `1`, and the rows holding `0` and `-1` yield NULL for every logarithm, and `-1` yields NULL for `sqrt` — on every Tier-1 dialect, never an error

#### Scenario: Power of a column

- **WHEN** a dimension is `pow(b, e)` over rows `(-8, 0.5)`, `(-8, 3)`, `(0, -1)` and `(4, 0.5)`
- **THEN** they yield NULL, `-512`, NULL and `2` on every Tier-1 dialect

### Requirement: Version-gated functions fail closed

A function that needs a newer server than the connected one SHALL be rejected at plan time with a typed error naming the function and the required version, when the server version is known. When the version is unknown, the SQL SHALL be emitted and the database decides. `greatest` / `least` require SQL Server 2022 or later; regex functions require SQL Server 2025 or later. On MySQL, the regex functions and operators — and `TRY_CAST` to a number, with the text-to-number coercion that reads through it — require 8.0 or later, and `json_value` requires 8.0.21 or later.

#### Scenario: Old SQL Server

- **WHEN** a dimension is `greatest(a, b)` against a SQL Server reporting version 2019
- **THEN** the query fails at plan time naming `greatest` and SQL Server 2022

#### Scenario: Unknown version

- **WHEN** the same query is rendered without a server profile
- **THEN** the SQL is emitted

#### Scenario: Old MySQL

- **WHEN** a dimension is `json_value(payload, '$.a')` against a MySQL server reporting version 8.0.20
- **THEN** the query fails at plan time naming `json_value` and MySQL 8.0.21, and on 8.0.21 it runs
