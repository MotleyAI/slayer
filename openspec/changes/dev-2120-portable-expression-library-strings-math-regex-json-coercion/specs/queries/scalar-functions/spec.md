## Purpose

Defines the Mode-B scalar function library: the rules that give every function one meaning on every Tier-1 dialect, and the string, math and `greatest` / `least` functions.

## ADDED Requirements

### Requirement: One meaning on every Tier-1 dialect

Every Mode-B scalar function SHALL return the same value for the same inputs on SQLite, Postgres, DuckDB, MySQL, ClickHouse, SQL Server, Trino, Snowflake and BigQuery. Where the databases disagree on an edge case — NULL arguments, out-of-range positions, empty strings, negative counts — the meaning SHALL be the value Postgres returns. Every function in the library SHALL be verified by execution on every Tier-1 dialect.

#### Scenario: Every library function is covered on every dialect

- **WHEN** a scalar function is in the Mode-B library
- **THEN** the per-dialect test matrix holds executed cases for it on every Tier-1 dialect, and a function without cases fails the test suite

#### Scenario: Postgres edge case is the meaning

- **WHEN** a dimension is `left(code, -2)` over a row holding `'ABCDE'`
- **THEN** it yields `'ABC'` on every Tier-1 dialect

### Requirement: Out-of-domain arguments yield NULL

A function argument outside the function's domain — `asin` / `acos` beyond ±1, `div` by zero, a `chr` code point that is a surrogate or above `0x10FFFF` — SHALL yield NULL on every dialect, as `/` by zero does. When the out-of-domain argument is a literal, the query SHALL be rejected at plan time with a typed error naming the function and the argument.

#### Scenario: Column argument out of domain

- **WHEN** a dimension is `asin(ratio)` over rows holding `0.5` and `2`
- **THEN** they yield `asin(0.5)` and NULL on every Tier-1 dialect

#### Scenario: Literal argument out of domain

- **WHEN** a dimension is `asin(2)`
- **THEN** the query fails at plan time with a typed error naming `asin` and its argument

#### Scenario: Division by zero

- **WHEN** a dimension is `div(amount, qty)` over a row where `qty` is 0
- **THEN** it yields NULL on every Tier-1 dialect

### Requirement: Version-gated functions fail closed

A function that needs a newer server than the connected one SHALL be rejected at plan time with a typed error naming the function and the required version, when the server version is known. When the version is unknown, the SQL SHALL be emitted and the database decides. `greatest` / `least` require SQL Server 2022 or later; regex functions require SQL Server 2025 or later.

#### Scenario: Old SQL Server

- **WHEN** a dimension is `greatest(a, b)` against a SQL Server reporting version 2019
- **THEN** the query fails at plan time naming `greatest` and SQL Server 2022

#### Scenario: Unknown version

- **WHEN** the same query is rendered without a server profile
- **THEN** the SQL is emitted

### Requirement: String functions

The library SHALL provide, with Postgres meanings:

- `left(s, n)` / `right(s, n)` — the first / last `n` characters; a negative `n` drops the last / first `|n|` characters.
- `starts_with(s, p)` / `ends_with(s, p)` / `contains(s, p)` — BOOLEAN; `p` is plain text, never a pattern, matched case- and accent-sensitively whatever the column's collation.
- `strpos(s, p)` — the 1-based position of the first `p` in `s`, 0 when absent, matched like `contains`; the same function as `instr` and `POSITION(p IN s)`.
- `lpad(s, n[, fill])` / `rpad(s, n[, fill])` — pad to `n` characters with `fill` (default one space), truncating `s` when it is longer than `n`.
- `reverse(s)` — the characters of `s` in reverse order.
- `repeat(s, n)` — `s` repeated `n` times; empty for `n ≤ 0`.
- `split_part(s, delimiter, n)` — the `n`-th field; `n` SHALL be a non-zero integer literal, negative counting from the end; a field beyond the last yields `''`.
- `concat_ws(separator, a, …)` — the non-NULL arguments joined by the separator.
- `chr(n)` — the character with a Unicode code point from `1` through `0x10FFFF`, excluding surrogates; `ascii(s)` — the code point of the first character of `s`.

Lengths, positions and counts SHALL be in characters, not bytes.

#### Scenario: Plain-text containment

- **WHEN** a filter is `contains(code, '5%')` over rows holding `'15%'` and `'150'`
- **THEN** it keeps `'15%'` only

#### Scenario: Case-sensitive whatever the collation

- **WHEN** a filter is `starts_with(name, 'ad')` over a row holding `'Ada'` on MySQL with a case-insensitive collation
- **THEN** it drops the row

#### Scenario: Non-ASCII characters

- **WHEN** dimensions are `reverse(name)`, `ascii(name)` and `left(name, 1)` over a row holding `'€é'`
- **THEN** they yield `'é€'`, `8364` and `'€'` on every Tier-1 dialect

#### Scenario: Astral code point

- **WHEN** a dimension is `chr(128512)`
- **THEN** it yields the one character U+1F600 on every Tier-1 dialect

#### Scenario: split_part index

- **WHEN** dimensions are `split_part(path, '/', 2)`, `split_part(path, '/', -1)` and `split_part(path, '/', 9)` over a row holding `'a/b/c'`
- **THEN** they yield `'b'`, `'c'` and `''`

#### Scenario: split_part non-literal or zero index

- **WHEN** a dimension is `split_part(path, '/', depth)` or `split_part(path, '/', 0)`
- **THEN** the query fails at plan time with a typed error

#### Scenario: lpad truncates

- **WHEN** a dimension is `lpad(code, 3, '0')` over rows holding `'7'` and `'12345'`
- **THEN** they yield `'007'` and `'123'`

#### Scenario: concat_ws skips NULLs

- **WHEN** a dimension is `concat_ws('-', a, b, c)` over a row where `b` is NULL, `a` is `'x'` and `c` is `'z'`
- **THEN** it yields `'x-z'`

### Requirement: Math functions

The library SHALL provide `div(a, b)` — the quotient truncated toward zero, INT when both operands are INT, else DOUBLE; `pi()`; `sin`, `cos`, `tan`, `asin`, `acos`, `atan` (radians); `atan2(y, x)`; and `cbrt(x)`, the real cube root keeping the sign of `x`.

#### Scenario: div truncates toward zero

- **WHEN** dimensions are `div(7, 2)`, `div(-7, 2)`, `div(7, -2)` and `div(-7, -2)`
- **THEN** they yield `3`, `-3`, `-3` and `3` on every Tier-1 dialect

#### Scenario: cbrt of a negative number

- **WHEN** a dimension is `cbrt(x)` over a row holding `-8`
- **THEN** it yields `-2`

#### Scenario: The floor-division hint names a working function

- **WHEN** a query uses `a // b` and then follows the error's hint with `div(a, b)`
- **THEN** the second query runs

### Requirement: greatest and least ignore NULLs

`greatest(a, b, …)` and `least(a, b, …)` SHALL return the greatest / least non-NULL argument, and NULL only when every argument is NULL, on every dialect.

#### Scenario: NULL in any position

- **WHEN** a dimension is `greatest(a, b, c)` over rows `(NULL, 2, 1)`, `(3, NULL, 1)`, `(1, 2, NULL)` and `(NULL, NULL, NULL)`
- **THEN** they yield `2`, `3`, `2` and NULL on every Tier-1 dialect

### Requirement: Functions outside the library are rejected

`initcap`, `random`, format-string parsers (`to_date(x, fmt)`, `strftime`), JSON builders and JSON table functions SHALL NOT be Mode-B functions; a call SHALL fail with the existing unknown-function error.

#### Scenario: initcap

- **WHEN** a dimension is `initcap(name)`
- **THEN** the query fails as an unknown function
