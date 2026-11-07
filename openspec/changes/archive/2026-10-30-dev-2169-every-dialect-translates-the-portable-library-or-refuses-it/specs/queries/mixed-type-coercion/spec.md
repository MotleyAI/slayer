## MODIFIED Requirements

### Requirement: TEXT against a number is numeric

When a TEXT operand meets an INT or DOUBLE operand in a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN`, `BETWEEN`, `IS [NOT] DISTINCT FROM`, arithmetic (`+`, `-`, `*`, `/`, `%`) or `greatest` / `least`, the TEXT side SHALL be read as `TRY_CAST(text AS DOUBLE)` with the meaning of `queries/casts` — invalid text yields NULL — and the operation SHALL be numeric on every dialect that translates `TRY_CAST`; on any other dialect the query SHALL fail at plan time as `queries/scalar-functions` requires. Arithmetic with a TEXT operand SHALL type as DOUBLE. On Postgres older than 16, or of unknown version, the conversion SHALL follow `TRY_CAST`'s contract there (a plain cast).

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
