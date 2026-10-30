# queries/string-matching Specification

## Purpose
Defines how Mode-B compares and pattern-matches strings: which comparisons follow the column's collation, which are guaranteed case-insensitive, and one LIKE pattern language on every Tier-1 dialect.

## Requirements

### Requirement: Comparisons follow the column's collation

`=`, `<>`, `<`, `<=`, `>`, `>=`, `IN` and `LIKE` over TEXT SHALL follow the database's collation for the operands, except on SQLite, where Mode-B `LIKE` SHALL be case-sensitive whatever the column's declared collation. `ILIKE` and the regex `'i'` flag SHALL be the case-insensitive forms on every dialect. Mode-A SQL (`Column.sql`, model `filters`) SHALL keep the database's own meaning.

#### Scenario: SQLite LIKE is case-sensitive

- **WHEN** a filter is `name LIKE 'ada%'` on SQLite over rows holding `'ada'` and `'Ada'`
- **THEN** it keeps `'ada'` only

#### Scenario: ILIKE is case-insensitive everywhere

- **WHEN** a filter is `name ILIKE 'ADA%'` over rows holding `'ada'` and `'Ada'`
- **THEN** it keeps both, on every Tier-1 dialect

#### Scenario: Mode-A LIKE on SQLite unchanged

- **WHEN** a model column has `sql` `name LIKE 'ada%'` on SQLite and a row holds `'Ada'`
- **THEN** the column is true for that row, as SQLite's own `LIKE` is

### Requirement: One LIKE pattern language

In Mode-B `LIKE` and `ILIKE` patterns, `%` SHALL match any sequence of characters (newlines included) and `_` exactly one character; a backslash SHALL make the following `%`, `_` or `\` literal and SHALL be literal before any other character; every other character, `[` included, SHALL be literal — on every Tier-1 dialect, for literal, variable and column-valued patterns.

#### Scenario: Escaped wildcard

- **WHEN** a filter is `code LIKE 'a\_%'` over rows holding `'a_1'` and `'ab1'`
- **THEN** it keeps `'a_1'` only, on every Tier-1 dialect

#### Scenario: Bracket is literal

- **WHEN** a filter is `code LIKE '[ab]%'` over rows holding `'[ab]1'` and `'a1'`
- **THEN** it keeps `'[ab]1'` only, on SQL Server as everywhere

#### Scenario: Lone backslash is literal

- **WHEN** a filter is `path LIKE 'c:\x%'` over a row holding `'c:\x1'`
- **THEN** it keeps the row on every Tier-1 dialect

#### Scenario: Column-valued pattern

- **WHEN** a filter is `name LIKE prefix || '%'` over a row where `prefix` is `'a_'` and `name` is `'a_b'`, and over a row where `name` is `'axb'`
- **THEN** both rows match, `_` in the column value being a wildcard

#### Scenario: GLOB metacharacters on SQLite

- **WHEN** a filter is `code LIKE 'a*?%'` on SQLite over rows holding `'a*?1'` and `'abc1'`
- **THEN** it keeps `'a*?1'` only
