## Purpose

Defines Mode-B regular expressions: the matching, extraction and replacement functions, their operator spellings, and one portable pattern language that means the same on every Tier-1 dialect.

## ADDED Requirements

### Requirement: Regex functions

Mode B SHALL provide:

- `regexp_like(x, p[, 'i'])` — BOOLEAN, true when `p` matches anywhere in `x`.
- `regexp_extract(x, p[, group])` — TEXT: the first match, or that capture group of the first match; NULL when nothing matches or the group did not participate. `group` SHALL be an integer literal from 0 (the whole match) to the pattern's group count.
- `regexp_replace(x, p, replacement[, 'i'])` — TEXT with every non-overlapping match replaced.

The only flag SHALL be the literal `'i'` (case-insensitive); without it matching SHALL be case-sensitive on every dialect, whatever the column's collation. A NULL `x` SHALL yield NULL.

#### Scenario: Partial match

- **WHEN** a filter is `regexp_like(code, '[0-9]{3}')` over rows holding `'ab123'` and `'ab12'`
- **THEN** it keeps `'ab123'` only, on every Tier-1 dialect

#### Scenario: Case-sensitive by default

- **WHEN** a filter is `regexp_like(status, '^paid$')` over rows holding `'paid'` and `'PAID'` on MySQL with a case-insensitive collation
- **THEN** it keeps `'paid'` only; with the `'i'` flag it keeps both

#### Scenario: Extract a group, NULL on no match

- **WHEN** a dimension is `regexp_extract(email, '@(.+)$', 1)` over rows holding `'a@x.org'` and `'nobody'`
- **THEN** they yield `'x.org'` and NULL on every Tier-1 dialect

#### Scenario: Group beyond the pattern

- **WHEN** a dimension is `regexp_extract(email, '@(.+)$', 2)`
- **THEN** the query fails at plan time with a typed error

#### Scenario: Replace every match

- **WHEN** a dimension is `regexp_replace(phone, '[^0-9]', '')` over a row holding `'+1 (555) 01'`
- **THEN** it yields `'155501'`

### Requirement: Regex operators

`x ~ p`, `x ~* p`, `x !~ p` and `x !~* p` SHALL mean `regexp_like(x, p)`, `regexp_like(x, p, 'i')`, `NOT regexp_like(x, p)` and `NOT regexp_like(x, p, 'i')`, binding like the comparison operators.

#### Scenario: Operator spelling

- **WHEN** a filter is `name ~* '^ad'`
- **THEN** it keeps the same rows as `regexp_like(name, '^ad', 'i')`

### Requirement: Patterns are literals or variables

A pattern SHALL be a string literal or a `{var}` placeholder whose value is the pattern text. Any other pattern expression (a column, a concatenation) SHALL be rejected at plan time.

#### Scenario: Column-valued pattern

- **WHEN** a filter is `regexp_like(name, pattern_col)`
- **THEN** the query fails at plan time with a typed error

#### Scenario: Variable pattern

- **WHEN** a filter is `regexp_like(name, {p})` with variable `p` = `'^a.c$'`
- **THEN** it keeps the rows matching `^a.c$`

### Requirement: The portable pattern subset

A pattern SHALL be validated at plan time against the portable subset: literal characters; `.`; bracket expressions `[…]` and `[^…]` with ranges; the quantifiers `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`; the anchors `^` and `$`; alternation `|`; groups `( … )`; the classes `\d`, `\w`, `\s` and their negations `\D`, `\W`, `\S`; the escapes `\n`, `\t`, `\r`; and a backslash before any metacharacter. Anything else — backreferences, lookaround, lazy quantifiers, `(?…)` groups, `\b`, `\p{…}`, POSIX classes such as `[[:alpha:]]` — SHALL be rejected with a typed error naming the construct; for a variable pattern the error SHALL also name the variable.

#### Scenario: Unsupported construct

- **WHEN** a filter is `regexp_like(name, '(?=a)b')`
- **THEN** the query fails at plan time naming the lookahead construct

#### Scenario: Unsupported construct in a variable

- **WHEN** a filter is `regexp_like(name, {p})` with `p` = `'\bword'`
- **THEN** the query fails at plan time naming `\b` and the variable `p`

### Requirement: One pattern meaning on every dialect

On every Tier-1 dialect: `.` SHALL match any character, newline included; `^` and `$` SHALL match only at the start and end of the whole string (never before a trailing newline); `\d`, `\w` and `\s` SHALL be the ASCII classes `[0-9]`, `[A-Za-z0-9_]` and `[ \t\n\r\f\v]`, inside bracket expressions too; `\n`, `\t` and `\r` SHALL be newline, tab and carriage return; the `'i'` flag SHALL fold at least ASCII letters, non-ASCII folding being the database's own.

#### Scenario: Dot matches newline

- **WHEN** a filter is `regexp_like(notes, 'paid.*late')` over a row holding `'paid` newline `late'`
- **THEN** it keeps the row on every Tier-1 dialect

#### Scenario: Dollar is the true end

- **WHEN** a filter is `regexp_like(code, '^ab$')` over a row holding `'ab'` followed by a newline
- **THEN** it drops the row on every Tier-1 dialect

#### Scenario: ASCII digit class

- **WHEN** a filter is `regexp_like(code, '^\d+$')` over rows holding `'123'` and `'١٢٣'` (Arabic-Indic digits)
- **THEN** it keeps `'123'` only, on every Tier-1 dialect

#### Scenario: Class inside brackets

- **WHEN** a filter is `regexp_like(code, '^[\d_]+$')` over a row holding `'1_2'`
- **THEN** it keeps the row on every Tier-1 dialect

#### Scenario: Newline escape

- **WHEN** a dimension is `regexp_extract(notes, '^[^\n]*')` over a row holding `'line1` newline `line2'`
- **THEN** it yields `'line1'`

### Requirement: Replacement strings

In a `regexp_replace` replacement, `\1` to `\9` SHALL insert that capture group (empty when it did not participate) and `\\` SHALL insert one backslash; every other character, `$` and `&` included, SHALL be literal on every dialect. Any other backslash sequence, or a group number beyond the pattern's groups, SHALL be rejected at plan time. The replacement SHALL be a string literal or a `{var}` placeholder.

#### Scenario: Group reference

- **WHEN** a dimension is `regexp_replace(name, '(\w+) (\w+)', '\2, \1')` over a row holding `'Ada Lovelace'`
- **THEN** it yields `'Lovelace, Ada'` on every Tier-1 dialect

#### Scenario: Literal dollar and ampersand

- **WHEN** a dimension is `regexp_replace(price, 'USD', '$ & co')` over a row holding `'5 USD'`
- **THEN** it yields `'5 $ & co'` on every Tier-1 dialect

#### Scenario: Unknown escape

- **WHEN** a dimension is `regexp_replace(name, 'a', '\q')`
- **THEN** the query fails at plan time with a typed error

### Requirement: SQL Server needs 2025

Regex functions and operators SHALL follow the version-gating rule of `queries/scalar-functions` with SQL Server 2025 as the minimum, at every database compatibility level.

#### Scenario: SQL Server 2022

- **WHEN** a filter is `regexp_like(code, '^a')` against a SQL Server reporting version 2022
- **THEN** the query fails at plan time naming SQL Server 2025
