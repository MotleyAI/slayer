## MODIFIED Requirements

### Requirement: One pattern meaning on every dialect

On every dialect that runs the regex functions — every Tier-1 dialect, and Presto / Athena: `.` SHALL match any character, newline included; `^` and `$` SHALL match only at the start and end of the whole string (never before a trailing newline); `\d`, `\w` and `\s` SHALL be the ASCII classes `[0-9]`, `[A-Za-z0-9_]` and `[ \t\n\r\f\v]`, inside bracket expressions too; `\n`, `\t` and `\r` SHALL be newline, tab and carriage return; the `'i'` flag SHALL fold at least ASCII letters, non-ASCII folding being the database's own.

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

#### Scenario: Presto spells the same pattern

- **WHEN** the dot-matches-newline and true-end filters are rendered for Presto
- **THEN** the patterns carry the same flags and end anchor as Trino's, so they mean the same there
