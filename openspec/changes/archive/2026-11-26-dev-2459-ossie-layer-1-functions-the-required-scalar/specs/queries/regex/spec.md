## MODIFIED Requirements

### Requirement: Regex functions

Mode B SHALL provide:

- `regexp_like(x, p[, 'i'])` — BOOLEAN, true when `p` matches anywhere in `x`.
- `regexp_extract(x, p[, group])` — TEXT: the first match, or that capture group of the first match; NULL when nothing matches; `''` when the pattern matches but the group did not take part (as in a replacement), on every dialect. `group` SHALL be an integer literal from 0 (the whole match) to the pattern's group count.
- `regexp_replace(x, p, replacement[, 'i'])` — TEXT with every non-overlapping match replaced.
- `regexp_count(x, p[, 'i'])` — INT: the number of non-overlapping matches of `p` in `x`, scanning left to right; 0 when nothing matches. A pattern that can match the empty string SHALL count as PostgreSQL counts it; a dialect that cannot reproduce that count SHALL reject such a pattern at plan time with a typed error naming `regexp_count`.

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

#### Scenario: Group that did not take part

- **WHEN** a dimension is `regexp_extract(code, '(x)?b', 1)` over rows holding `'ab'` and `'zz'`
- **THEN** they yield `''` and NULL on every Tier-1 dialect

#### Scenario: Group beyond the pattern

- **WHEN** a dimension is `regexp_extract(email, '@(.+)$', 2)`
- **THEN** the query fails at plan time with a typed error

#### Scenario: Replace every match

- **WHEN** a dimension is `regexp_replace(phone, '[^0-9]', '')` over a row holding `'+1 (555) 01'`
- **THEN** it yields `'155501'`

#### Scenario: Count matches

- **WHEN** dimensions are `regexp_count(status, 'n')`, `REGEXP_COUNT(status, 'N')`, `regexp_count(status, 'N', 'i')`, `regexp_count(status, 'aa')` and `regexp_count(status, 'z')` over rows holding `'pending'` and `'aaaa'`, and over a row holding NULL
- **THEN** `'pending'` yields `2`, `0`, `2`, `0` and `0`, `'aaaa'` yields `0`, `0`, `0`, `2` and `0`, and NULL yields NULL for each, on every Tier-1 dialect that translates `regexp_count`

#### Scenario: Pattern matching the empty string

- **WHEN** a dimension is `regexp_count(code, 'x*')` over a row holding `'abc'`
- **THEN** it yields `4`, as on PostgreSQL, or the query fails at plan time with a typed error naming `regexp_count` on a dialect that cannot reproduce that count
