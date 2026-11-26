## MODIFIED Requirements

### Requirement: Forms rejected with a hint

The following SHALL fail at parse time with an error naming the form and the remedy: `a | b` (use `||`); `a // b` (use `div(a, b)` or `floor(a / b)`); `TRIM(BOTH | LEADING | TRAILING …)`; a scalar `min(a, b)` / `max(a, b)` with two positional values (use `greatest` / `least`); `SIMILAR TO`; `REGEXP` / `RLIKE` operators; raw `OVER (…)`. A syntax error SHALL report the character position where parsing failed. A `TIME '…'` literal SHALL parse (`queries/time-of-day`).

#### Scenario: Bitwise-or spelling

- **WHEN** a measure is `min(status | region)`
- **THEN** it fails naming `|` and suggesting `||`

#### Scenario: Floor-division spelling

- **WHEN** a measure is `sum(amount) // 2`
- **THEN** it fails suggesting `div(a, b)` or `floor(a / b)`

#### Scenario: Two-argument scalar max

- **WHEN** a dimension is `max(amount, 10)`
- **THEN** it fails suggesting `greatest(amount, 10)`

#### Scenario: Syntax error position

- **WHEN** a filter is `amount > > 5`
- **THEN** the error names the position of the second `>`

#### Scenario: TIME literal parses

- **WHEN** a filter is `opened_at > TIME '10:00'`
- **THEN** it parses
