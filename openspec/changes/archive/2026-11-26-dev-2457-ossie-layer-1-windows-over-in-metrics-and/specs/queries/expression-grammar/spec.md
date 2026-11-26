## MODIFIED Requirements

### Requirement: Forms rejected with a hint

The following SHALL fail at parse time with an error naming the form and the remedy: `a | b` (use `||`); `a // b` (use `div(a, b)` or `floor(a / b)`); `TRIM(BOTH | LEADING | TRAILING …)`; a scalar `min(a, b)` / `max(a, b)` with two positional values (use `greatest` / `least`); `TIME '…'` literals; `SIMILAR TO`; `REGEXP` / `RLIKE` operators. A syntax error SHALL report the character position where parsing failed.

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

#### Scenario: OVER is not a rejected form

- **WHEN** a dimension is `ROW_NUMBER() OVER (ORDER BY orders.amount)`
- **THEN** it parses as a window function (`queries/window-functions`), with no hint to use a transform
