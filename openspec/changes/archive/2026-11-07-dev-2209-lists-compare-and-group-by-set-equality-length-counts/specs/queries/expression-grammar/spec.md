## ADDED Requirements

### Requirement: An IN list may hold list operands

The right-hand side of `IN` / `NOT IN` SHALL accept a parenthesised list whose items are all array literals or placeholders, besides one whose items are all scalar literals or placeholders; a list mixing array literals with scalar literals SHALL fail to parse with an error naming the right-hand side. When the left side is a list, each item SHALL be one list operand: a list variable `{wanted}` is one list, so `L IN ({wanted})` equals `L = {wanted}`, and an empty list there binds as `[]`. When the left side is a scalar, a list variable SHALL keep filling the whole right-hand side.

#### Scenario: Several array literals
- **WHEN** a filter is `skills IN (['go'], ['sql', 'go'])`
- **THEN** it parses and binds as `skills = ['go'] OR skills = ['sql', 'go']`

#### Scenario: Mixed items
- **WHEN** a filter is `skills IN (['go'], 'sql')`
- **THEN** it fails to parse with an error naming the right-hand side of `IN`

#### Scenario: A list variable on a list's right-hand side
- **WHEN** a filter is `skills IN ({wanted})` with `variables={"wanted": ["sql", "go"]}`, and another is `region IN ({regions})` with `variables={"regions": ["EU", "US"]}`
- **THEN** the first keeps the rows whose `skills` element set is `['go', 'sql']`, and the second keeps the rows whose `region` is `EU` or `US`
