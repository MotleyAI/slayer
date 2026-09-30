## MODIFIED Requirements

### Requirement: ISO literals are typed date values in date positions
A string literal in a temporal operand position — directly, or as a value argument of a
`coalesce`, `ifnull`, `nullif`, `greatest`, `least` or `iif` / `CASE` in such a position — SHALL
bind as a date value: `'YYYY-MM-DD'` as a DATE and an instant in the `queries/time-points` grammar
(`YYYY-MM-DD HH:MM[:SS[.fraction]]`, space or `T` separator) as a TIMESTAMP. A string in such a
position that is not one of those shapes or not a real calendar value SHALL be rejected with a typed
query error. `{variable}` placeholders SHALL work in these positions. Outside date positions a string
literal keeps its string meaning, except a time point compared with a temporal operand
(`queries/time-points`).

#### Scenario: Literal anchor
- **WHEN** a filter uses `date_diff('day', '2024-01-01', created_at) < 30`
- **THEN** it counts orders created in the first 30 calendar days of 2024 (and earlier)

#### Scenario: Literal inside coalesce
- **WHEN** a measure uses `min(date_part('year', coalesce(shipped_at, '2024-01-01')))`
- **THEN** it binds, and unshipped orders contribute 2024

#### Scenario: Invalid calendar value
- **WHEN** a filter uses `date_diff('day', '2024-02-30', created_at) > 0`
- **THEN** the query is rejected with a type error naming the literal

#### Scenario: Placeholder in a date position
- **WHEN** a filter `date_diff('day', '{launch}', created_at) >= 0` runs with variables `{"launch": "2024-05-01"}`
- **THEN** it counts orders created on or after 2024-05-01

#### Scenario: Minute-precision instant
- **WHEN** a dimension uses `date_add('2024-01-31 10:15', 1, 'month')`
- **THEN** it is the TIMESTAMP `2024-02-29 10:15:00`
