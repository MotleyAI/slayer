## MODIFIED Requirements

### Requirement: Single-string in tests period membership

`x in '<time point>'` SHALL mean `x = P` and `x not in '<time point>'` SHALL mean `x != P`. An `in` / `not in` whose right-hand side is a tuple or list SHALL keep its value-list meaning, even when its single element is a period literal. A parenthesised single literal (`x in ('v')`, `x in (5)`) SHALL read exactly as the one-element tuple `(v,)`, except a string against a DATE or TIMESTAMP operand, which SHALL keep period membership; a bare `x in '<string>'` against a non-temporal operand stays refused.

#### Scenario: Period membership

- **WHEN** filters are `ordered_at in '2025-Q1'` and `ordered_at not in 'this month'`
- **THEN** they match the rows inside, respectively outside, the period

#### Scenario: Tuple keeps list meaning

- **WHEN** a filter on a TEXT column is `code in ('2025-Q1',)`
- **THEN** it matches rows whose value equals the string `2025-Q1`

#### Scenario: A parenthesised single string against a TEXT column is a value list

- **WHEN** filters are `state in ('paid')` and `state not in ('paid')`, alone or inside a measure condition such as `sum(iif(state in ('paid'), amount, 0))`
- **THEN** they match the rows whose value equals, respectively does not equal, the string `paid`, exactly as `('paid',)` would

#### Scenario: A parenthesised single number is a value list

- **WHEN** a filter is `id in (5)`
- **THEN** it matches the rows whose `id` equals 5, exactly as `(5,)` would

#### Scenario: A parenthesised single string against a temporal operand stays a period

- **WHEN** a filter on a TIMESTAMP column is `issued_at in ('2025')`
- **THEN** it matches the rows inside 2025, exactly as `issued_at in '2025'` does

#### Scenario: A bare string against a TEXT column stays refused

- **WHEN** a filter is `state in 'paid'`
- **THEN** it fails with the error that `'paid'` is a time point and `state` is not a DATE or TIMESTAMP operand
