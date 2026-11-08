## MODIFIED Requirements

### Requirement: Every Mode-B formula surface substitutes variables

`{var}` placeholders in a query's filters, measure formulas, computed-dimension expressions (string and object form) and order expressions SHALL be bound from the query's effective variables as literal values, never as text spliced into the expression. A placeholder inside a string literal, wholly or partly (`'{status}'`, `'%{q}%'`), SHALL contribute its value's text to that literal; a placeholder in a bare literal position SHALL take a number or boolean value, a numeric string reading as its number; a placeholder forming the whole parenthesised list of `IN` SHALL take a list value. `{{` and `}}` inside a string literal SHALL render literal braces, and a brace pair inside a string literal whose contents are only digits, commas and spaces (`'[0-9]{3}'`, `'{1, 2, 3}'`, `'{}'`) SHALL be literal text, not a placeholder. A query carrying placeholders on these surfaces SHALL construct successfully and SHALL execute with the bound values.

#### Scenario: Measure formula

- **WHEN** a query on `orders` has measure `sum(amount) * {k} / 100` and `variables={"k": 10}`
- **THEN** it executes and returns the total amount multiplied by 10 and divided by 100, by executed values on SQLite and DuckDB

#### Scenario: Computed dimension in string and object form

- **WHEN** a query has dimension `"amount * {k}"`, or dimension `{"expression": "amount * {k}"}`, with `variables={"k": 10}`
- **THEN** both construct and group by `amount * 10`

#### Scenario: Order expression in colon and call form

- **WHEN** a query orders by `sum(amount) * {k}` descending, with `variables={"k": -1}`
- **THEN** both construct and the rows come back in ascending total-amount order

#### Scenario: Placeholder in operand positions

- **WHEN** a placeholder appears as an aggregation argument (`sum({k})`), a transform argument, a scalar-function argument, a CASE branch, or the whole right-hand side of `in` (`region in ({regions})` with a list value)
- **THEN** the query constructs and executes with the substituted value

#### Scenario: Quoted string value is escaped

- **WHEN** a measure contains `'{name}'` inside a quoted literal and `variables={"name": "O'Brien"}`
- **THEN** the literal's value is `O'Brien` and the query executes without a parse error

#### Scenario: Digit-and-comma braces in a string literal

- **WHEN** a filter is `regexp_like(code, '^[a-z]{1,2}[0-9]{2,}$')`, `status = '{1, 2, 3}'` or `status = '{}'`, with or without variables
- **THEN** the braces stay in the literal and no variable is looked up
