## MODIFIED Requirements

### Requirement: Every Mode-B formula surface substitutes variables

`{var}` placeholders in a query's filters, measure formulas, computed-dimension expressions (string and object form) and order expressions SHALL be bound from the query's effective variables as literal values, never as text spliced into the expression. A placeholder inside a string literal, wholly or partly (`'{status}'`, `'%{q}%'`), SHALL contribute its value's text to that literal; a placeholder in a bare literal position SHALL take a number or boolean value, a numeric string reading as its number; a placeholder forming the whole parenthesised list of `IN` SHALL take a list value. `{{` and `}}` inside a string literal SHALL render literal braces. A query carrying placeholders on these surfaces SHALL construct successfully and SHALL execute with the bound values.

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

### Requirement: Saved measure formulas substitute on the models whose SQL surfaces substitute

A saved `ModelMeasure.formula` SHALL bind `{var}` placeholders as literal values, by the same rules as query formula surfaces, on exactly the models whose Mode-A surfaces substitute (the query's direct source model and each stage's own source model), using that model's effective variables. Its placeholders SHALL be reported by model variable inspection as required unless the model defaults them. A saved measure reached through a join or a cross-model reference SHALL NOT substitute, and a placeholder in it SHALL raise the unresolved-placeholder error.

#### Scenario: Saved measure on the source model

- **WHEN** model `orders` has saved measure `amt_scaled` with formula `sum(amount) * {k}` and a query requests `amt_scaled` with `variables={"k": 10}`
- **THEN** it returns ten times the total amount

#### Scenario: Saved measure through a join

- **WHEN** model `customers`, joined from `orders`, has saved measure `spend_scaled` with formula `sum(spend) * {k}` and a query on `orders` requests `customers.spend_scaled` with `variables={"k": 10}`
- **THEN** the query fails with the unresolved-placeholder error naming `{k}`

#### Scenario: Inspection lists saved-measure variables

- **WHEN** model `orders` has saved measure formula `sum(amount) * {k}` and no default for `k`
- **THEN** model variable inspection reports `k` as required

## ADDED Requirements

### Requirement: Variables supply literal values only

A placeholder in any other position — an identifier or path segment (`{col}:sum`, `t.{c}`), a function name (`{f}(x)`), a keyword argument's name, or a value that must be a reference (`partition_by={x}`) — SHALL be rejected with an error stating that variables supply literal values. A string value bound into a bare literal position SHALL be rejected with the same error plus a hint to put the placeholder inside quotes. No variable value SHALL ever change the structure of the expression it is bound into.

#### Scenario: Identifier injection rejected

- **WHEN** a query on `orders` has measure `sum({col})` with `variables={"col": "amount"}`
- **THEN** it fails with the literal-values error and the hint to quote the placeholder

#### Scenario: Function and path positions rejected

- **WHEN** a measure is `{f}(amount)`, or a dimension is `customers.{c}`, or a measure is `sum(amount, partition_by={x})`
- **THEN** each fails with the literal-values error

#### Scenario: Numeric string in a bare position

- **WHEN** a filter is `amount > {min}` with `variables={"min": "10"}`
- **THEN** it keeps the rows with amount above 10

#### Scenario: Hostile string stays a literal

- **WHEN** a filter is `status = '{s}'` with `variables={"s": "x' OR '1' = '1"}`
- **THEN** it keeps only the rows whose status equals that whole string — none, on data without it
