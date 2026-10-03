## Purpose

Governs `{var}` placeholder substitution across query and model surfaces: which surfaces substitute, under which escaping regime, how result names stay independent of variable values, and the errors raised when substitution cannot apply.

## ADDED Requirements

### Requirement: Every Mode-B formula surface substitutes variables

`{var}` placeholders SHALL be substituted from the query's effective variables in a query's filters, measure formulas, computed-dimension expressions (string and object form), and order expressions, using the Mode-B escaping regime: a string value is escaped so it stays inside the quoted literal the author wrote, a number or boolean is inserted verbatim, a list renders a Mode-B tuple, and `{{` / `}}` render literal braces. A query carrying placeholders on these surfaces SHALL construct successfully and SHALL execute with the substituted values.

#### Scenario: Measure formula

- **WHEN** a query on `orders` has measure `amount:sum * {k} / 100` and `variables={"k": 10}`
- **THEN** it executes and returns the total amount multiplied by 10 and divided by 100, by executed values on SQLite and DuckDB

#### Scenario: Computed dimension in string and object form

- **WHEN** a query has dimension `"amount * {k}"`, or dimension `{"expression": "amount * {k}"}`, with `variables={"k": 10}`
- **THEN** both construct and group by `amount * 10`

#### Scenario: Order expression in colon and call form

- **WHEN** a query orders by `amount:sum * {k}` or by `sum(amount) * {k}` descending, with `variables={"k": -1}`
- **THEN** both construct and the rows come back in ascending total-amount order

#### Scenario: Placeholder in operand positions

- **WHEN** a placeholder appears as an aggregation argument (`sum({k})`), a transform argument, a scalar-function argument, a CASE branch, or the whole right-hand side of `in` (`region in ({regions})` with a list value)
- **THEN** the query constructs and executes with the substituted value

#### Scenario: Quoted string value is escaped

- **WHEN** a measure contains `'{name}'` inside a quoted literal and `variables={"name": "O'Brien"}`
- **THEN** the value stays inside the literal and the query executes without a parse error

### Requirement: Saved measure formulas substitute on the models whose SQL surfaces substitute

A saved `ModelMeasure.formula` SHALL substitute `{var}` under the Mode-B regime on exactly the models whose Mode-A surfaces substitute (the query's direct source model and each stage's own source model), using that model's effective variables. Its placeholders SHALL be reported by model variable inspection as required unless the model defaults them. A saved measure reached through a join or a cross-model reference SHALL NOT substitute, and a placeholder in it SHALL raise the unresolved-placeholder error.

#### Scenario: Saved measure on the source model

- **WHEN** model `orders` has saved measure `amt_scaled` with formula `amount:sum * {k}` and a query requests `amt_scaled` with `variables={"k": 10}`
- **THEN** it returns ten times the total amount

#### Scenario: Saved measure through a join

- **WHEN** model `customers`, joined from `orders`, has saved measure `spend_scaled` with formula `spend:sum * {k}` and a query on `orders` requests `customers.spend_scaled` with `variables={"k": 10}`
- **THEN** the query fails with the unresolved-placeholder error naming `{k}`

#### Scenario: Inspection lists saved-measure variables

- **WHEN** model `orders` has saved measure formula `amount:sum * {k}` and no default for `k`
- **THEN** model variable inspection reports `k` as required

### Requirement: Time-dimension date ranges substitute variables; granularities and columns do not

A time dimension's `date_range` bounds SHALL substitute `{var}` and SHALL then be validated as time points exactly like literal bounds. A placeholder in a time dimension's granularity or column SHALL be rejected with an error stating that variables supply literal values and cannot name a granularity or a column.

#### Scenario: Variable date range

- **WHEN** a time dimension has `date_range: ["{start}", "{end}"]` with `variables={"start": "2024-01-01", "end": "2024-02-28"}`
- **THEN** rows are restricted to that range, by executed values on SQLite and DuckDB

#### Scenario: Substituted bound is malformed

- **WHEN** a time dimension has `date_range: ["{start}", null]` with `variables={"start": "2025/01/01"}`
- **THEN** execution fails with the typed date-range error listing the accepted time-point forms

#### Scenario: Granularity or column placeholder

- **WHEN** a time dimension has granularity `{g}`, or is written `"{g}(ordered_at)"`, or names column `{col}`
- **THEN** the query is rejected with the error stating variables cannot name a granularity or a column

### Requirement: Result names come from the template

An unnamed measure or computed dimension whose text contains placeholders SHALL take its result name from the pre-substitution template under the existing derivation convention (the canonical formula text for measures, the expression text for computed dimensions), so its result key never depends on variable values. Such an entry SHALL keep every other unnamed-entry behaviour: equal unnamed entries merge into one column, and a derived-key collision between different values fails loudly. Filters and order items SHALL resolve the entry by its template-derived name and by its substituted formula text.

#### Scenario: Template-derived measure key

- **WHEN** an unnamed measure `amount:sum * {k} / 100` is queried on `orders` with `variables={"k": 10}`
- **THEN** its result key is `orders.amount_sum_k_100`

#### Scenario: Spelling-insensitive template key

- **WHEN** the unnamed measure is written `sum(amount) * {k} / 100`
- **THEN** its result key is the same `orders.amount_sum_k_100`

#### Scenario: Equal unnamed template measures merge

- **WHEN** a query lists the unnamed measure `amount:sum * {k}` twice
- **THEN** the result has one column for it

#### Scenario: References by template name

- **WHEN** a query has unnamed measure `amount:sum * {k}`, filter `amount_sum_k > 100`, and order by `amount_sum_k`
- **THEN** both resolve to that measure

#### Scenario: Stable columns of a saved query-backed model

- **WHEN** a query-backed model is saved from a query whose unnamed measure is `amount:sum * {k}` and it is queried with `k=10` and with `k=20`
- **THEN** both runs return the same column names

### Requirement: Substitution errors on formula surfaces match filters

On every formula surface, an undefined variable SHALL raise the same error as in filters; an invalid variable name SHALL raise the same error as in filters; and an optional block `{? ... ?}` SHALL be rejected with an error stating optional blocks are supported only on raw-SQL model surfaces.

#### Scenario: Undefined variable

- **WHEN** a measure contains `{k}` and no layer supplies `k`
- **THEN** execution fails with the undefined-variable error naming `k`

#### Scenario: Optional block on a formula surface

- **WHEN** a measure, computed dimension, or order expression contains `{? ... ?}`
- **THEN** it is rejected with the optional-block error

### Requirement: Saving a query-backed model requires every variable

Saving a query-backed model SHALL render it exactly as execution with no runtime variables would. A placeholder on any substituted surface with no value from the model's variables, a stage's variables or a source model's defaults SHALL refuse the save with the undefined-variable error naming the variable. A model that is not query-backed SHALL save regardless of undefaulted placeholders.

#### Scenario: Undefaulted variable refuses the save

- **WHEN** a query-backed model is saved from a query with measure `amount:sum * {k}`, or with `date_range: ["{start}", null]`, and no default for that variable
- **THEN** the save fails with the undefined-variable error naming it

#### Scenario: Defaulted variable saves the exact SQL

- **WHEN** the same model is saved with `variables={"k": 10}`
- **THEN** the save succeeds and its backing SQL equals the SQL execution renders with `k=10`

#### Scenario: Plain model with a saved-measure placeholder

- **WHEN** a model that is not query-backed has saved measure formula `amount:sum * {k}` and no default for `k`
- **THEN** the save succeeds

### Requirement: Unsubstituted placeholders fail with a typed error

A `{name}` placeholder that reaches query binding unsubstituted SHALL raise a typed error stating that it looks like a variable placeholder, naming the expression, and pointing at `variables` and at `{{` / `}}` for literal braces. It SHALL NOT surface as an unsupported-syntax error. A set literal with more than one element SHALL remain unsupported syntax.

#### Scenario: Escaped braces reaching binding

- **WHEN** a filter is `amount > {{k}}`
- **THEN** execution fails with the typed placeholder error naming `{k}`

#### Scenario: Multi-element set

- **WHEN** a measure is `amount:sum * {a, b}`
- **THEN** it fails as unsupported syntax
