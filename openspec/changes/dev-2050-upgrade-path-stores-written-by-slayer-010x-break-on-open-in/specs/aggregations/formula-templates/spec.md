## REMOVED Requirements

### Requirement: Placeholders are recognised as whole SQL tokens

**Reason**: That requirement made brace text inside a string literal inert. This silently changed the results of formulas such as `SUM({value}) * '{n}'`, which substituted in 0.10.x. Placeholders inside ordinary string literals are now recognised; see "Placeholders are recognised as SQL tokens and inside string literals".
**Migration**: No stored-document change. A formula that relied on `'{name}'` being emitted verbatim writes `'{{name}}'` instead.

## ADDED Requirements

### Requirement: Placeholders are recognised as SQL tokens and inside string literals

A placeholder SHALL be one of:
- a `{` token, one identifier-shaped token (keyword names such as `{order}` included; surrounding whitespace ignored), and a `}` token of the formula's SQL;
- a `{name}` occurrence, with `name` identifier-shaped, inside an ordinary single-quoted string literal of the formula.

Inside an ordinary string literal, `{{` and `}}` SHALL denote one literal brace each, and SHALL NOT start or end a placeholder. A placeholder inside a string literal SHALL count as a read of its parameter wherever reads are counted, including the save-time "parameter never referenced" check.

Brace text inside a quoted identifier or a comment SHALL NOT be a placeholder and SHALL be emitted unchanged. A placeholder inside any other string-literal kind (national, escape, raw, byte, or dollar-quoted strings) SHALL be rejected with a typed template error naming the placeholder and the formula. Brace syntax that is not a placeholder (e.g. a DuckDB struct literal `{'a': 1}`) SHALL be left to the SQL grammar.

#### Scenario: Whitespace inside braces is tolerated

- **WHEN** a custom aggregation formula is `SUM({ value })`
- **THEN** it renders identically to `SUM({value})`

#### Scenario: A quoted placeholder counts as a read

- **WHEN** a model's aggregation has formula `SUM({value}) * '{n}'` and parameter `n` with default `2`
- **THEN** the model saves without a "never referenced" error
- **AND** the formula's placeholder names include `n` on every supported dialect

#### Scenario: Doubled braces inside a literal are literal braces

- **WHEN** a custom aggregation formula is `MAX(CASE WHEN {value} > 0 THEN '{{n}}' END)` and a query aggregates `amount` with it
- **THEN** the emitted string literal's value is `{n}`

#### Scenario: Braces in a quoted identifier or a comment are inert

- **WHEN** a custom aggregation formula contains `"{n}"` as a quoted identifier, or `{n}` inside a SQL comment
- **THEN** that text is emitted unchanged and is not a read of `n`

#### Scenario: A placeholder in a non-ordinary string literal is rejected

- **WHEN** a custom aggregation formula contains `N'{n}'`, or, on Postgres, `E'{n}'` or a dollar-quoted string containing `{n}`
- **THEN** saving or rendering the formula fails with a typed template error naming `n` and the formula

### Requirement: A quoted placeholder renders its binding's literal value

A placeholder inside an ordinary string literal SHALL be bound exactly as an unquoted placeholder is: a parameter default or a query-time argument is SQL text, and a string argument resolves like its unquoted spelling. The binding MUST be a literal: a number, a string literal, or a negated number.

The rendered string literal SHALL carry the original literal's text, with each placeholder replaced by the binding's value (`2` gives `2`, and `'x'` gives `x`) and each doubled brace collapsed to one brace. The literal SHALL be escaped for the target dialect, so that no character of the value can end the literal.

A non-literal binding SHALL be rejected with a typed template error naming the placeholder and the formula: a column, an expression, or `{value}`, which is always the aggregated column. For a parameter default the error SHALL occur when the model is saved; for a query-time argument, when the query is bound. Unquoted placeholders SHALL keep their structural substitution unchanged.

#### Scenario: A quoted numeric default substitutes its value

- **WHEN** an aggregation `sum_n` has formula `SUM({value}) * '{n}'` with `n` defaulting to `2`, and a SQLite query aggregates `amount` with it
- **THEN** the result equals twice the sum of `amount`, as it did in SLayer 0.10.2

#### Scenario: A quoted numeric default inside a cast substitutes its value

- **WHEN** an aggregation has formula `SUM({value}) * CAST('{n}' AS DOUBLE)` with `n` defaulting to `2`, and a DuckDB query aggregates `amount` with it
- **THEN** the result equals twice the sum of `amount`, as it did in SLayer 0.10.2

#### Scenario: A quoted string argument cannot escape the literal

- **WHEN** a query passes `n="'a''b\c'"` (a SQL string literal whose value is `a'b\c`) to an aggregation whose formula reads `'{n}'`
- **THEN** the emitted literal's value is exactly `a'b\c`, quoted correctly on quote-doubling dialects (DuckDB, Postgres), backslash-escaping dialects (MySQL, ClickHouse) and BigQuery

#### Scenario: The aggregated value inside quotes is rejected

- **WHEN** a model is saved whose aggregation formula contains `'{value}'`
- **THEN** the save fails with a typed template error naming `value` and the formula

#### Scenario: A column default inside quotes is rejected at save

- **WHEN** a model is saved whose aggregation formula contains `'{w}'` and whose parameter `w` defaults to a column name
- **THEN** the save fails with a typed template error naming `w` and the formula, before any query runs

#### Scenario: A column argument inside quotes is rejected at binding

- **WHEN** a query passes a column reference for a parameter that the formula reads only as `'{n}'`
- **THEN** the query fails with a typed template error naming `n`, and no SQL is executed
