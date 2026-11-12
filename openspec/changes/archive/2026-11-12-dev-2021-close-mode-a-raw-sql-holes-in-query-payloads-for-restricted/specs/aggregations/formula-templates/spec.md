## MODIFIED Requirements

### Requirement: Aggregation parameters bind once

Every parameter value an aggregation's rendering reads — a query argument, or the
aggregation definition's default for a parameter the query does not supply — SHALL be
resolved once, when the query is bound, into the value it denotes: a column reference,
a literal, or an expression over column references. A definition default SHALL then be
indistinguishable from the same value supplied explicitly: identical results, identical
behaviour in every producer kind and position, and a single computation when both
appear in one query; only the public auto-generated name follows the query's own
spelling (`wpop(amount)` keeps the name `amount_wpop`). A string argument naming a
formula placeholder SHALL be read as Mode B and resolve exactly as the same text written
unquoted, never as SQL of the datasource's dialect; any other string argument (e.g.
`window='90d'`) is a setting, never SQL. A definition default is Mode A. Parameter text
that cannot be analysed — a default that does not parse in the datasource's dialect, a
string argument that is not valid Mode B (including one calling a function outside the
Mode-B library), or text containing an aggregate, a window function or a subquery — SHALL fail when the query is bound with a
typed error naming the aggregation, the parameter and the text, never emit invalid SQL
or be treated as referencing nothing.

#### Scenario: A default equals its explicit spelling

- **WHEN** a model declares `wsum` (`SUM({value} * {weight})`) with `weight`
  defaulting to `quantity`, and one query selects `wsum(amount)` and
  `wsum(amount, weight=quantity)`
- **THEN** both return identical values from one computation in the generated SQL, and
  the first keeps the public name `amount_wsum`

#### Scenario: An expression default renders in every producer kind

- **WHEN** a default is a free SQL expression such as
  `CASE WHEN quantity > 0 THEN quantity ELSE 0 END` and the aggregation runs locally,
  as a cross-model producer, as an association producer, as a trailing-window producer
  and as the outer aggregation of a second-order producer
- **THEN** each executes on SQLite and DuckDB with hand-computed values equal to the
  expression-source spelling `sum(<value> * CASE WHEN quantity > 0 THEN quantity ELSE 0
  END)` evaluated at the same home, by executed values

#### Scenario: A default used in a filter or an order key

- **WHEN** a measure-typed filter or an order key uses an aggregation whose default is a
  dotted reference to a joined column (`customers.spend`) or a derived column
- **THEN** it evaluates exactly as the same aggregation in measure position, by
  executed values, with the join the default crosses present in the SQL

#### Scenario: A string argument equals its unquoted spelling

- **WHEN** a query passes `w='customers.region_id'` to an aggregation whose formula
  references `{w}`, and another passes `w=customers.region_id`
- **THEN** both resolve the same column from the query root and return identical
  values

#### Scenario: Unparseable parameter text fails at binding

- **WHEN** a query uses an aggregation whose default for a referenced parameter does
  not parse in the datasource's dialect, or passes as a string argument naming a
  placeholder text that is not valid Mode B, such as `'length(sqlite_version())'` or
  `'(SELECT count(*) FROM users)'`
- **THEN** the query fails with a typed error naming the aggregation, the parameter and
  the text, before any SQL is generated

#### Scenario: Aggregate, window or subquery parameter text fails at binding

- **WHEN** an aggregation's default is `SUM(quantity)`, `ROW_NUMBER() OVER (ORDER BY
  id)` or `(SELECT MAX(quantity) FROM orders)`
- **THEN** a query using it fails with the same typed error, naming the parameter

#### Scenario: A default naming a missing column fails at binding

- **WHEN** an aggregation's default names a column that does not exist on the model it
  resolves to
- **THEN** a query using it fails with the same unknown-column error the explicit
  argument spelling raises

#### Scenario: A string expression argument equals its unquoted expression
- **WHEN** one query selects `weighted_avg(amount, weight='id * 2')` and another `weighted_avg(amount, weight=id * 2)`
- **THEN** both return identical values on SQLite and DuckDB

### Requirement: A quoted placeholder renders its binding's literal value

A placeholder inside an ordinary string literal SHALL be bound exactly as an unquoted placeholder is: a parameter default is SQL text, and a query-time argument is Mode B, a string argument resolving like its unquoted spelling. The binding MUST be a literal: a number, a string literal, or a negated number.

The rendered string literal SHALL carry the original literal's text, with each placeholder replaced by the binding's value (`2` gives `2`, and `'x'` gives `x`) and each doubled brace collapsed to one brace. The literal SHALL be escaped for the target dialect, so that no character of the value can end the literal.

A non-literal binding SHALL be rejected with a typed template error naming the placeholder and the formula: a column, an expression, or `{value}`, which is always the aggregated column. For a parameter default the error SHALL occur when the model is saved; for a query-time argument, when the query is bound. Unquoted placeholders SHALL keep their structural substitution unchanged.

#### Scenario: A quoted numeric default substitutes its value

- **WHEN** an aggregation `sum_n` has formula `SUM({value}) * '{n}'` with `n` defaulting to `2`, and a SQLite query aggregates `amount` with it
- **THEN** the result equals twice the sum of `amount`, as it did in SLayer 0.10.2

#### Scenario: A quoted numeric default inside a cast substitutes its value

- **WHEN** an aggregation has formula `SUM({value}) * CAST('{n}' AS DOUBLE)` with `n` defaulting to `2`, and a DuckDB query aggregates `amount` with it
- **THEN** the result equals twice the sum of `amount`, as it did in SLayer 0.10.2

#### Scenario: A quoted string argument cannot escape the literal

- **WHEN** a query passes for `n` the Mode-B string literal `'a''b\c'`, whose value is `a'b\c`, to an aggregation whose formula reads `'{n}'`
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
