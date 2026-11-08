# aggregations/formula-templates Specification

## Purpose
Defines how `{value}` and `{param}` placeholders in an aggregation formula (built-in or
model-defined) are recognised and substituted into the generated SQL, and when a
formula template is rejected.

## Requirements

### Requirement: Substitution preserves operator precedence structurally

Each placeholder occurrence SHALL be replaced by an independent copy of the resolved value
or parameter expression, as a structural node of the formula's SQL. When the substituted
expression is itself an operator expression and it sits as an operand of an operator in
the formula, it SHALL be parenthesised; in function-argument or other self-delimiting
positions it SHALL NOT be wrapped.

#### Scenario: Compound value under multiplication keeps its grouping

- **WHEN** `weighted_avg` aggregates a column whose `sql` is `price - discount` with
  `weight=quantity`
- **THEN** the generated SQL multiplies the whole `(price - discount)` by `quantity`

#### Scenario: Repeated placeholder renders each occurrence

- **WHEN** a formula uses `{weight}` twice (as `weighted_avg` does)
- **THEN** both occurrences render the resolved weight expression

### Requirement: Invalid templates fail closed with a typed error

A formula that does not parse once its placeholders are replaced SHALL be rejected with a
typed error naming the aggregation. A placeholder in a non-expression position (a
qualifier such as `{t}.col`, an alias such as `AS {x}`, or a function name such as
`{fn}(a)`) SHALL be rejected with a typed error. A placeholder with no value — neither a
declared parameter default nor a query-time argument — SHALL be rejected at query time
with a typed error naming the aggregation and the placeholder; no placeholder text SHALL
ever reach the emitted SQL. The parameter names `value` and `window` are reserved:
`{value}` always renders the aggregated column and `window` is the trailing-window
argument, so a declared parameter or formula placeholder named `window` SHALL be
rejected with a typed error naming the aggregation, at model save and — for a model
stored before this rule — when a query uses the aggregation.

#### Scenario: Unparseable formula is rejected

- **WHEN** a query uses a custom aggregation whose formula is `SUM({value}`
- **THEN** the query fails with a typed error naming the aggregation

#### Scenario: Placeholder in qualifier position is rejected

- **WHEN** a custom aggregation formula is `SUM({t}.amount)`
- **THEN** it is rejected with a typed error naming the aggregation

#### Scenario: Unbound placeholder is rejected at query time

- **WHEN** a custom aggregation formula references `{scale}`, `scale` is not a declared
  parameter, and the query supplies no `scale` argument
- **THEN** the query fails with a typed error naming the aggregation and `scale`

#### Scenario: empty aggregation parameter default is rejected

- **WHEN** a model declares an `AggregationParam` whose `sql` is empty
- **THEN** model validation fails naming the parameter

#### Scenario: `value` is reserved for the aggregated source

- **WHEN** a model declares an aggregation parameter named `value`
- **THEN** it is rejected with a typed error naming the aggregation and explaining that
  `{value}` always renders the aggregated column

#### Scenario: unknown aggregation arguments are rejected

- **WHEN** a query passes an argument, by keyword or positionally, that the aggregation does
  not accept — accepted names are the parameters the rendered formula references in the
  query's datasource dialect (or a formula-less built-in's own parameters), and generic
  arguments such as `window` or `partition_by`
- **THEN** it is rejected with a typed unknown-argument error naming the aggregation and the
  argument, and listing the accepted names, before any argument value is resolved
- **AND** for a `value=` argument to an aggregation whose formula uses `{value}`, the error
  explains that `{value}` always renders the aggregated column

#### Scenario: `window` is reserved for the trailing-window argument

- **WHEN** a model declares an aggregation whose formula references `{window}` or that
  declares a parameter named `window`
- **THEN** saving the model is rejected with a typed error naming the aggregation and
  explaining that `window` is the trailing-window argument
- **AND** a query using such an aggregation from a model stored before this rule fails
  with the same typed error

### Requirement: A model formula overriding a built-in renders

When a model's aggregation definition for a built-in name supplies a `formula`, that formula
SHALL render wherever the aggregation renders, windowed included, and SHALL accept only the
parameters it references; a definition without a `formula` SHALL keep the built-in's own
rendering and only supply parameter defaults.

#### Scenario: A model formula overriding a built-in is rendered

- **WHEN** a model defines `sum` with formula `SUM({value}) * {scale}` and a `scale`
  parameter defaulting to `2`, and a query aggregates `sum(price)`
- **THEN** the generated SQL is `SUM(price) * 2`
- **AND** `sum(price, scale=3)` renders `SUM(price) * 3`

### Requirement: Percentile p accepts a numeric literal in [0, 1]

The `percentile` aggregation's `p` SHALL be a finite numeric literal — optionally signed or
parenthesised — whose value lies in [0, 1]; its spelling SHALL be emitted verbatim. Any
other `p` (a column, an arithmetic expression, a string, NaN, an out-of-range or
non-finite number) SHALL be rejected with the existing typed error.

#### Scenario: Non-literal p is rejected

- **WHEN** a query aggregates `amount` with `percentile(p=quantity)`
- **THEN** the query fails with the typed "must be a numeric literal in [0, 1]" error

### Requirement: Aggregation parameters bind once

Every parameter value an aggregation's rendering reads — a query argument, or the
aggregation definition's default for a parameter the query does not supply — SHALL be
resolved once, when the query is bound, into the value it denotes: a column reference,
a literal, or an expression over column references. A definition default SHALL then be
indistinguishable from the same value supplied explicitly: identical results, identical
behaviour in every producer kind and position, and a single computation when both
appear in one query; only the public auto-generated name follows the query's own
spelling (`wpop(amount)` keeps the name `amount_wpop`). A string argument naming a
formula placeholder SHALL resolve exactly as the same text written unquoted; any other
string argument (e.g. `window='90d'`) is a setting, never SQL. Parameter text that
cannot be analysed — it does not parse in the datasource's dialect, or it contains an
aggregate, a window function or a subquery — SHALL fail when the query is bound with a
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
  not parse in the datasource's dialect, or passes such text as a string argument
  naming a placeholder
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

- **WHEN** a query passes for `n` the target dialect's spelling of a SQL string literal whose value is `a'b\c` (`'a''b\c'` on DuckDB and Postgres) to an aggregation whose formula reads `'{n}'`
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

### Requirement: Median and percentile on Trino are approximate
On a `trino` datasource, `median` and `percentile(p=...)` SHALL return Trino's approximate
percentile of the aggregated values (`APPROX_PERCENTILE`), which need not equal the interpolated
`PERCENTILE_CONT` value the other Tier-1 databases return.

#### Scenario: Result equals Trino's own approximate percentile
- **WHEN** `percentile(amount, p=0.9)` and `median(amount)` run on Trino
- **THEN** the results equal `APPROX_PERCENTILE(amount, 0.9)` and `APPROX_PERCENTILE(amount, 0.5)`
  computed directly by Trino over the same rows

#### Scenario: Approximation differs from interpolation
- **WHEN** `percentile(amount, p=0.9)` runs on Trino over the amounts 100, 200, 50, 150, 300, 25
  and 10.01
- **THEN** the result is 300, where the interpolated value would be 240
