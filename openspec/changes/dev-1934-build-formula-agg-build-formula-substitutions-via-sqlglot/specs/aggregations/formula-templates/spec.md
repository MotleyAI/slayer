## Purpose

Defines how `{value}` and `{param}` placeholders in an aggregation formula (built-in or
model-defined) are recognised and substituted into the generated SQL, and when a
formula template is rejected.

## ADDED Requirements

### Requirement: Placeholders are recognised as whole SQL tokens

A placeholder SHALL be a `{` token, one identifier-shaped token (a keyword name such as
`{order}` included; surrounding whitespace ignored), and a `}` token of the formula's SQL.
Brace text inside a string literal, a quoted identifier, or a comment SHALL NOT be a
placeholder and SHALL be emitted unchanged. Brace syntax that is not a placeholder (e.g. a
DuckDB struct literal `{'a': 1}`) SHALL be left to the SQL grammar.

#### Scenario: Placeholder inside a string literal is inert

- **WHEN** a custom aggregation formula is `MAX(CASE WHEN {value} > 0 THEN '{value}' END)`
  and a query aggregates `amount` with it
- **THEN** the first `{value}` is replaced by the `amount` column
- **AND** the string literal `'{value}'` is emitted unchanged

#### Scenario: Whitespace inside braces is tolerated

- **WHEN** a custom aggregation formula is `SUM({ value })`
- **THEN** it renders identically to `SUM({value})`

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
ever reach the emitted SQL.

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

### Requirement: A model formula overriding a built-in renders

When a model's aggregation definition for a built-in name supplies a `formula`, that formula
SHALL render wherever the aggregation renders, windowed included, and SHALL accept only the
parameters it references; a definition without a `formula` SHALL keep the built-in's own
rendering and only supply parameter defaults.

#### Scenario: A model formula overriding a built-in is rendered

- **WHEN** a model defines `sum` with formula `SUM({value}) * {scale}` and a `scale`
  parameter defaulting to `2`, and a query aggregates `price:sum`
- **THEN** the generated SQL is `SUM(price) * 2`
- **AND** `price:sum(scale=3)` renders `SUM(price) * 3`

### Requirement: Percentile p accepts a numeric literal in [0, 1]

The `percentile` aggregation's `p` SHALL be a finite numeric literal — optionally signed or
parenthesised — whose value lies in [0, 1]; its spelling SHALL be emitted verbatim. Any
other `p` (a column, an arithmetic expression, a string, NaN, an out-of-range or
non-finite number) SHALL be rejected with the existing typed error.

#### Scenario: Non-literal p is rejected

- **WHEN** a query aggregates `amount` with `percentile(p=quantity)`
- **THEN** the query fails with the typed "must be a numeric literal in [0, 1]" error
