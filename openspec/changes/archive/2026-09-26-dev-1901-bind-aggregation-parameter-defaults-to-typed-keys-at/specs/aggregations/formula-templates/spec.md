## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: Aggregation parameters bind once

Every parameter value an aggregation's rendering reads — a query argument, or the
aggregation definition's default for a parameter the query does not supply — SHALL be
resolved once, when the query is bound, into the value it denotes: a column reference,
a literal, or an expression over column references. A definition default SHALL then be
indistinguishable from the same value supplied explicitly: identical results, identical
behaviour in every producer kind and position, and a single computation when both
appear in one query; only the public auto-generated name follows the query's own
spelling (`amount:wpop` keeps the name `amount_wpop`). A string argument naming a
formula placeholder SHALL resolve exactly as the same text written unquoted; any other
string argument (e.g. `window='90d'`) is a setting, never SQL. Parameter text that
cannot be analysed — it does not parse in the datasource's dialect, or it contains an
aggregate, a window function or a subquery — SHALL fail when the query is bound with a
typed error naming the aggregation, the parameter and the text, never emit invalid SQL
or be treated as referencing nothing.

#### Scenario: A default equals its explicit spelling

- **WHEN** a model declares `wsum` (`SUM({value} * {weight})`) with `weight`
  defaulting to `quantity`, and one query selects `amount:wsum` and
  `amount:wsum(weight=quantity)`
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
