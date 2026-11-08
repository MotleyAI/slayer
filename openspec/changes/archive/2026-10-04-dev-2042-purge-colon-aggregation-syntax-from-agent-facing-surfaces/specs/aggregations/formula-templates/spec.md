## MODIFIED Requirements

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
