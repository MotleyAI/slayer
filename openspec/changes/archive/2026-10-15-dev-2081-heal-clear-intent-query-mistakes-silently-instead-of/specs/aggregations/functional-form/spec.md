## ADDED Requirements

### Requirement: The method-call spelling is an aggregation spelling
`<path>.<name>(<source>[, <literal arguments>])` SHALL be accepted in every position that accepts an
aggregation as an exact equivalent of `<name>(<path>.<source>[, <literal arguments>])` — same
aggregation, result key, SQL and values — where `<path>` is a dotted join path, `<source>` is `*`, a
bare column name, or an expression whose column leaves are all bare names (each leaf read under
`<path>`), and every further argument, positional or keyword, is a literal. `<name>` SHALL be resolved
exactly as in its functional twin: builtin aggregations, aggregation-name aliases, custom aggregations
validated at binding, granularity calls, and the argument-shape dispatch of `first` / `last`. Any other
method-call shape SHALL be rejected with an error showing the functional spelling: a dotted source
leaf, a non-literal further argument, a `partition_by=` argument, or a callee that is a scalar
function or a transform-only name.

#### Scenario: Count of a joined model's rows
- **WHEN** a query rooted at `orders` measures `order_items.count(*)`
- **THEN** its columns, rows and SQL equal those of `count(order_items.*)`

#### Scenario: Expression source
- **WHEN** a measure is written `order_items.sum(price * qty)`
- **THEN** it equals `sum(order_items.price * order_items.qty)`

#### Scenario: Literal parameter
- **WHEN** a measure is written `order_items.percentile(price, 0.9)`
- **THEN** it equals `percentile(order_items.price, 0.9)`

#### Scenario: Granularity callee follows its twin
- **WHEN** a dimension expression is written `order_items.month(created_at)`
- **THEN** it equals `month(order_items.created_at)`

#### Scenario: Ambiguous method-call shapes are rejected
- **WHEN** a measure is written `order_items.weighted_avg(price, qty)`, `order_items.sum(products.price)`, `order_items.sum(price, partition_by=region)` or `order_items.round(price)`
- **THEN** each is rejected with an error showing the functional spelling to write

## MODIFIED Requirements

### Requirement: Unknown and custom aggregation names defer to binding
A function call whose first argument is aggregatable and whose name is not a
scalar function or transform SHALL be treated as an aggregation candidate and
validated at binding: model-defined custom aggregations resolve, granularity
names resolve to time buckets, and any other name SHALL fail with the
unknown-function error — "Unknown function '<name>'", naming the categories of
callable names (aggregations, scalar functions, transforms, time granularities)
and a close match when one exists — regardless of source shape (column, star,
or expression).

#### Scenario: Custom aggregation functional call
- **WHEN** a model defines a custom aggregation `my_agg` and a measure is written `my_agg(price)`
- **THEN** it renders the model's `my_agg` definition over `price`

#### Scenario: Unknown name over a column
- **WHEN** a measure is written `bogus(price)`
- **THEN** binding fails with the unknown-function error naming `bogus`

#### Scenario: Unknown name over star
- **WHEN** a measure is written `bogus(*)`
- **THEN** it fails with the unknown-function error (not a downstream SQL-generation failure)

#### Scenario: Construction-time filter with custom functional aggregation
- **WHEN** a query containing the filter `my_agg(price) > 0` is constructed before any model context exists
- **THEN** construction succeeds and the name is validated later at binding

### Requirement: Syntax boundaries are preserved
Mode-A raw-SQL surfaces SHALL continue to treat `SUM(x)` as raw SQL. In Mode B,
`count(distinct <expr>[, <arguments>])` (`count` and `distinct` matched
case-insensitively, `<expr>` any aggregation source other than `*`) SHALL be an
exact equivalent of `count_distinct(<expr>[, <arguments>])` — same result key,
SQL and values. Every other SQL-style `DISTINCT` inside a call — another
aggregation, `count(distinct *)`, or several distinct operands — SHALL be
rejected with an error stating that `DISTINCT` is supported only as
`count(distinct x)`, i.e. `count_distinct(x)`. Text inside string literals SHALL
never be read as `distinct`.

#### Scenario: Mode A unchanged
- **WHEN** a model column's `sql` contains `SUM(amount)`
- **THEN** it is passed through as raw SQL exactly as before

#### Scenario: count(distinct) equals count_distinct
- **WHEN** a measure is written `count(distinct user_id)` or `COUNT(DISTINCT user_id)`
- **THEN** its columns, rows and SQL equal those of `count_distinct(user_id)`

#### Scenario: count(distinct) with an expression and a partition
- **WHEN** a measure is written `count(distinct coalesce(user_id, 0), partition_by=region)`
- **THEN** it equals `count_distinct(coalesce(user_id, 0), partition_by=region)`

#### Scenario: DISTINCT keyword rejected
- **WHEN** a measure is written `sum(distinct amount)`, `count(distinct *)` or `count(distinct a, b)`
- **THEN** each is rejected with the error stating that `DISTINCT` is supported only as `count(distinct x)`
