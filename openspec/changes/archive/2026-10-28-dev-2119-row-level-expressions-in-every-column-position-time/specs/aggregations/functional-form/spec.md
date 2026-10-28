## ADDED Requirements

### Requirement: Default ranking key
A ranked `first` / `last` aggregation without an explicit ranking key SHALL rank by,
in order: the first entry of the query's `dimensions` (in projection order) that is
row-level and types DATE or TIMESTAMP — a column or a computed dimension, never a
granularity bucket, which ties every row of a cell; else the first time dimension's un-truncated source, column or
expression; else the model's default time dimension; else it SHALL fail with the
existing ranking-key resolution error.

#### Scenario: A temporal computed dimension is the default ranking key
- **WHEN** a query groups by the computed dimension `coalesce(shipped_at, created_at)`
  (typed TIMESTAMP) and a month time dimension on `created_at`, and selects
  `last(amount)`
- **THEN** each cell's value is the `amount` of its row latest by the coalesced
  timestamp, by executed values on SQLite and DuckDB

#### Scenario: An expression time dimension's source is the default ranking key
- **WHEN** a query's only temporal dimension is the time dimension
  `coalesce(shipped_at, created_at)` at `month` and it selects `last(amount)`
- **THEN** each month's value is the `amount` of its row latest by the un-truncated
  coalesced timestamp

### Requirement: An expression ranking key is judged by its leaves
An explicit or defaulted ranking key SHALL be determined by the aggregation's home
and never widen it (semantics Axiom 2.4), judged over every leaf the expression reads
and every dependency of a derived column it names, for local and cross-model
(target-rooted) producers alike: a key reading a leaf across a fanning or unproven
hop from the home SHALL fail with the existing ranked-input safety error naming that
leaf and the hop. Two ranked aggregations differing only in their ranking key SHALL
stay separate producers; equal keys, however spelled, SHALL share one.

#### Scenario: Fanning leaf inside an expression ranking key
- **WHEN** a query rooted at `customers` selects
  `last(spend, coalesce(orders.created_at, signup_ts))`, `customers → orders`
  one-to-many
- **THEN** it fails at plan time with the ranked-input safety error naming
  `orders.created_at` and the hop, never a value

#### Scenario: Cross-model ranked aggregate with an expression key
- **WHEN** a query rooted at `orders` selects
  `last(customers.spend, coalesce(customers.updated_at, customers.signup_ts), partition_by=region)`
- **THEN** it executes ranked inside the `customers`-rooted producer, by executed values

## MODIFIED Requirements

### Requirement: Ambiguous first and last names dispatch by argument shape
For `first` and `last` (both aggregation and transform names), the call SHALL
parse to one node and dispatch by the type of its bound first argument, which
binds exactly like any transform input: an argument that resolves to a row-level
value (a column, a literal, or an expression with a row-level leaf) SHALL be the
aggregation; an argument that resolves to an aggregate-valued operand — an
aggregation, a saved measure, or a grained transform, alone or composed through
arithmetic and scalar calls — SHALL be the transform over that series. The
dispatch SHALL NOT depend on the spelling of the argument, and SHALL NOT pre-empt
transform-input validation: an ill-formed transform inside the argument fails
with that transform's own error.

#### Scenario: Aggregation reading
- **WHEN** a measure is written `last(balance)` or `last(balance, updated_at)`
- **THEN** it is the `last` aggregation — the latest `balance` ranked by the time
  axis, or by `updated_at`

#### Scenario: Transform reading
- **WHEN** a measure is written `last(sum(revenue))`
- **THEN** it is the `last` transform over the aggregated series

#### Scenario: Saved-measure operand reads as the transform
- **WHEN** the model declares a measure `rev` with formula `sum(revenue)` and a
  query measure is written `first(rev)` or `first(rev * 2)`
- **THEN** it is the `first` transform over the saved measure's series — the
  same plan and executed values as `first(sum(revenue))` / `first(sum(revenue) * 2)`
  on SQLite and DuckDB — never an unknown-reference error

#### Scenario: Row-grain composite keeps the aggregation reading
- **WHEN** a measure is written `first(quantity * avg(unit_price, partition_by=product))`
  or `first(1)`
- **THEN** it is the `first` aggregation, picking the row-level value at the ranked
  row, by executed values — never the transform reading

#### Scenario: Dispatch does not pre-empt transform-input validation
- **WHEN** a measure is written `first(cumsum(weight))` with `weight` an unprojected
  row-level column
- **THEN** it fails at plan time with the grain-refining row-level-leaf error naming
  `cumsum` — never the aggregation reading

### Requirement: Positional parameters fold onto declared parameter order
An aggregation call MAY pass declared parameters positionally after its source
(or, for a re-aggregation, after its operand): positional values SHALL bind to
the aggregation's declared parameter order — the built-in registry
(`percentile` → `p`; `weighted_avg` → `weight`; `corr`/`covar_samp`/`covar_pop`
→ `other`) or a custom aggregation's `params` declaration order — yielding the
identical aggregation identity, SQL, results, and result keys as the named
spelling. Passing a parameter both positionally and by name, or more positional
values than declared parameters — any positional value at all on an aggregation
that declares none — SHALL fail with a clear error naming the rule. Ranked
`first`/`last` declare no parameters — they take at most one positional value,
their ranking key (resolved per › Default ranking key when omitted), which, when
given, SHALL be a row-level expression — a column being the one-leaf case — never a
literal-only expression or one carrying an aggregate, transform or attached value. After binding an
attached (aggregate- or transform-valued) parameter is therefore always a named
parameter.

#### Scenario: Positional percentile equals named
- **WHEN** a measure is written `percentile(price, 0.9)`
- **THEN** SQL, results, and result keys are identical to `percentile(price, p=0.9)`

#### Scenario: Positional parameter on a re-aggregation outer
- **WHEN** a measure is written
  `percentile(sum(amount, partition_by=[city, region]), 0.9)`
- **THEN** it equals the `p=0.9` spelling by executed values

#### Scenario: Custom aggregation binds positionals by declared order
- **WHEN** a model declares `wavg(weight)` and a measure is written `wavg(amount, id)`
- **THEN** it equals `wavg(amount, weight=id)` by executed values

#### Scenario: Duplicate and excess positional parameters error
- **WHEN** `percentile(price, 0.9, p=0.5)` or `percentile(price, 0.9, 0.5)` is submitted
- **THEN** each fails with a clear error naming the duplicated parameter or the
  declared-parameter count

#### Scenario: Positional value on a parameterless aggregation errors
- **WHEN** `sum(amount, 1)` or `sum(customers.spend, sum(customers.spend, partition_by=status))`
  is submitted, under any `to_many_handling` mode
- **THEN** each fails at bind with a clear error naming the aggregation and that it
  takes no parameters — never an executed value

#### Scenario: Invalid first/last ranking key errors
- **WHEN** `last(amount, sum(amount, partition_by=region))`, `last(amount, 1)` or
  `last(amount, id, amount)` is submitted
- **THEN** each fails at bind with a clear error naming the ranking-key rule

#### Scenario: Expression ranking key
- **WHEN** a measure is written `last(balance, coalesce(updated_at, created_at))`
- **THEN** it is the `balance` of the row latest by the coalesced timestamp, by
  executed values on SQLite and DuckDB

#### Scenario: Positional transform parameter equals named
- **WHEN** a measure is written
  `weighted_avg(customers.spend, rank(sum(amount, partition_by=customers.regions.name), direction='desc'))`
  rooted at `orders`
- **THEN** it binds to the identical aggregation identity as the `weight=rank(..., direction='desc')`
  spelling and returns identical result keys and values

