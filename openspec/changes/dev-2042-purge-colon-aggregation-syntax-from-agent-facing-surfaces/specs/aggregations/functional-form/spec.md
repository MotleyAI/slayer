## ADDED Requirements

### Requirement: SLayer emits only the functional spelling
Every aggregation SLayer writes or shows to an agent SHALL use the functional
spelling `agg(col, args)`, whatever spelling its input used: model measure
formulas written by the dbt, Cube (including view facades) and OSI importers,
root-model recommendation paths, error and warning remedies, and tool
descriptions and help.

#### Scenario: Importer-written formulas are functional
- **WHEN** a dbt, Cube or OSI project is imported
- **THEN** every saved measure formula uses the functional spelling (for example
  `sum(amount, window='30d')`, `count(orders.*)`, `percentile(price, p=0.9)`,
  `sum(a) / count(*)`) and parses

#### Scenario: Recommendation paths are functional
- **WHEN** `recommend_root_model` is given `sum(orders.amount)`, its legacy colon
  twin, or `percentile(orders.amount, p=0.9)`, and recommends `orders`
- **THEN** the item paths are `sum(amount)` and `percentile(amount, p=0.9)`; a
  cross-model item keeps its join path inside the call (`sum(customers.regions.population)`)

#### Scenario: Error remedies are functional
- **WHEN** a model defines a derived column across a fanning hop
- **THEN** the save error's remedy reads `<aggregation>(orders.line_items.qty)`

#### Scenario: Tool descriptions teach the functional spelling
- **WHEN** the MCP tool descriptions and help are listed
- **THEN** none shows the colon spelling; a custom aggregation is shown used as
  `sum_sq(column)`

### Requirement: The legacy colon spelling is accepted as an exact equivalent
The system SHALL accept the legacy colon spelling `col:agg(args)` as an exact
equivalent of `agg(col, args)` in every position that accepts an aggregation:
same generated SQL, same result values, same result-column keys, the same
measure, filter and order matching across the two spellings, and the same error
behavior for invalid combinations.

#### Scenario: Every builtin aggregation has an exact colon twin
- **WHEN** each builtin aggregation is written in the legacy colon spelling —
  including star (`*:count`), cross-model star (`customers.*:count`), a join-path
  column, keyword and positional parameters (`price:percentile(p=0.9)`,
  `balance:last(updated_at)`), and the reserved `window=` / `partition_by=` kwargs
- **THEN** its SQL, results, result key and errors equal those of its functional
  twin (parametrized over the full builtin set, not a hardcoded list)

## MODIFIED Requirements

### Requirement: Functional spelling works in every aggregation position
The system SHALL accept the functional spelling in every position that accepts
an aggregation: query measures, query filters (both row-level and
post-aggregation phases), order, model measure formulas (saved via the API and
hand-authored in YAML storage), model-extension measures, inline source-model
measures, multi-stage source-query formulas, computed-dimension expressions,
and inside transform or arithmetic expressions (including mixed-grain
arithmetic over partitioned aggregates).

#### Scenario: Query filter routed to HAVING
- **WHEN** a query filter is written `sum(revenue) > 100`
- **THEN** it applies after aggregation (HAVING), keeping only the groups whose
  `revenue_sum` exceeds 100

#### Scenario: Order by functional aggregation
- **WHEN** an order entry is written `sum(revenue)`
- **THEN** results are ordered by that aggregate, under the result key `revenue_sum`

#### Scenario: Hand-authored YAML model measure
- **WHEN** a model whose measure formula is `sum(revenue)` is loaded from YAML storage without passing through save
- **THEN** queries against it succeed and return the summed revenue

#### Scenario: Model-extension and inline-model measures
- **WHEN** a `ModelExtension` measure or an inline `source_model` measure uses the functional spelling
- **THEN** the query succeeds and returns the aggregation's executed values

#### Scenario: Multi-stage source-query formulas
- **WHEN** a stage formula in a `source_queries` pipeline uses the functional spelling
- **THEN** the stage computes the aggregation and exposes it to later stages

#### Scenario: Inside transforms and arithmetic
- **WHEN** a measure is written `cumsum(sum(revenue))` or `sum(revenue) / count(*)`
- **THEN** the first is the running total of `revenue_sum` and the second the
  per-row average revenue of each group

#### Scenario: Cross-spelling rename and filter-form matching
- **WHEN** a measure is declared `{"formula": "sum(revenue)", "name": "rev"}` and a filter references `SUM(revenue)` (or vice versa)
- **THEN** the filter resolves to the same measure — spelling never affects matching

#### Scenario: Computed dimension with a functional partitioned aggregate
- **WHEN** a computed dimension is written with `sum(amount, partition_by=city)` in its expression (bare, banded via CASE, or under a transform)
- **THEN** the dimension groups rows by each city's amount total, named and
  grouped like any computed dimension

#### Scenario: Computed-dimension guards fire for both spellings
- **WHEN** a computed dimension contains `sum(amount)` with no `partition_by=`
- **THEN** it fails with the bare-aggregate-requires-partition_by error

#### Scenario: Mixed-grain arithmetic with functional spellings
- **WHEN** a measure or filter combines aggregates at different partition grains written functionally (e.g. `sum(a, partition_by=region) - sum(b, partition_by=city)`)
- **THEN** each aggregate is computed at its own partition grain and the
  expression combines them per row, by executed values

### Requirement: Aggregation-name healing applies to functional spelling
The system SHALL match functional aggregation names case-insensitively against
the builtin aggregations and SHALL heal aggregation-name aliases.

#### Scenario: Uppercase builtin
- **WHEN** a measure is written `SUM(revenue)`
- **THEN** it behaves identically to `sum(revenue)`

#### Scenario: Alias healing
- **WHEN** a measure is written `countD(user_id)`
- **THEN** it behaves identically to `count_distinct(user_id)`

### Requirement: Unknown and custom aggregation names defer to binding
A function call whose first argument is aggregatable and whose name is not a
scalar function or transform SHALL be treated as an aggregation candidate and
validated at binding: model-defined custom aggregations resolve, and unknown
names fail with the standard unknown-aggregation error regardless of source
shape (column, star, or expression).

#### Scenario: Custom aggregation functional call
- **WHEN** a model defines a custom aggregation `my_agg` and a measure is written `my_agg(price)`
- **THEN** it renders the model's `my_agg` definition over `price`

#### Scenario: Unknown name over a column
- **WHEN** a measure is written `bogus(price)`
- **THEN** binding fails with the standard unknown-aggregation error naming `bogus`

#### Scenario: Unknown name over star
- **WHEN** a measure is written `bogus(*)`
- **THEN** it fails with the standard unknown-aggregation error (not a downstream SQL-generation failure)

#### Scenario: Construction-time filter with custom functional aggregation
- **WHEN** a query containing the filter `my_agg(price) > 0` is constructed before any model context exists
- **THEN** construction succeeds and the name is validated later at binding

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
- **THEN** it is the `first` aggregation, which rejects the expression source with
  the existing "not supported over an expression" error

#### Scenario: Dispatch does not pre-empt transform-input validation
- **WHEN** a measure is written `first(cumsum(weight))` with `weight` an unprojected
  row-level column
- **THEN** it fails at plan time with the grain-refining row-level-leaf error naming
  `cumsum` — never the aggregation's "not supported over an expression" error

### Requirement: Entity references accept functional spelling
Entity-reference surfaces (memories/search resolution, root-model
recommendation) SHALL accept the functional spelling of an aggregated column
reference, interpreted by the same rules as query parsing; multi-column
expression text is not a valid entity reference.

#### Scenario: Functional entity reference
- **WHEN** an entity reference is written `sum(orders.revenue)`
- **THEN** it resolves to the `sum` aggregation of the `orders.revenue` column

#### Scenario: Expression is not an entity reference
- **WHEN** an entity reference is written `sum(orders.amount - orders.cost)`
- **THEN** resolution fails (no silent partial match)

### Requirement: Syntax boundaries are preserved
Mode-A raw-SQL surfaces SHALL continue to treat `SUM(x)` as raw SQL, and
SQL-style `DISTINCT` inside a functional call SHALL remain a syntax error.

#### Scenario: Mode A unchanged
- **WHEN** a model column's `sql` contains `SUM(amount)`
- **THEN** it is passed through as raw SQL exactly as before

#### Scenario: DISTINCT keyword rejected
- **WHEN** a measure is written `count(distinct user_id)`
- **THEN** parsing fails (the supported spelling is `count_distinct(user_id)`)

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
their ranking column (the time axis when omitted), which, when given, SHALL be a
column reference, never a literal or an attached value. After binding an
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
- **THEN** each fails at bind with a clear error naming the ranking-column rule

#### Scenario: Positional transform parameter equals named
- **WHEN** a measure is written
  `weighted_avg(customers.spend, rank(sum(amount, partition_by=customers.regions.name)))`
  rooted at `orders`
- **THEN** it binds to the identical aggregation identity as the `weight=rank(...)`
  spelling and returns identical result keys and values

### Requirement: Repeated keyword arguments are rejected
A call in a Mode-B expression — an aggregation or a transform — SHALL reject a
keyword argument that appears more than once with a parse-time error naming the
call and the keyword; the parser never keeps the last occurrence and never
concatenates the values.

#### Scenario: Repeated partition_by on a transform
- **WHEN** a measure names `rank(sum(amount), partition_by=region, partition_by=city)`
- **THEN** parsing fails with an error naming `rank` and `partition_by`

#### Scenario: Repeated keyword on an aggregation
- **WHEN** a measure names `sum(amount, partition_by=region, partition_by=city)`
- **THEN** parsing fails with an error naming the aggregation and `partition_by`

## REMOVED Requirements

### Requirement: Functional spelling is equivalent to colon spelling
**Reason**: The functional spelling is canonical; colon is legacy input. The equivalence guarantee moves, unchanged, to "The legacy colon spelling is accepted as an exact equivalent", stated from the legacy side with one scenario parametrized over every builtin.
**Migration**: None for users — colon input keeps parsing identically; tests of the colon twins map to the new requirement's parametrized scenario.
