# aggregations/functional-form Specification

## Purpose
Makes the functional spelling `agg(col, args)` the canonical aggregation spelling in
every position where aggregations are accepted, for all builtin, aliased, and custom
aggregations.

## Requirements

### Requirement: Functional spelling works in every aggregation position
The system SHALL accept the functional spelling in every position that accepts
an aggregation: query measures, query filters (both row-level and
post-aggregation phases), order, model measure formulas (saved via the API and
hand-authored in YAML storage), model-extension measures, multi-stage source-query formulas, computed-dimension expressions,
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
- **WHEN** a `ModelExtension` measure uses the functional spelling
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

### Requirement: Functional input is first-class — no rewriting, no warning
The system SHALL NOT emit a normalization warning for functional aggregations,
and SHALL NOT rewrite stored formula text: saving a model preserves the
author's spelling.

#### Scenario: No warning on execute
- **WHEN** a query using `sum(revenue)` executes
- **THEN** the response contains no FUNC_STYLE_AGG (or equivalent) normalization warning

#### Scenario: Save preserves spelling
- **WHEN** a model measure written `sum(revenue)` is saved and re-read
- **THEN** the stored formula text is still `sum(revenue)`

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

### Requirement: Custom aggregation names cannot shadow scalar functions
Model validation SHALL reject a custom aggregation whose name collides with a
scalar-allowlist function, as it already rejects transform-name collisions, so
every legal aggregation is reachable in functional form.

#### Scenario: Scalar-colliding custom aggregation rejected
- **WHEN** a model defines a custom aggregation named `round`
- **THEN** validation fails with an error naming the collision

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

### Requirement: Repeated keyword arguments are rejected
A call in a Mode-B expression — an aggregation or a transform — SHALL reject a
keyword argument that appears more than once with a parse-time error naming the
call and the keyword; the parser never keeps the last occurrence and never
concatenates the values.

#### Scenario: Repeated partition_by on a transform
- **WHEN** a measure names `rank(sum(amount), partition_by=region, partition_by=city, direction='desc')`
- **THEN** parsing fails with an error naming `rank` and `partition_by`

#### Scenario: Repeated keyword on an aggregation
- **WHEN** a measure names `sum(amount, partition_by=region, partition_by=city)`
- **THEN** parsing fails with an error naming the aggregation and `partition_by`

### Requirement: SLayer emits only the functional spelling
Every aggregation SLayer writes or shows to an agent, or passes to its own engine,
SHALL use the functional spelling `agg(col, args)`, whatever spelling its input
used: model measure formulas written by the dbt, Cube (including view facades) and
OSI importers, the SQL facade's catalog and translated queries, schema-probe queries,
memory entity resolution, root-model recommendation paths, error and warning
remedies, and tool descriptions and help. Result keys SHALL be unchanged by the
spelling: an unnamed measure keeps the key it had when the colon spelling was the
canonical text.

#### Scenario: Importer-written formulas are functional
- **WHEN** a dbt, Cube or OSI project is imported
- **THEN** every saved measure formula uses the functional spelling (for example
  `sum(amount, window='30d')`, `count(orders.*)`, `percentile(price, p=0.9)`,
  `sum(a) / count(*)`) and parses

#### Scenario: Recommendation paths are functional
- **WHEN** `recommend_root_model` is given `sum(orders.amount)` or
  `percentile(orders.amount, p=0.9)`, and recommends `orders`
- **THEN** the item paths are `sum(amount)` and `percentile(amount, p=0.9)`; a
  cross-model item keeps its join path inside the call (`sum(customers.regions.population)`)

#### Scenario: Error remedies are functional
- **WHEN** a model defines a derived column across a fanning hop
- **THEN** the save error's remedy reads `<aggregation>(orders.line_items.qty)`

#### Scenario: Tool descriptions teach the functional spelling
- **WHEN** the MCP tool descriptions and help are listed
- **THEN** none shows the colon spelling; a custom aggregation is shown used as
  `sum_sq(column)`

#### Scenario: The SQL facade translates to functional queries
- **WHEN** a SQL client runs `SELECT COUNT(*), SUM(revenue) FROM orders GROUP BY status HAVING SUM(revenue) > 1000`, and another selects a joined model's row count and `customers.region` count
- **THEN** the translated SLayer queries measure `count(*)`, `sum(revenue)`, `count(customers.*)` and `count(customers.region)` and filter `sum(revenue) > 1000`, and the result column names are unchanged

#### Scenario: Result keys are unchanged
- **WHEN** a query on `orders` has the unnamed measures `sum(amount)`, `count(*)`, `sum(amount) / count(*)`, `cumsum(sum(amount))` and `sum(amount) FILTER (WHERE status = 'paid')`, and an unnamed computed dimension `sum(amount, partition_by=region)`
- **THEN** their result keys are `orders.amount_sum`, `orders._count`, `orders.amount_sum_count`, `orders.cumsum_amount_sum`, `orders.amount_sum_filter_where_status_paid` and `orders.amount_sum_partition_by_region`, as before this change, including names long enough to carry a hash

### Requirement: The method-call spelling is an aggregation spelling
`<path>.<name>(<source>[, <literal arguments>])` SHALL be accepted in every position that accepts an
aggregation as a deprecated exact equivalent of `<name>(<path>.<source>[, <literal arguments>])` — same
aggregation, result key, SQL and values, with the deprecation warnings and save-time rewrite of
`queries/expression-grammar` › Legacy spellings keep their meaning — where `<path>` is a dotted join path,
`<source>` is `*`, a bare column name, or an expression whose column leaves are all bare names (each leaf
read under `<path>`), and every further argument, positional or keyword, is a literal. `<name>` SHALL be
resolved exactly as in its functional twin: builtin aggregations, aggregation-name aliases, custom
aggregations validated at binding, and the argument-shape dispatch of `first` / `last`. Any other
method-call shape SHALL be rejected with an error showing the functional spelling: a dotted source leaf,
a non-literal further argument, a `partition_by=` argument, or a callee that is a scalar function, a
date-part alias (including `year` … `second`) or a transform-only name.

#### Scenario: Count of a joined model's rows
- **WHEN** a query rooted at `orders` measures `order_items.count(*)`
- **THEN** its columns, rows and SQL equal those of `count(order_items.*)`, and the response warns naming `count(order_items.*)`

#### Scenario: Expression source
- **WHEN** a measure is written `order_items.sum(price * qty)`
- **THEN** it equals `sum(order_items.price * order_items.qty)`

#### Scenario: Literal parameter
- **WHEN** a measure is written `order_items.percentile(price, 0.9)`
- **THEN** it equals `percentile(order_items.price, 0.9)`

#### Scenario: Granularity callee follows its twin
- **WHEN** a dimension expression is written `order_items.month(created_at)`
- **THEN** it is rejected with an error showing `month(order_items.created_at)`, because `month` is a date-part function, not an aggregation

#### Scenario: Ambiguous method-call shapes are rejected
- **WHEN** a measure is written `order_items.weighted_avg(price, qty)`, `order_items.sum(products.price)`, `order_items.sum(price, partition_by=region)` or `order_items.round(price)`
- **THEN** each is rejected with an error showing the functional spelling to write

### Requirement: An aggregation accepts a FILTER clause
`agg(…) FILTER (WHERE c)` SHALL be accepted on every aggregation — built-in, aliased and model-defined custom — in every aggregation position, and SHALL mean the aggregation computed over only the rows where `c` is true. `c` SHALL be a row-level predicate over the aggregation's dataset; an aggregate, a transform or an attached value inside `c` SHALL be rejected with a typed query error naming the clause. Every row the aggregation reads SHALL be restricted by `c` — the source value, every column-valued parameter (`weight=`, `other=`), the ranking key of `first` / `last`, and every row a custom aggregation's formula reads directly (`COUNT(*)`, a bare column); literal parameters are unaffected. The restriction SHALL apply to the aggregation's own rows only, never to the query's result rows, a windowed aggregation's time buckets or any other measure. A grouped cell with rows but none passing `c` SHALL take the aggregation's empty value — NULL for a custom aggregation; with no grain, a custom aggregation SHALL read its formula's value over no rows. The clause SHALL compose with `partition_by=` and `window=`. An unnamed formula containing a filtered aggregation SHALL take its result key from its full text by the existing expression-naming rule — the same rule whether or not the condition holds a `{variable}`, a variable naming itself by its placeholder — and spellings of one formula that differ only in case or whitespace SHALL share that key. A filtered aggregation and the same aggregation unfiltered, or filtered by a different condition, SHALL never share a result key or a value, whether they are named or unnamed, in one formula or in separate measures.

#### Scenario: Filtered count
- **WHEN** a query on `orders` by `region` has measure `count(*) FILTER (WHERE status = 'paid')`
- **THEN** each region's value is its number of paid orders, 0 where it has none

#### Scenario: Filtered sum equals a masked sum
- **WHEN** one measure is `sum(amount) FILTER (WHERE status = 'paid')` and another `sum(iif(status = 'paid', amount, NULL))`
- **THEN** both return the same values

#### Scenario: Filtered weighted average uses only passing rows
- **WHEN** a measure is `weighted_avg(price, weight=quantity) FILTER (WHERE status = 'paid')`
- **THEN** it equals the weighted average computed over the paid rows alone, numerator and denominator both

#### Scenario: Filtered last picks the newest passing row
- **WHEN** a query by `customer_id` has measure `last(balance, updated_at) FILTER (WHERE status = 'active')` and a customer's newest row is inactive
- **THEN** the value is the balance of that customer's newest active row

#### Scenario: Aggregate inside the condition rejected
- **WHEN** a measure is `sum(amount) FILTER (WHERE amount > avg(amount))`
- **THEN** it is rejected with the typed error naming the FILTER clause

#### Scenario: With partition_by
- **WHEN** a query by `region, city` has measure `sum(amount, partition_by=region) FILTER (WHERE status = 'paid')`
- **THEN** each row carries its region's paid total

#### Scenario: Constant condition named from its text
- **WHEN** a query on `orders` has the unnamed measures `count(*) FILTER (WHERE status = 'paid')` and `sum(amount) FILTER (WHERE amount > 10)`
- **THEN** their result keys are `orders.count_filter_where_status_paid` and `orders.amount_sum_filter_where_amount_10`, never a name built from an internal masked form

#### Scenario: Variable and constant conditions follow one rule
- **WHEN** one query has the unnamed measure `sum(amount) FILTER (WHERE amount > {n})` with `n = 10` and another has `sum(amount) FILTER (WHERE amount > 10)`
- **THEN** their result keys are `orders.amount_sum_filter_where_amount_n` and `orders.amount_sum_filter_where_amount_10`, both derived from the formula text as written

#### Scenario: Spellings of one formula share a key
- **WHEN** the unnamed measure is written `count(*) filter (where status = 'paid')` instead
- **THEN** its result key is `orders.count_filter_where_status_paid`

#### Scenario: Filtered first beside the unfiltered first
- **WHEN** a query on `orders` has the unnamed measures `first(amount) FILTER (WHERE status = 'open')` and `first(amount)`, and a second query has the same two measures named `fo` and `f`
- **THEN** both queries execute, each measure under its own result key, with `fo` the amount of the earliest open order and `f` the amount of the earliest order

#### Scenario: Filtered and unfiltered first inside one formula
- **WHEN** a measure is `first(amount) FILTER (WHERE status = 'open') - first(amount)` and the same with `last`
- **THEN** each returns the difference of the two distinct picks, never 0 from one pick read twice

#### Scenario: Conditions whose names coincide stay distinct
- **WHEN** a query has the measures `first(amount) FILTER (WHERE status = 'open')` and `first(amount) FILTER (WHERE status <> 'open')`, named `a` and `b`
- **THEN** `a` is the amount of the earliest open order and `b` the amount of the earliest order that is not open

#### Scenario: Filtered star count over a joined condition beside the plain count
- **WHEN** a query on `orders` has the unnamed measures `count(*) FILTER (WHERE customers.name = 'Alice')` and `count(*)`
- **THEN** both execute under distinct result keys, with Alice's order count and the total order count

#### Scenario: Ordering by the filtered formula
- **WHEN** a query on `orders` by `status` has the measure `count(*) FILTER (WHERE status = 'paid')` and orders by the same formula text descending
- **THEN** the order applies to that measure's column

#### Scenario: Filtered custom row count
- **WHEN** `orders` defines the custom aggregation `rows` with formula `COUNT(*)` and a query on `orders` by `region` has measure `rows(amount) FILTER (WHERE status = 'paid')`
- **THEN** each region's value is its number of paid orders, and NULL for a region whose orders include none that are paid

#### Scenario: Filtered custom formula reading a raw column
- **WHEN** `orders` defines the custom aggregation `per_order` with formula `SUM({value}) / COUNT(DISTINCT id)` and a measure is `per_order(amount) FILTER (WHERE status = 'paid')`
- **THEN** it equals the paid amount divided by the number of distinct paid orders, grouped or ungrouped

#### Scenario: Filtered custom formula reading only placeholders
- **WHEN** `orders` defines `wsum` with formula `SUM({value} * {weight})` (`weight` defaulting to `quantity`) and a measure is `wsum(price) FILTER (WHERE status = 'paid')`
- **THEN** it equals `wsum(price)` over a query restricted to the paid rows

#### Scenario: Ungrouped filtered custom aggregation with no passing rows
- **WHEN** a query on `orders` with no dimensions has measure `rows(amount) FILTER (WHERE status = 'none')`
- **THEN** the value is 0, the formula's value over no rows

#### Scenario: Filtered custom aggregation in a trailing window
- **WHEN** a query on `orders` by month has measure `rows(amount, window='2m') FILTER (WHERE status = 'paid')` and one month has orders but none paid while the month before has a paid order
- **THEN** that month is in the result and its value counts the paid orders of its trailing interval

#### Scenario: Filtered custom aggregation beside unfiltered measures
- **WHEN** a query on `orders` by `region` has the measures `rows(amount) FILTER (WHERE status = 'paid')`, `rows(amount)` and `count(*)`
- **THEN** the result has one row per region and the unfiltered measures equal their values in the query without the filtered one

#### Scenario: Filtered custom aggregation over a joined model
- **WHEN** a query on `customers` by `name` has measure `rows(orders.amount) FILTER (WHERE orders.status = 'paid')`
- **THEN** each customer's value is their number of paid orders, NULL for a customer with no paid order

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

### Requirement: The colon spelling is refused

Fresh input using the colon aggregation spelling `<source>:<name>[(<args>)]` — a column, a join-path column, `*` or `<path>.*` source — or the colon granularity spelling `<column>:<granularity>` SHALL be rejected with a typed error naming the colon span and its twin, in every Mode-B position (query measures, filters, order keys, dimensions, time dimensions, `main_time_dimension`, computed-dimension expressions, model measure formulas, model-extension measures, stage formulas), when a model or saved query is saved, and in entity references (`recommend_root_model` items, memory `linked_entities`). The twin SHALL be `date_trunc('<name>', <source>)` when `<name>` is a built-in granularity, and `<name>(<source>[, <args>])` otherwise, with the source and arguments as written. A colon inside a string literal, a quoted identifier or a `::` cast SHALL NOT be refused.

#### Scenario: Measure

- **WHEN** a query on `orders` has the measure `amount:sum`
- **THEN** it is rejected with the typed error naming `amount:sum` and `sum(amount)`

#### Scenario: Every builtin aggregation and alias

- **WHEN** each builtin aggregation and each aggregation-name alias is written in the colon spelling as a measure
- **THEN** each is rejected naming its functional twin (parametrized over the full builtin and alias sets)

#### Scenario: Star and cross-model star

- **WHEN** a query on `orders` has the measure `*:count`, and another `customers.*:count`
- **THEN** they are rejected naming `count(*)` and `count(customers.*)`

#### Scenario: Parameters

- **WHEN** a measure is `price:percentile(p=0.9)`, `balance:last(updated_at)` or `amount:sum(window='30d')`
- **THEN** each is rejected naming `percentile(price, p=0.9)`, `last(balance, updated_at)` and `sum(amount, window='30d')`

#### Scenario: Filter and order key

- **WHEN** a query filters `amount:sum > 100`, and another orders by `amount:sum` descending, as a string and as an object order item
- **THEN** each is rejected with the typed error naming `sum(amount)`

#### Scenario: Built-in granularity

- **WHEN** a query has the dimension `created_at:month`, the time dimension `created_at:month`, or the order key `customers.created_at:month`
- **THEN** each is rejected naming `date_trunc('month', created_at)` (`date_trunc('month', customers.created_at)` for the order key)

#### Scenario: Custom granularity

- **WHEN** a query on a datasource defining the granularity `fiscal_quarter` has the time dimension `created_at:fiscal_quarter`
- **THEN** it is rejected naming `fiscal_quarter(created_at)`

#### Scenario: Model and saved-query saves

- **WHEN** a model is saved with the measure formula `amount:sum`, or a query-backed model with a source query measuring `*:count`
- **THEN** the save is refused with the typed error naming the twin, and nothing is stored

#### Scenario: Entity references

- **WHEN** `recommend_root_model` is given `orders.amount:sum`, or a memory is saved with the linked entity `orders.amount:sum`
- **THEN** each is rejected with the typed error naming `sum(orders.amount)`

#### Scenario: Colons that are not the colon spelling

- **WHEN** a filter is `note = 'a:sum'`, a measure is `sum(amount::INT)`, and a measure is `sum(amount) FILTER (WHERE note = 'x:count')`
- **THEN** each is accepted

### Requirement: first and last never pick a NULL ranking key
A ranked `first` / `last` aggregation (plain, partitioned, windowed or cross-model) SHALL pick only among the rows whose ranking key is non-NULL: `first` the row with the smallest key, `last` the row with the largest, on every dialect and independent of its native NULL ordering or session settings. A cell whose rows all have a NULL ranking key SHALL take the empty value, NULL. A NULL value under a non-NULL key SHALL be picked like any other value.

#### Scenario: NULL keys are skipped in both directions
- **WHEN** a customer's rows are `(updated_at 2024-01-01, status 'a')`, `(NULL, 'b')`, `(2024-03-01, 'c')` and a query selects `first(status, updated_at)` and `last(status, updated_at)`
- **THEN** they are `'a'` and `'c'` on SQLite, DuckDB, Postgres and every other Tier-1 dialect

#### Scenario: All keys NULL
- **WHEN** a customer's rows all have a NULL `updated_at`
- **THEN** `first(status, updated_at)` and `last(status, updated_at)` are NULL for that customer

#### Scenario: NULL value under a non-NULL key
- **WHEN** the latest-by-`updated_at` row's `status` is NULL
- **THEN** `last(status, updated_at)` is NULL, not an earlier row's status

#### Scenario: Partitioned and cross-model
- **WHEN** a query rooted at `orders` selects `last(customers.status, customers.updated_at, partition_by=region)` over customers some of whose `updated_at` are NULL
- **THEN** each region's value is the status of its latest non-NULL-keyed customer, by executed values on SQLite and DuckDB
