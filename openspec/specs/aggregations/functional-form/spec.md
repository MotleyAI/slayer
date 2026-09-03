# aggregations/functional-form Specification

## Purpose
Makes the functional aggregation spelling `agg(col, args)` a first-class, warning-free
equivalent of colon syntax `col:agg(args)` in every position where aggregations are
accepted, for all builtin, aliased, and custom aggregations.

## Requirements

### Requirement: Functional spelling is equivalent to colon spelling
For every aggregation expressible as `col:agg(args)`, the system SHALL accept
`agg(col, args)` as an exact equivalent: same generated SQL, same result values,
same result-column keys, and same error behavior for invalid combinations.

#### Scenario: Simple aggregation
- **WHEN** a query measure is written `sum(revenue)` instead of `revenue:sum`
- **THEN** the generated SQL and the result key `orders.revenue_sum` are identical to the colon form

#### Scenario: Star count
- **WHEN** a query measure is written `count(*)` instead of `*:count`
- **THEN** the result is identical to the colon form, with result key `orders._count`

#### Scenario: Cross-model star count
- **WHEN** a query measure is written `count(customers.*)` instead of `customers.*:count`
- **THEN** the result is identical to the colon form, with result key `orders.customers._count`

#### Scenario: Cross-model single column
- **WHEN** a query measure is written `count(customers.regions.name)` instead of `customers.regions.name:count`
- **THEN** the result is identical to the colon form, including the join-path result key

#### Scenario: Parametric aggregation with kwargs
- **WHEN** a measure is written `percentile(price, p=0.9)` instead of `price:percentile(p=0.9)`
- **THEN** SQL, result, and result key (`orders.price_percentile_p_0_9`) are identical to the colon form

#### Scenario: Reserved kwargs window and partition_by
- **WHEN** measures are written `sum(revenue, window='90d')` and `sum(revenue, partition_by=region)`
- **THEN** each behaves identically to its colon twin, including result-key suffixes

#### Scenario: Ranked aggregation with positional time column
- **WHEN** a measure is written `last(balance, updated_at)` instead of `balance:last(updated_at)`
- **THEN** the result is identical to the colon form

#### Scenario: Required-parameter aggregations
- **WHEN** measures are written `weighted_avg(price, weight=quantity)` and `corr(x, other=y)`
- **THEN** each behaves identically to its colon twin, and omitting a required parameter raises the same error as the colon form

#### Scenario: Invalid combination errors match
- **WHEN** `avg(*)` is submitted
- **THEN** it fails with the same error as `*:avg`

#### Scenario: Every builtin aggregation
- **WHEN** each builtin aggregation is written functionally over a compatible column
- **THEN** each is equivalent to its colon twin (parametrized over the full builtin set, not a hardcoded list)

### Requirement: Functional spelling works in every aggregation position
The system SHALL accept the functional spelling in every position that accepts
colon aggregations: query measures, query filters (both row-level and
post-aggregation phases), order, model measure formulas (saved via the API and
hand-authored in YAML storage), model-extension measures, inline source-model
measures, multi-stage source-query formulas, computed-dimension expressions,
and inside transform or arithmetic expressions (including mixed-grain
arithmetic over partitioned aggregates).

#### Scenario: Query filter routed to HAVING
- **WHEN** a query filter is written `sum(revenue) > 100`
- **THEN** it behaves identically to `revenue:sum > 100`

#### Scenario: Order by functional aggregation
- **WHEN** an order entry is written `sum(revenue)`
- **THEN** results are ordered as for `revenue:sum`, under the same result key `revenue_sum`

#### Scenario: Hand-authored YAML model measure
- **WHEN** a model whose measure formula is `sum(revenue)` is loaded from YAML storage without passing through save
- **THEN** queries against it succeed identically to a colon-form measure

#### Scenario: Model-extension and inline-model measures
- **WHEN** a `ModelExtension` measure or an inline `source_model` measure uses the functional spelling
- **THEN** the query succeeds identically to the colon form

#### Scenario: Multi-stage source-query formulas
- **WHEN** a stage formula in a `source_queries` pipeline uses the functional spelling
- **THEN** the stage behaves identically to the colon form

#### Scenario: Inside transforms and arithmetic
- **WHEN** a measure is written `cumsum(sum(revenue))` or `sum(revenue) / count(*)`
- **THEN** it behaves identically to `cumsum(revenue:sum)` and `revenue:sum / *:count`

#### Scenario: Cross-spelling rename and filter-form matching
- **WHEN** a measure is declared `{"formula": "sum(revenue)", "name": "rev"}` and a filter references `revenue:sum` (or vice versa)
- **THEN** the filter resolves to the same measure — spelling never affects matching

#### Scenario: Computed dimension with a functional partitioned aggregate
- **WHEN** a computed dimension is written with `sum(amount, partition_by=city)` in its expression (bare, banded via CASE, or under a transform) instead of `amount:sum(partition_by=city)`
- **THEN** the dimension behaves identically to the colon form, including result naming and grouping

#### Scenario: Computed-dimension guards fire for both spellings
- **WHEN** a computed dimension contains `sum(amount)` with no `partition_by=`
- **THEN** it fails with the same bare-aggregate-requires-partition_by error as the colon form

#### Scenario: Mixed-grain arithmetic with functional spellings
- **WHEN** a measure or filter combines aggregates at different partition grains written functionally (e.g. `sum(a, partition_by=region) - sum(b, partition_by=city)`)
- **THEN** it behaves identically to the colon-form mixed-grain expression

### Requirement: Aggregation-name healing applies to functional spelling
The system SHALL apply the same case-insensitive builtin matching and alias
healing to functional aggregation names as it applies to colon-form names.

#### Scenario: Uppercase builtin
- **WHEN** a measure is written `SUM(revenue)`
- **THEN** it behaves identically to `revenue:SUM` and `revenue:sum`

#### Scenario: Alias healing
- **WHEN** a measure is written `countD(user_id)`
- **THEN** it behaves identically to `user_id:count_distinct`

### Requirement: Unknown and custom aggregation names defer to binding
A function call whose first argument is aggregatable and whose name is not a
scalar function or transform SHALL be treated as an aggregation candidate and
validated at binding, exactly as colon-form names are: model-defined custom
aggregations resolve, and unknown names fail with the standard
unknown-aggregation error regardless of source shape (column, star, or
expression).

#### Scenario: Custom aggregation functional call
- **WHEN** a model defines a custom aggregation `my_agg` and a measure is written `my_agg(price)`
- **THEN** it behaves identically to `price:my_agg`

#### Scenario: Unknown name over a column
- **WHEN** a measure is written `bogus(price)`
- **THEN** binding fails with the same unknown-aggregation error as `price:bogus`

#### Scenario: Unknown name over star
- **WHEN** a measure is written `bogus(*)` or `*:bogus`
- **THEN** both fail with the standard unknown-aggregation error (not a downstream SQL-generation failure)

#### Scenario: Construction-time filter with custom functional aggregation
- **WHEN** a query containing the filter `my_agg(price) > 0` is constructed before any model context exists
- **THEN** construction succeeds and the name is validated later at binding

### Requirement: Ambiguous first and last names dispatch by argument shape
For `first` and `last` (both aggregation and transform names), a call whose
first argument contains no aggregation SHALL be an aggregation; a call whose
first argument is an aggregated expression SHALL be a transform.

#### Scenario: Aggregation reading
- **WHEN** a measure is written `last(balance)` or `last(balance, updated_at)`
- **THEN** it is the `last` aggregation, identical to `balance:last` / `balance:last(updated_at)`

#### Scenario: Transform reading
- **WHEN** a measure is written `last(revenue:sum)` or `last(sum(revenue))`
- **THEN** it is the `last` transform over the aggregated series

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
Entity-reference surfaces that accept colon-suffixed result-column references
(memories/search resolution, root-model recommendation) SHALL equally accept
the functional spelling of the same reference, interpreted by the same rules
as query parsing; multi-column expression text is not a valid entity reference.

#### Scenario: Functional entity reference
- **WHEN** an entity reference is written `sum(orders.revenue)` instead of `orders.revenue:sum`
- **THEN** it resolves to the same entity

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
Mode-A raw-SQL surfaces SHALL continue to treat `SUM(x)` as raw SQL, and
SQL-style `DISTINCT` inside a functional call SHALL remain a syntax error.

#### Scenario: Mode A unchanged
- **WHEN** a model column's `sql` contains `SUM(amount)`
- **THEN** it is passed through as raw SQL exactly as before

#### Scenario: DISTINCT keyword rejected
- **WHEN** a measure is written `count(distinct user_id)`
- **THEN** parsing fails (the supported spellings are `count_distinct(user_id)` / `user_id:count_distinct`)
