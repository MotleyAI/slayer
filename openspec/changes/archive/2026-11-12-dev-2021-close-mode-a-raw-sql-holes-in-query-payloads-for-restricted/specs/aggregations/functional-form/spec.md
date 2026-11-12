## MODIFIED Requirements

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
