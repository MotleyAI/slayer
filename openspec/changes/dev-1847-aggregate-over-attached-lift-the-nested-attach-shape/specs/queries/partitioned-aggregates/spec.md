# queries/partitioned-aggregates delta

## ADDED Requirements

### Requirement: Re-aggregation consumes attached operands as datasets
A partitioned aggregate (or a composite of partitioned aggregates) SHALL be a
legal aggregation source: the outer aggregation consumes the operand dataset's
cells per `queries/semantics` › Second-order aggregation over attached values.
The outer aggregation SHALL support the plain scalar aggregation family —
`sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `median`,
parametric aggregations, and model-defined custom aggregations; `count` counts
the operand's cells with a non-null value and `count_distinct` its distinct
values. The outer aggregation MAY declare its own `partition_by=`, resolved by
the same attributability and mode rules as its default (query-grain) form, and
its value behaves as a normal attached value in every consumer context —
measure, arithmetic or transform input, ORDER BY target, filter-only reference,
and computed dimension (with an explicit outer grain, per the dimension
grain-self-containment rule). `first`/`last` over an aggregated first argument
keep their transform dispatch. The outer aggregation SHALL reject, with typed
errors naming the combination and the remedy: `window=` or ranked (`first`/
`last`) aggregation over an attached operand, and a measure-local `filter=` on
the outer aggregation.

#### Scenario: Count and parametric outer aggregations
- **WHEN** a query over `[region]` selects
  `count(sum(amount, partition_by=[city, region]))` and
  `percentile(sum(amount, partition_by=[city, region]), p=0.9)`
- **THEN** each region row carries the number of its city cells with a non-null
  total and the 0.9-percentile of those totals, by executed values

#### Scenario: Transform over a re-aggregated value
- **WHEN** a query selects `rank(avg(sum(amount, partition_by=[city, region])))`
- **THEN** result rows are ranked at the query grain by the attached
  re-aggregated value, and no other column's values change

#### Scenario: Explicit outer grain broadcasts per the combined rules
- **WHEN** a query over `[region, product]` selects
  `avg(sum(amount, partition_by=[city, region]), partition_by=region)`
- **THEN** the per-region average broadcasts across `product` exactly as any
  explicit-grain partitioned measure does, with no implicit-broadcast warning

#### Scenario: Filter on the re-aggregated value prunes only
- **WHEN** a query filters on `avg(sum(amount, partition_by=[city, region])) > 100`
- **THEN** only qualifying result rows remain and every surviving value equals
  the unfiltered query's value for that row

#### Scenario: Row-phase filters reach the inner producer
- **WHEN** the query carries a row-level filter conjunct
- **THEN** it restricts the inner producer's population per the established
  producer filter routing, and the re-aggregated value reflects it

#### Scenario: Outer window and outer filter fail closed
- **WHEN** a query selects `sum(sum(amount, partition_by=[city, region]), window='90d')`
  or gives the outer aggregation a measure-local `filter=`
- **THEN** each fails with a typed error naming the unsupported combination and
  the remedy — never a silently wrong value

#### Scenario: First and last keep transform dispatch
- **WHEN** a query selects `last(sum(amount, partition_by=[city, region]))`
- **THEN** it is the `last` transform over the aggregated series, unchanged

### Requirement: Nested producers compose to arbitrary depth
Producers SHALL nest to arbitrary depth: a producer's sub-plan may carry nested
producers that themselves carry producers, at any grain admitted by the
consumption seam (equality included). Every attach at every depth joins on the
nested producer's complete grain; structurally identical producers render once
across all depths and scopes; the emitted statement carries one flat `WITH`;
each warning surfaces once per semantic event regardless of consuming depth.

#### Scenario: Depth-three re-aggregation
- **WHEN** a query over `[product]` selects
  `max(avg(sum(amount, partition_by=[city, region, product]), partition_by=[region, product]))`
- **THEN** it executes with hand-computed values — per product, the maximum over
  regions of the average over cities — with one flat `WITH` and closed scopes

#### Scenario: One producer consumed at two depths renders once
- **WHEN** a computed dimension bands `sum(amount, partition_by=[city, region])`
  and a measure selects `avg(sum(amount, partition_by=[city, region]))`
- **THEN** the emitted SQL contains a single city-region producer relation,
  consumed by both the row attach and the outer producer, with correct values

### Requirement: Re-aggregation null, empty, and keyless cases are pinned
A NULL grain-key value SHALL form its own cell (null-safe grouping and
attachment). An outer cell whose operand cells all carry NULL values SHALL
yield NULL for value aggregations and 0 for `count`. A population cell with no
operand rows at all is governed by the population rules (no fabricated rows). A
keyless inner aggregate (`partition_by=[]`) forms a single-cell dataset; its
re-aggregation is the degenerate identity and follows the degenerate-warning
rule.

#### Scenario: Null grain component is one cell
- **WHEN** the inner grain contains a nullable column and rows with NULL exist
- **THEN** the NULL value forms exactly one inner cell, aggregated and attached
  null-safely, on every supported engine

#### Scenario: All-null operand values
- **WHEN** every inner cell of an outer cell has a NULL value
- **THEN** `avg`/`sum` yield NULL and `count` yields 0 for that cell, by
  executed values

#### Scenario: Keyless inner aggregate
- **WHEN** a query selects `avg(sum(amount, partition_by=[]))`
- **THEN** it executes as the global total (identity re-aggregation) with the
  degenerate-re-aggregation warning and no error

### Requirement: Partitioning by an attach-carrying computed dimension
An aggregate whose `partition_by=` names a computed dimension whose expression
itself contains an attached aggregate SHALL compile: the aggregate's producer
materializes the dimension's value (row-attaching the nested producer) and
groups by it. It SHALL be legal as a measure when the computed dimension is a
query dimension, and inside another computed dimension under the dimension
finer-grain exemption.

#### Scenario: Measure partitioned by a banded dimension
- **WHEN** a computed dimension `spend_band` bands
  `sum(amount, partition_by=[city, region])` and a measure selects
  `sum(revenue, partition_by=spend_band)` with `spend_band` a query dimension
- **THEN** the query executes with correct values and unchanged cardinality

#### Scenario: Nested inside another computed dimension
- **WHEN** a second computed dimension's expression contains an aggregate
  partitioned by `spend_band`
- **THEN** the query plans and executes without the nested-attach error

## MODIFIED Requirements

### Requirement: Combined-consumer partition keys are query dimensions
Every explicit partition key of a partitioned aggregate consumed in a combined position — as a non-dimension measure, inside an arithmetic / scalar-call composite or transform used as a measure, as a raw ORDER BY target, or as a filter-only reference — SHALL be a query dimension or a time dimension's source column (rewritten to its truncated bucket), for local and cross-model aggregates alike. A violation SHALL fail at plan time with a clear error naming the offending key and the remedy, never with an internal join-back failure. A partitioned aggregate consumed only inside computed dimensions or as a re-aggregation operand keeps the finer-grain exemption (its partition set declares an internal producer grain; the outer aggregation is the combined consumer and carries the rule). A filter or ORDER BY reference to a computed dimension's own aggregate is a row-scope reference, legal at any partition grain: such a filter restricts the aggregated population per base row at the partition grain, and MAY therefore change surviving groups' aggregate values — unlike a combined-scope partitioned-aggregate filter, which only prunes result rows.

#### Scenario: Keyless dual-role measure fails cleanly, local and cross-model alike
- WHEN the same partitioned aggregate — local or cross-model — is consumed by a computed dimension and selected as a measure while a partition key is not among the query dimensions
- THEN the query fails at plan time with the same clear error in both variants, naming the key and the remedy (add it to dimensions/time_dimensions), never with an internal error

#### Scenario: Keyless raw ORDER BY target fails cleanly
- WHEN `order` names the raw partitioned aggregate alongside a computed dimension using it and a partition key is not a query dimension
- THEN the query fails at plan time with the same clear partition-key error as the measure role, local and cross-model alike

#### Scenario: Composite and transform consumers are combined positions
- WHEN the keyless partitioned aggregate is consumed inside an arithmetic composite measure or as a transform input used as a measure
- THEN the query fails at plan time with the same clear partition-key error

#### Scenario: Dimension-only consumption keeps the finer-grain exemption
- WHEN a partitioned aggregate with partition keys finer than the query grain is consumed only inside computed dimensions (with row-scope filter or ORDER-BY-name references at most)
- THEN the query plans and executes without any partition-key error

#### Scenario: Re-aggregation operands keep the finer-grain exemption
- WHEN a partitioned aggregate whose partition keys are not query dimensions is consumed only as the operand of an outer aggregation
- THEN the query plans and executes without any partition-key error, and the outer aggregation's own explicit keys still carry the combined-consumer rule
