# queries/partitioned-aggregates — delta

## MODIFIED Requirements

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
keep their transform dispatch. The outer aggregation's parameters — explicit,
positional, or defaulted by the aggregation definition — follow
`queries/semantics` › Aggregation parameters are typed by the home dataset's
grain against the operand dataset's grain: an aggregate grained within the
operand grain, or a column that grain determines, is picked once per cell and
read by the outer aggregation, and its partition keys are exempt from the
combined-consumer partition-key rule exactly as the source's constituents are;
a population-row column against a coarser cell grain, a definition default
naming such a column, or an aggregate grained outside the operand grain is a
typed error naming the parameter and the remedy. The outer aggregation SHALL
reject, with typed errors naming the combination and the remedy: `window=` or
ranked (`first`/`last`) aggregation over an attached operand, and a
measure-local `filter=` on the outer aggregation.

#### Scenario: Count and parametric outer aggregations
- **WHEN** a query over `[region]` selects
  `count(sum(amount, partition_by=[city, region]))` and
  `percentile(sum(amount, partition_by=[city, region]), p=0.9)`
- **THEN** each region row carries the number of its city cells with a non-null
  total and the 0.9-percentile of those totals, by executed values

#### Scenario: Operand-grain parameter executes
- **WHEN** a query over `[region]` selects
  `weighted_avg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`,
  and separately the custom `wavg(sum(amount, partition_by=[city, region]), weight=count(id, partition_by=[city, region]))`
  with the weight passed positionally
- **THEN** each region row carries the row-count-weighted average of its city totals,
  by hand-computed executed values on SQLite and DuckDB, the keyword and positional
  spellings identical, and the emitted SQL carries the parameter as a column of the
  operand carrier — one producer relation for the operand, no duplicate

#### Scenario: Parameter partition keys need not be query dimensions
- **WHEN** the outer aggregation's parameter is an aggregate grained at the operand grain
  and that grain's keys are not query dimensions
- **THEN** the query plans and executes without the combined-consumer partition-key
  error, exactly as the source's constituents are exempt

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

#### Scenario: Row filters bound the operand dataset's cells
- **WHEN** a row filter removes every operand row of a union-grain cell and the
  operand is consumed through a NULL-restoring composite (e.g. `coalesce(…, 0)`)
- **THEN** the operand dataset excludes that cell — the composite cannot
  fabricate it — by executed values

#### Scenario: Column-reference outer parameter fails closed
- **WHEN** the outer aggregation carries a parameter the operand grain does not
  determine — explicit (`wavg(sum(amount, partition_by=[city, region]), weight=id)`)
  or defaulted by its aggregation definition to such a column
- **THEN** it fails at plan time with a typed error naming the parameter, the grain,
  and the remedy — never invalid SQL, a render-time failure, or a silently wrong value

#### Scenario: Outer window and outer filter fail closed
- **WHEN** a query selects `sum(sum(amount, partition_by=[city, region]), window='90d')`
  or gives the outer aggregation a measure-local `filter=`
- **THEN** each fails with a typed error naming the unsupported combination and
  the remedy — never a silently wrong value

#### Scenario: First and last keep transform dispatch
- **WHEN** a query selects `last(sum(amount, partition_by=[city, region]))`
- **THEN** it is the `last` transform over the aggregated series, unchanged
