# queries/partitioned-aggregates Delta

## MODIFIED Requirements

### Requirement: Row and combined attachment coexistence
A query SHALL support partitioned aggregates consumed inside computed dimensions and as measures simultaneously, whether they share the same partition set, use independent partition sets, or are the very same aggregate in both roles. Exception: a CROSS-MODEL partitioned aggregate in both roles requires its partition key among the query dimensions — without it the combined join-back has no host-side key after aggregation, and the query SHALL fail with a clear error rather than compute wrong values (keyless-grain support is tracked separately).

#### Scenario: Dimension banding and a partitioned measure together
- WHEN a query has a computed dimension banding `amount:sum(partition_by=city)` and the measure `amount:sum(partition_by=region)`
- THEN both the band and the measure are correct by executed values in one result

#### Scenario: Same aggregate in both roles
- WHEN the same partitioned aggregate appears inside a computed dimension and as a directly selected measure
- THEN both values are correct and consistent with each other

#### Scenario: ORDER BY the raw aggregate alongside a computed dimension using it
- WHEN a computed dimension bands `amount:sum(partition_by=city)` and `order` names the raw `amount:sum(partition_by=city)`
- THEN rows sort by the partition-grain value; ordering by the computed dimension's name instead sorts by the banded value; neither form raises an internal placeholder error

#### Scenario: Cross-model dual role without the partition key in the grain is rejected
- WHEN the same cross-model partitioned aggregate is consumed by a computed dimension and selected as a measure while its partition key is not among the query dimensions
- THEN the query fails with a clear error instead of silently mis-joining or broadcasting

### Requirement: Structurally identical producers render once
When several consumed aggregates resolve to the same producer — same source and root, same effective grain, same normalized aggregate set including per-measure filters and ranking/window kernel context, the same inherited row-filter context, and recursively identical nested producers (a producer whose sub-plan embeds other producers matches only one whose embedded producers are identical by this same rule) — the query SHALL compute that producer once and attach it at every consuming position: across both attach phases (dimension and measure roles) and across nesting scopes, including a producer consumed both at the top level and inside another producer's sub-plan. Producers that differ in any part of that specification — a different inherited filter, a different frame-bound rewrite, a different window duration, measure-level filter, ranking column, or a differing nested sub-plan producer — MUST stay separate. Sharing a producer MUST NOT change, duplicate, or drop any response warning: each warning surfaces once per semantic event regardless of how many scopes consume the producer.

#### Scenario: Same aggregate in both roles shares one producer
- WHEN the same partitioned aggregate appears inside a computed dimension and as a selected measure
- THEN the emitted SQL contains a single producer relation for it, with both roles' values correct

#### Scenario: Different producer inputs stay separate
- WHEN two aggregations differ in window duration, in a measure-level filter, or in an explicit ranking column
- THEN they render as separate producers and each value is correct

#### Scenario: A producer shared between a nested sub-plan and the top level renders once
- WHEN a query groups by a dimension banding `amount:sum(partition_by=city)` and selects `amount:sum(window='1y')`, so the city-total producer is needed by both the base's computed dimension and the windowed producer's sub-plan
- THEN the emitted SQL contains exactly one city-total producer relation, referenced from both scopes, with executed values unchanged

#### Scenario: Differing inherited filter context prevents merging
- WHEN two structurally identical aggregates are consumed in scopes that inherit different row-filter conjuncts into their producers
- THEN each scope keeps its own producer relation and each consumer's executed values are correct

#### Scenario: Consumers at different depths keep their own attach coordinates
- WHEN one shared producer is consumed at two nesting depths whose attach join keys differ in coordinates
- THEN each consumer joins the shared relation on its own keys and both executed values are correct

#### Scenario: A shared producer's warning surfaces once
- WHEN a producer that triggers a broadcast or dropped-filter warning is consumed from two scopes
- THEN the response carries that warning exactly once per semantic event
