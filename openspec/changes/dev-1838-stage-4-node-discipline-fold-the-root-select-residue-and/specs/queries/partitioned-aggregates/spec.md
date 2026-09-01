# queries/partitioned-aggregates Delta

## MODIFIED Requirements

### Requirement: Structurally identical producers render once
When several consumed aggregates resolve to the same producer — same source and root, same effective grain, same normalized aggregate set including per-measure filters and ranking/window kernel context, and the same inherited row-filter context — the query SHALL compute that producer once and attach it at every consuming position: across both attach phases (dimension and measure roles) and across nesting scopes, including a producer consumed both at the top level and inside another producer's sub-plan. Producers that differ in any part of that specification — a different inherited filter, a different frame-bound rewrite, a different window duration, measure-level filter, or ranking column — MUST stay separate. Sharing a producer MUST NOT change, duplicate, or drop any response warning: each warning surfaces once per semantic event regardless of how many scopes consume the producer.

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
