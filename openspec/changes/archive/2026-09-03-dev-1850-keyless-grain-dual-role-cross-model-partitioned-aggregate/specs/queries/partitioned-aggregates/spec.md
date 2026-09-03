# queries/partitioned-aggregates Delta

## ADDED Requirements

### Requirement: Combined-consumer partition keys are query dimensions
Every explicit partition key of a partitioned aggregate consumed in a combined position — as a non-dimension measure, inside an arithmetic / scalar-call composite or transform used as a measure, as a raw ORDER BY target, or as a filter-only reference — SHALL be a query dimension or a time dimension's source column (rewritten to its truncated bucket), for local and cross-model aggregates alike. A violation SHALL fail at plan time with a clear error naming the offending key and the remedy, never with an internal join-back failure. A partitioned aggregate consumed only inside computed dimensions keeps the finer-grain exemption (its partition set declares an internal producer grain). A filter or ORDER BY reference to a computed dimension's own aggregate is a row-scope reference, legal at any partition grain: such a filter restricts the aggregated population per base row at the partition grain, and MAY therefore change surviving groups' aggregate values — unlike a combined-scope partitioned-aggregate filter, which only prunes result rows.

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

## MODIFIED Requirements

### Requirement: Row and combined attachment coexistence
A query SHALL support partitioned aggregates consumed inside computed dimensions and as measures simultaneously, whether they share the same partition set, use independent partition sets, or are the very same aggregate in both roles.

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
- THEN the query fails at plan time with the clear partition-key error of the combined-consumer requirement — identical in shape to the local variant — never with an internal join-back failure
