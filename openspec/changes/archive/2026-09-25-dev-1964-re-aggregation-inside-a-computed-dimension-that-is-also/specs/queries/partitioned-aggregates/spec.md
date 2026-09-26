## MODIFIED Requirements

### Requirement: Combined-consumer partition keys are query dimensions
Every explicit partition key of a partitioned aggregate consumed in a combined position — as a non-dimension measure, inside an arithmetic / scalar-call composite or transform used as a measure, as a raw ORDER BY target, or as a filter-only reference — SHALL be a query dimension or a time dimension's source column (rewritten to its truncated bucket), for local and cross-model aggregates alike. A violation SHALL fail at plan time with a clear error naming the offending key and the remedy, never with an internal join-back failure. A partitioned aggregate consumed only inside computed dimensions or as a re-aggregation operand keeps the finer-grain exemption (its partition set declares an internal producer grain; the outer aggregation is the combined consumer and carries the rule). A filter or ORDER BY reference to a computed dimension's own aggregate is a row-scope reference, legal at any partition grain: such a filter restricts the aggregated population per base row at the partition grain, and MAY therefore change surviving groups' aggregate values — unlike a combined-scope partitioned-aggregate filter, which only prunes result rows. Whether a filter conjunct or order target is a combined consumer SHALL be decided by its position typing (field vs measure) — a measure-typed conjunct that also references a computed dimension's own aggregate is a combined consumer of that aggregate. The rule SHALL apply identically to the explicit outer partition keys of a re-aggregation: a re-aggregation consumed only inside computed dimensions keeps the finer-grain exemption, and one consumed in any combined position carries the rule; the error SHALL name the consuming position (measure, filter or order), never a dimension.

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

#### Scenario: Measure-typed filter over a computed dimension's finer-grained aggregate fails cleanly
- WHEN a computed dimension bands `amount:sum(partition_by=[city, region])` over dimensions `[region, <band>]` and the filter `amount:sum(partition_by=[city, region]) < amount:sum` types as measure
- THEN the query fails at plan time with the partition-key error naming `city` — the same error as the query without the computed dimension — never with an internal join-back failure

#### Scenario: Re-aggregation in a computed dimension keeps the finer-grain exemption
- WHEN a query over dimensions `[region, x]` declares `x` = `CASE WHEN avg(sum(amount, partition_by=[city, region, product]), partition_by=[city, region]) > 30 THEN 'hi' ELSE 'lo' END`
- THEN the query executes, the re-aggregation is computed per `(city, region)` cell and broadcast onto its rows, and `amount:sum` by `(region, x)` is correct by executed values

#### Scenario: Re-aggregation outer keys carry the rule in every combined position
- WHEN the re-aggregation `avg(sum(amount, partition_by=[city, region, product]), partition_by=[city, region])` over dimensions `[region]` (with or without a computed dimension using it) is consumed as a measure, as a raw ORDER BY target, inside an arithmetic measure, inside a transform used as a measure, or in a measure-typed filter conjunct (including one produced by splitting an AND filter)
- THEN the query fails at plan time with the partition-key error naming `city` and the consuming position, never with an internal error

#### Scenario: Cross-model re-aggregation outer key carries the rule
- WHEN a cross-model re-aggregation declares an outer partition key that is not a query dimension and is consumed as a measure
- THEN the query fails at plan time with the partition-key error naming that key
