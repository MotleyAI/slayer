# queries/partitioned-aggregates Delta

## MODIFIED Requirements

### Requirement: Filters referencing partitioned aggregates
Query filters SHALL be able to reference partitioned aggregates. Such predicates type as measures: they apply after attachment, prune result rows, and MUST NOT alter the aggregate values of surviving rows. Each top-level conjunct of a filter types independently as a field (aggregate-free after reference resolution) or a measure (legal as a declared measure in the same query); a conjunct valid as neither fails with a clear typing error naming both failed typings.

#### Scenario: Keep rows whose partition total qualifies
- WHEN a query over dimensions `[region, city]` filters on `revenue:sum(partition_by=region) > 5000`
- THEN only rows belonging to qualifying regions remain and every remaining value equals the unfiltered query's value for that row

#### Scenario: Conjunction splits by scope
- WHEN one filter string is an AND of a partitioned-aggregate predicate and a row-level predicate
- THEN the results equal the same query with the two predicates given as separate filters

#### Scenario: Mixing with a plain aggregate in one predicate is legal
- WHEN a single predicate combines a partitioned-aggregate reference with a plain aggregate reference (e.g. `revenue:sum(partition_by=region) > 5000 AND revenue:sum > 100`)
- THEN the whole predicate evaluates after aggregation and attachment, and the results are correct by executed values

#### Scenario: Mixing a computed dimension's aggregate with a row-level reference is legal
- WHEN a query bands a computed dimension on `amount:sum(partition_by=city)` and one predicate combines that aggregate with a row-level reference (e.g. `amount:sum(partition_by=city) > 5000 AND status = 'ok'`)
- THEN the predicate types as field — both references resolve at row scope — and applies per base row before re-aggregation, correct by executed values, never the former split-the-conjuncts error

#### Scenario: Mixing a computed dimension's aggregate with a plain aggregate is legal
- WHEN the same query's predicate combines the computed dimension's aggregate with a plain aggregate (e.g. `amount:sum(partition_by=city) > 5000 AND amount:sum > 100`)
- THEN the predicate types as measure and masks result cells after evaluation at query grain, with surviving values unchanged, by executed values

#### Scenario: No common scope fails closed
- WHEN a single OR predicate mixes a partitioned-aggregate reference with a reference resolvable only before aggregation
- THEN the query fails with the typing error naming the aggregate that blocks field typing and the row-level reference that blocks measure typing — not with an internal error
