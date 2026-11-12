## MODIFIED Requirements

### Requirement: A dimension value reads the dimension's grouped value in every position
A sub-expression of a measure, an order target or a filter that is equal to an entire query dimension — a plain, joined or stage column, or a computed dimension's whole expression, including a partitioned aggregate, a re-aggregation or a transform — SHALL evaluate as that dimension's value in the result cell, in every position and on every supported dialect — except a transform-bearing computed dimension in measure position, which keeps its query-grain evaluation. The same expression SHALL yield the same value as a measure, a measure-typed filter and an order target. A reference inside an aggregation's source, arguments or partition keys SHALL remain a row-level input of that aggregation, never the dimension's grouped value.

#### Scenario: A plain dimension combined with an aggregate
- **WHEN** a `sales` query over dimensions `[quantity]` declares the measure `quantity * count(*)`
- **THEN** the measure is 4, 12, 9, 4, 5 for quantities 1 through 5; ordering by `quantity * count(*)` descending puts quantity 2 first and 3 second; and the filter `quantity * count(*) > 5` keeps exactly quantities 2 and 3

#### Scenario: A conditional over a dimension
- **WHEN** a `sales` query over dimensions `[region]` declares the measure `iif(region = 'North', sum(amount), 0)`
- **THEN** the measure is 90 for North and 0 for every other region

#### Scenario: A computed plain dimension combined with an aggregate
- **WHEN** a `sales` query over dimensions `[region, q2]`, with `q2` = `quantity * 2`, declares the measure `quantity * 2 + count(*)`
- **THEN** each row's measure equals its `q2` plus the row count of its cell

#### Scenario: A joined dimension combined with an aggregate
- **WHEN** a `corders` query over dimensions `[customers.regions.name]` declares `iif(customers.regions.name = 'North', sum(amount), 0)`, and another over `[customers.region_id]` declares `customers.region_id * sum(amount)`
- **THEN** the first is North 70 and South 0, the second is 70 for region 1 and 200 for region 2, and each expression orders and filters by the same values

#### Scenario: A stage-backed dimension combined with an aggregate
- **WHEN** a stage sums `amount` by `[region, city]` as `tot` and the next stage, over dimensions `[region]`, declares `iif(region = 'North', sum(tot), 0)`
- **THEN** the measure is 90 for North and 0 for every other region

#### Scenario: A dimension value inside an aggregation stays that aggregation's input
- **WHEN** a `sales` query over dimensions `[region, rd]`, with `rd` = `sum(amount, partition_by=region)`, declares the measure `sum(sum(amount, partition_by=region)) + sum(amount, partition_by=region)`
- **THEN** the outer sum re-aggregates over the partition cells exactly as it does without the dimension, the added term is `rd`, and the measure is North 180, South 280, East 360, Gap 40, Void NULL

#### Scenario: Measure, filter and order agree on a dimension value
- **WHEN** a measure-typed filter or an order target uses an expression over a dimension value that also executes as a declared measure in the same query
- **THEN** the filter keeps exactly the rows where the declared measure's predicate is TRUE and the order sorts by the declared measure's values
