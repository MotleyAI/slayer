## ADDED Requirements

### Requirement: Declared measures type at query grain
Every declared measure SHALL be typed at query grain: each row-level reference outside an aggregation SHALL be a query dimension's value (see "A dimension value reads the dimension's grouped value in every position"). A violation SHALL fail at plan time with the typing error naming the consuming position and the offending row-level references — never an internal error, a render error, or an internal placeholder name. A measure whose every row-level reference is a dimension value SHALL be legal whether or not it contains an aggregation, evaluating once per result cell. The typing error SHALL NOT pre-empt an error an existing measure rule raises for the same measure (partition keys, transform inputs, re-aggregation).

#### Scenario: A measure mixing a non-dimension column with an aggregate fails as a typing error
- **WHEN** a query over dimensions `[region]` declares the measure `amount + sum(amount)` named `m`
- **THEN** planning fails with the position typing error at `measure 'm'` naming the row-level reference `amount` as not available at the query grain, and the message contains no internal placeholder name

#### Scenario: An aggregate-free measure over a non-dimension column keeps the aggregation remedy
- **WHEN** a query over dimensions `[region]` declares the measure `round(amount, 2)`
- **THEN** planning fails with the position typing error naming `amount`, whose message says it needs an aggregation inside an expression and suggests wrapping it (e.g. `sum(amount)`)

#### Scenario: A raw time column under a bucketed time dimension is not a dimension value
- **WHEN** a query with a monthly time dimension on `ordered_at` declares a measure combining the raw `ordered_at` column with `count(*)`
- **THEN** planning fails with the position typing error naming `ordered_at`, exactly as the same expression fails as a filter

#### Scenario: A joined column that is not a dimension fails as a typing error
- **WHEN** a `corders` query over dimensions `[customers.regions.name]` declares the measure `customers.region_id * sum(amount)`
- **THEN** planning fails with the position typing error naming `customers.region_id`

#### Scenario: An aggregate-free measure over a dimension is legal
- **WHEN** a query over dimensions `[quantity]` declares the measure `quantity + 1`
- **THEN** the query executes and each row's measure equals its `quantity` plus 1

### Requirement: A dimension value reads the dimension's grouped value in every position
A sub-expression of a measure, an order target or a filter that is equal to an entire query dimension — a plain, joined or stage column, or a computed dimension's whole expression, including a partitioned aggregate, a re-aggregation or a transform — SHALL evaluate as that dimension's value in the result cell, in every position and on every supported dialect — except a transform-bearing computed dimension in measure position, which keeps its query-grain evaluation. The same expression SHALL yield the same value as a measure, a measure-typed filter and an order target. A reference inside an aggregation's source, arguments or partition keys SHALL remain a row-level input of that aggregation, never the dimension's grouped value.

#### Scenario: A plain dimension combined with an aggregate
- **WHEN** a `sales` query over dimensions `[quantity]` declares the measure `quantity * count(*)`
- **THEN** the measure is 4, 12, 9, 4, 5 for quantities 1 through 5; ordering by `quantity * count(*)` descending puts quantity 2 first and 3 second; and the filter `quantity * count(*) > 5` keeps exactly quantities 2 and 3

#### Scenario: A conditional over a dimension
- **WHEN** a `sales` query over dimensions `[region]` declares the measure `iif(region == 'North', sum(amount), 0)`
- **THEN** the measure is 90 for North and 0 for every other region

#### Scenario: A computed plain dimension combined with an aggregate
- **WHEN** a `sales` query over dimensions `[region, q2]`, with `q2` = `quantity * 2`, declares the measure `quantity * 2 + count(*)`
- **THEN** each row's measure equals its `q2` plus the row count of its cell

#### Scenario: A joined dimension combined with an aggregate
- **WHEN** a `corders` query over dimensions `[customers.regions.name]` declares `iif(customers.regions.name == 'North', sum(amount), 0)`, and another over `[customers.region_id]` declares `customers.region_id * sum(amount)`
- **THEN** the first is North 70 and South 0, the second is 70 for region 1 and 200 for region 2, and each expression orders and filters by the same values

#### Scenario: A stage-backed dimension combined with an aggregate
- **WHEN** a stage sums `amount` by `[region, city]` as `tot` and the next stage, over dimensions `[region]`, declares `iif(region == 'North', sum(tot), 0)`
- **THEN** the measure is 90 for North and 0 for every other region

#### Scenario: A dimension value inside an aggregation stays that aggregation's input
- **WHEN** a `sales` query over dimensions `[region, rd]`, with `rd` = `amount:sum(partition_by=region)`, declares the measure `sum(amount:sum(partition_by=region)) + amount:sum(partition_by=region)`
- **THEN** the outer sum re-aggregates over the partition cells exactly as it does without the dimension, the added term is `rd`, and the measure is North 180, South 280, East 360, Gap 40, Void NULL

#### Scenario: Measure, filter and order agree on a dimension value
- **WHEN** a measure-typed filter or an order target uses an expression over a dimension value that also executes as a declared measure in the same query
- **THEN** the filter keeps exactly the rows where the declared measure's predicate is TRUE and the order sorts by the declared measure's values
