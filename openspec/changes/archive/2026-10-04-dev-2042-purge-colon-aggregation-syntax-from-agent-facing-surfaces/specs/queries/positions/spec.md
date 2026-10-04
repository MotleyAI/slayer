## MODIFIED Requirements

### Requirement: Position expressions type as field or measure
Every query-filter conjunct (after splitting a filter string on top-level AND) and every order target SHALL be typed as exactly one of: a **field** — aggregate-free after reference resolution and legal as a projected field expression — or a **measure** — legal as a declared measure in the same query, under all measure legality rules. Reference resolution SHALL use only the query's existing bindings (columns, dimensions, computed dimensions, and the row-level attached values that computed-dimension consumption creates) and MUST never synthesize new attachments; a resolved reference to a computed dimension's attached value counts as aggregate-free. An expression valid as both SHALL type as field.

#### Scenario: A plain dimension reference types as field
- **WHEN** a query over dimensions `[region]` with measure `sum(amount)` filters on `region <> 'EU'`
- **THEN** the predicate masks host rows before aggregation, and each surviving group's aggregate equals the same query with the excluded rows absent from the source

#### Scenario: An aggregate expression types as measure
- **WHEN** the same query filters on `sum(amount) > 100`
- **THEN** the predicate masks result cells after evaluation at query grain, and surviving cells keep the values the unfiltered query gives them

#### Scenario: A computed dimension's own aggregate resolves to its attached value and types as field
- **WHEN** a query declares a computed dimension banding `sum(amount, partition_by=city)` and filters on `sum(amount, partition_by=city) > 5000`
- **THEN** the filter applies per base row against the attached partition-grain value before re-aggregation, exactly as the same query behaved before this change

### Requirement: A typing failure names both failed typings
An expression valid as neither field nor measure SHALL fail with a clear typing error stating why field typing failed (naming the aggregate references) and why measure typing failed (naming the row-level references not available at query grain, or the failing measure legality rule) — never an internal error, and never a silently wrong result.

#### Scenario: Mixed-grain OR fails as a typing error
- **WHEN** a single OR predicate mixes a partitioned-aggregate reference with a row-level reference that is not a query dimension
- **THEN** the query fails with the typing error naming the aggregate that blocks field typing and the row-level reference that blocks measure typing

#### Scenario: Raw-rows queries have no measure position
- **WHEN** a query with `distinct_dimension_values: false` filters or orders on `sum(amount)`
- **THEN** the query fails with a clear error: measure typing is unavailable because the query has no measure position

#### Scenario: An untypeable order target is rejected
- **WHEN** `order` names an expression valid as neither field nor measure in the query
- **THEN** the query fails with the same typing error, never a silently unsorted result

### Requirement: Mask and sort points follow the typing
A field-typed filter SHALL mask host rows before aggregation. A measure-typed filter SHALL mask result cells after the expression evaluates at query grain. A field-typed order target SHALL sort rows at row grain (raw-rows queries); a measure-typed order target SHALL sort result cells by the value evaluated at query grain. The masked or sorted value is computed exactly as the same expression would be in field/measure position.

#### Scenario: Field mask restricts the aggregated population
- **WHEN** a query with measure `sum(amount)` filters on `status = 'ok'`
- **THEN** every aggregate is computed over only the passing rows

#### Scenario: Measure mask prunes cells only
- **WHEN** a query grouped by `[region]` filters on `sum(amount) > 100`
- **THEN** failing groups are dropped and surviving groups' values are unchanged

### Requirement: Filter conjuncts stratify
Aggregate-free field-typed conjuncts whose references are all base-level SHALL define the row population that every producer and measure sees (stratum 0). A field-typed conjunct referencing an attached value SHALL mask rows at its attachment point, over the stratum-0 population, without feeding other producers. A measure-typed conjunct SHALL evaluate over the stratum-0 population and mask at query grain; adding a measure-typed filter MUST NOT change any surviving cell's values.

#### Scenario: Stratum-0 conjunct reaches every producer
- **WHEN** a query with a row-level filter `status = 'ok'` selects a partitioned aggregate, a windowed aggregate, and a cross-model aggregate
- **THEN** each producer's population contains only rows (or related rows, per the established cross-root propagation) passing the filter, by executed values

#### Scenario: Attached-value conjunct does not feed producers
- **WHEN** a query bands a computed dimension on `sum(amount, partition_by=city)`, filters on that same aggregate, and also selects `sum(amount, partition_by=[])`
- **THEN** the grand-total producer computes over the unmasked stratum-0 population while the re-aggregation sees only rows passing the attached-value predicate, by executed values

#### Scenario: Measure-typed filters are value-preserving
- **WHEN** any supported query adds a measure-typed filter — plain, partitioned, cross-model, or windowed
- **THEN** every surviving cell's values equal the unfiltered query's values for that cell

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
- **WHEN** a `sales` query over dimensions `[region, rd]`, with `rd` = `sum(amount, partition_by=region)`, declares the measure `sum(sum(amount, partition_by=region)) + sum(amount, partition_by=region)`
- **THEN** the outer sum re-aggregates over the partition cells exactly as it does without the dimension, the added term is `rd`, and the measure is North 180, South 280, East 360, Gap 40, Void NULL

#### Scenario: Measure, filter and order agree on a dimension value
- **WHEN** a measure-typed filter or an order target uses an expression over a dimension value that also executes as a declared measure in the same query
- **THEN** the filter keeps exactly the rows where the declared measure's predicate is TRUE and the order sorts by the declared measure's values
