# queries/positions Specification

## Purpose
Defines how filter and order-target expressions are typed and evaluated: every expression in a query is either a field (row-level) or a measure (an aggregation expression legal in the same query), and positions — returned, masked on, sorted by — differ only in what happens to the value.

## Requirements

### Requirement: Position expressions type as field or measure
Every query-filter conjunct (after splitting a filter string on top-level AND) and every order target SHALL be typed as exactly one of: a **field** — aggregate-free after reference resolution and legal as a projected field expression — or a **measure** — legal as a declared measure in the same query, under all measure legality rules. Reference resolution SHALL use only the query's existing bindings (columns, dimensions, computed dimensions, and the row-level attached values that computed-dimension consumption creates) and MUST never synthesize new attachments; a resolved reference to a computed dimension's attached value counts as aggregate-free. An expression valid as both SHALL type as field.

#### Scenario: A plain dimension reference types as field
- **WHEN** a query over dimensions `[region]` with measure `amount:sum` filters on `region <> 'EU'`
- **THEN** the predicate masks host rows before aggregation, and each surviving group's aggregate equals the same query with the excluded rows absent from the source

#### Scenario: An aggregate expression types as measure
- **WHEN** the same query filters on `amount:sum > 100`
- **THEN** the predicate masks result cells after evaluation at query grain, and surviving cells keep the values the unfiltered query gives them

#### Scenario: A computed dimension's own aggregate resolves to its attached value and types as field
- **WHEN** a query declares a computed dimension banding `amount:sum(partition_by=city)` and filters on `amount:sum(partition_by=city) > 5000`
- **THEN** the filter applies per base row against the attached partition-grain value before re-aggregation, exactly as the same query behaved before this change

### Requirement: A typing failure names both failed typings
An expression valid as neither field nor measure SHALL fail with a clear typing error stating why field typing failed (naming the aggregate references) and why measure typing failed (naming the row-level references not available at query grain, or the failing measure legality rule) — never an internal error, and never a silently wrong result.

#### Scenario: Mixed-grain OR fails as a typing error
- **WHEN** a single OR predicate mixes a partitioned-aggregate reference with a row-level reference that is not a query dimension
- **THEN** the query fails with the typing error naming the aggregate that blocks field typing and the row-level reference that blocks measure typing

#### Scenario: Raw-rows queries have no measure position
- **WHEN** a query with `distinct_dimension_values: false` filters or orders on `amount:sum`
- **THEN** the query fails with a clear error: measure typing is unavailable because the query has no measure position

#### Scenario: An untypeable order target is rejected
- **WHEN** `order` names an expression valid as neither field nor measure in the query
- **THEN** the query fails with the same typing error, never a silently unsorted result

### Requirement: Mask and sort points follow the typing
A field-typed filter SHALL mask host rows before aggregation. A measure-typed filter SHALL mask result cells after the expression evaluates at query grain. A field-typed order target SHALL sort rows at row grain (raw-rows queries); a measure-typed order target SHALL sort result cells by the value evaluated at query grain. The masked or sorted value is computed exactly as the same expression would be in field/measure position.

#### Scenario: Field mask restricts the aggregated population
- **WHEN** a query with measure `amount:sum` filters on `status = 'ok'`
- **THEN** every aggregate is computed over only the passing rows

#### Scenario: Measure mask prunes cells only
- **WHEN** a query grouped by `[region]` filters on `amount:sum > 100`
- **THEN** failing groups are dropped and surviving groups' values are unchanged

### Requirement: Filter conjuncts stratify
Aggregate-free field-typed conjuncts whose references are all base-level SHALL define the row population that every producer and measure sees (stratum 0). A field-typed conjunct referencing an attached value SHALL mask rows at its attachment point, over the stratum-0 population, without feeding other producers. A measure-typed conjunct SHALL evaluate over the stratum-0 population and mask at query grain; adding a measure-typed filter MUST NOT change any surviving cell's values.

#### Scenario: Stratum-0 conjunct reaches every producer
- **WHEN** a query with a row-level filter `status = 'ok'` selects a partitioned aggregate, a windowed aggregate, and a cross-model aggregate
- **THEN** each producer's population contains only rows (or related rows, per the established cross-root propagation) passing the filter, by executed values

#### Scenario: Attached-value conjunct does not feed producers
- **WHEN** a query bands a computed dimension on `amount:sum(partition_by=city)`, filters on that same aggregate, and also selects `amount:sum(partition_by=[])`
- **THEN** the grand-total producer computes over the unmasked stratum-0 population while the re-aggregation sees only rows passing the attached-value predicate, by executed values

#### Scenario: Measure-typed filters are value-preserving
- **WHEN** any supported query adds a measure-typed filter — plain, partitioned, cross-model, or windowed
- **THEN** every surviving cell's values equal the unfiltered query's values for that cell

### Requirement: Position values agree with measure position
A filter's or order target's compiled value SHALL equal the value of the same expression declared as a measure (or projected as a field) in the same query. Masking keeps exactly the rows where the predicate value is TRUE — a FALSE or NULL predicate value drops the row.

#### Scenario: Twin-query law
- **WHEN** query Q filters on measure-typed predicate E, and query Q' instead declares E as a measure with no filter
- **THEN** Q's rows are exactly Q's rows in Q' where E is TRUE, with all shared column values identical

#### Scenario: NULL predicate values drop rows
- **WHEN** a filter predicate evaluates to NULL for a cell
- **THEN** that cell is dropped, exactly as SQL WHERE semantics dictate

### Requirement: Position support cannot lag measure support
Any expression legal as a measure in a query SHALL be legal as a filter conjunct and as an order target in that query, with mask/cell semantics per its typing and no position-specific support required.

#### Scenario: A measure-legal expression filters and orders
- **WHEN** an expression executes correctly as a declared measure in a query
- **THEN** the same expression is accepted as a filter and as an order target in that query, with the filter pruning cells by the same value and the order sorting by it

### Requirement: Hidden position values are invisible in results
An expression used only for filtering or ordering SHALL be computed but not returned: result columns, their order, and row values match the same query without the hidden expression. Generated SQL for shapes legal before this change SHALL stay byte-identical, except divergences individually approved and recorded.

#### Scenario: Filter-only and order-only expressions add no columns
- **WHEN** a query filters on one undeclared aggregate and orders by another
- **THEN** the response projects only the declared dimensions and measures, in declared order

#### Scenario: Golden baselines hold
- **WHEN** the golden-SQL suites for previously supported filter and order shapes run
- **THEN** every baseline matches byte-for-byte

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
