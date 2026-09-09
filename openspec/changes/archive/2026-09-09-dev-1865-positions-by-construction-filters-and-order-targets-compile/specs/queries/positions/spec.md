# queries/positions Delta

## Purpose

Defines how filter and order-target expressions are typed and evaluated: every expression in a query is either a field (row-level) or a measure (an aggregation expression legal in the same query), and positions — returned, masked on, sorted by — differ only in what happens to the value.

## ADDED Requirements

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
