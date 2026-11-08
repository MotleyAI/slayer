## ADDED Requirements

### Requirement: Grain-union broadcasting across consumption contexts
An arithmetic expression combining aggregates at different declared partition grains SHALL be well-defined at the union of those grains: each aggregate is computed at its own declared grain and broadcast over the grain keys it lacks. The expression SHALL be consumable at any grain refining the union — as a measure, the query grain — with every operand broadcast to the consuming rows; combining aggregates at different grains is never, by itself, an error. A transform over such an expression used as a measure SHALL evaluate at the query grain over the broadcast operands. Filter conjuncts referencing such expressions apply after attachment and MUST NOT alter surviving rows' values.

#### Scenario: Mixed-grain arithmetic as a measure
- WHEN a query over dimensions `[region, city]` selects the measure `a:sum(partition_by=region) - b:sum(partition_by=city)`
- THEN each row's value is its region total minus its city total, by executed values

#### Scenario: Same-grain partitioned arithmetic as a measure
- WHEN a query selects the measure `a:sum(partition_by=region) - b:sum(partition_by=region)` (the degenerate union of two identical grains)
- THEN each row's value is the difference of its two broadcast region totals, by executed values

#### Scenario: Plain and partitioned aggregates mix in one expression
- WHEN a query selects the measure `amount:sum - amount:sum(partition_by=region)`
- THEN each row's value is its query-grain total minus its broadcast region total, by executed values

#### Scenario: Transform over mixed-grain arithmetic as a measure
- WHEN a query selects the measure `rank(a:sum(partition_by=region) - b:sum(partition_by=city))`
- THEN result rows are ranked at the query grain by the broadcast difference, and adding the measure changes no other column's values

#### Scenario: Filter over mixed-grain arithmetic
- WHEN a query filters on `a:sum(partition_by=region) - b:sum(partition_by=city) > 0`
- THEN only qualifying rows remain and every surviving value equals the unfiltered query's value for that row
