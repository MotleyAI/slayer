## MODIFIED Requirements

### Requirement: Association across a list hop needs no key

With `to_many_handling: associate`, an aggregate homed on `M` SHALL, for each cell, aggregate over the `M` rows associated with the cell's element values, each `M` row once per cell, and SHALL NOT require a key on `M` when every unattributable dimension is reached through list hops of `M`'s own rows. When an unattributable dimension is reached through any other to-many hop, association SHALL deduplicate by `M`'s primary key, else by its per-query row identity (per `queries/attribution-modes`).

#### Scenario: Contractors per skill
- **WHEN** a query on keyless `contractors` has the dimension `skills.value`, the measure `count(*)` and `to_many_handling: associate`
- **THEN** it returns `go` 2 and `sql` 2 with an associated warning

#### Scenario: Identical parent rows stay distinct
- **WHEN** `contractors` holds two identical rows with skills `['go']` and the same query runs
- **THEN** `go` counts both rows
