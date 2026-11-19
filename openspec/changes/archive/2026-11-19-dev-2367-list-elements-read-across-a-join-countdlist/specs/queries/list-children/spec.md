## MODIFIED Requirements

### Requirement: Association across a list hop needs no key

When the mode is omitted or `to_many_handling: associate`, an aggregate homed on `M` SHALL, for each cell, aggregate over the `M` rows associated with the cell's element values, each `M` row once per cell, and SHALL NOT require a key on `M` when every unattributable dimension is reached through list hops of `M`'s own rows. When an unattributable dimension is reached through any other to-many hop, association SHALL deduplicate by `M`'s primary key, else by its per-query row identity (per `queries/attribution-modes`). When the home is the list child `M.L` itself and an unattributable dimension is reached across another to-many hop — from any population — association SHALL tell its rows apart by their `M` row (its primary key, else its per-query row identity) and their position in the list, so each element counts once per cell it reaches and repeated elements count separately. The position SHALL be read inside `M`'s own row, numbered once per `M` row after NULL elements are dropped and before any filter applies, and SHALL never appear in results.

#### Scenario: Contractors per skill
- **WHEN** a query on keyless `contractors` has the dimension `skills.value`, the measure `count(*)` and `to_many_handling: associate`
- **THEN** it returns `go` 2 and `sql` 2 with an associated warning

#### Scenario: Identical parent rows stay distinct
- **WHEN** `contractors` holds two identical rows with skills `['go']` and the same query runs
- **THEN** `go` counts both rows

#### Scenario: Elements per assignment
- **WHEN** `assignments` (ids 1–6, of contractors Ann, Ann, Bob, Cid, Eve and a missing contractor 9) joins `contractors` many-to-one, and a query on `assignments` with the dimension `id` measures `count(contractors.skills.*)`, alone or alongside `contractors.team` as a dimension
- **THEN** it returns 1: 3, 2: 3, 3: 1, 4: 0, 5: 1, 6: 0 — Ann's repeated `go` counted twice — by executed values

#### Scenario: Saved element metrics read across the join
- **WHEN** `contractors` saves `n_skills` as `count(skills.*)` and `n_go` as `count(skills.*) FILTER (WHERE skills.value = 'go')`, and a query on `assignments` with the dimension `id` measures both
- **THEN** `n_skills` returns 1: 3, 2: 3, 3: 1, 4: 0, 5: 1, 6: 0 and `n_go` returns 1: 2, 2: 2, 3: 0, 4: 0, 5: 1, 6: 0, equal to the inline spellings, by executed values

#### Scenario: Element collect per assignment
- **WHEN** a query on `assignments` with the dimension `id` measures `collect(contractors.skills.value)`
- **THEN** assignments 1 and 2 return `['go', 'sql']`, 3 returns `['sql']`, 5 returns `['go']`, and 4 and 6 return `[]`

#### Scenario: Elements per assignment from the parent's population
- **WHEN** a query on `contractors` has the dimension `assignments.id` and measures `count(skills.*)`
- **THEN** it returns 1: 3, 2: 3, 3: 1, 4: 0, 5: 1 and 0 in the NULL cell of the contractor without an assignment, by executed values

#### Scenario: Keyless parent
- **WHEN** `contractors` declares no primary or unique key and the elements-per-assignment query runs
- **THEN** it returns the same values

#### Scenario: Identical keyless parent rows keep their elements apart across a join
- **WHEN** a keyless dataset holds two identical rows with skills `['go']`, a joined dataset reaches both rows from one row of its own, and a query on the joined dataset grouped by that row counts the list elements
- **THEN** the cell counts 2

#### Scenario: Positions do not depend on filters
- **WHEN** `contractors` carries the model filter `team = 'A'`, or a query on `assignments` measures two element aggregates with different `FILTER` conditions over `contractors.skills` in one query
- **THEN** only the kept parent rows' elements contribute, each element once per cell, and each aggregate equals its value when queried alone, by executed values

#### Scenario: The position renders on every native-list dialect
- **WHEN** the elements-per-assignment query is rendered for Postgres, DuckDB, BigQuery, Snowflake, ClickHouse and Trino
- **THEN** no statement joins `contractors` to itself, each element is expanded once with its position, and the DuckDB and Postgres executions return the stated values, including over lists holding NULL elements and repeated values
