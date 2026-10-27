## Purpose

Defines element access: each list column is a keyless one-to-many child of its model, reached as `<column>.value`, so filters, dimensions and aggregates over elements follow the ordinary query semantics without fanning parent values.

The examples use `contractors(id, team, rate, skills ARRAY<TEXT>)` with these rows:
- Ann: team A, rate 10, skills `['go', 'sql', 'go']`
- Bob: team A, rate 20, skills `['sql', NULL]`
- Cid: team B, rate 30, skills `[]`
- Dee: team B, rate 40, skills NULL
- Eve: team B, rate 50, skills `['go']`

## ADDED Requirements

### Requirement: A list column is a one-to-many child

A list column `L` of model `M` SHALL be reachable as a join-path hop `L` from `M` to a child dataset with one row per non-NULL element of each `M` row, repeated elements kept. The child's one column `L.value` SHALL have the column's `element_type`, and `count(L.*)` SHALL count child rows. Each child row SHALL determine its `M` row, so `M`'s columns are attributable from the child. No key on `M` SHALL be required.

#### Scenario: Counting element occurrences
- **WHEN** a query on `contractors` measures `count(skills.*)`
- **THEN** it returns 5 (Ann 3, Bob 1, Eve 1)

#### Scenario: Keyless model
- **WHEN** `contractors` declares no primary or unique key and a query filters `skills.value = 'go'`
- **THEN** the query runs and keeps Ann and Eve

### Requirement: Element filters restrict the parent rows

A field-typed filter over `L.value` SHALL keep the `M` rows that have at least one element satisfying it, without changing any kept row's values or count. `NOT (L.value = x)` SHALL keep the `M` rows having some element other than `x`; absence is `NOT contains(L, x)`.

#### Scenario: Restricting by an element
- **WHEN** a query measures `count(*)` and `sum(rate)` with the filter `skills.value = 'go'`
- **THEN** it returns 2 and 60, Ann counted once despite holding `go` twice

#### Scenario: Any of several elements
- **WHEN** the filter is `skills.value IN ('go', 'sql')`
- **THEN** `count(*)` is 3

#### Scenario: Negated element filter
- **WHEN** the filter is `NOT (skills.value = 'go')`
- **THEN** it keeps Ann and Bob, the rows having an element other than `go`

### Requirement: An element can be a dimension

`L.value` SHALL be usable as a dimension; the population is then the list child per the population rules. Aggregates over the child are counted per element; `M`'s aggregates follow the query's `to_many_handling`.

#### Scenario: Element occurrences per skill
- **WHEN** a query has the dimension `skills.value` and the measure `count(skills.*)`
- **THEN** it returns `go` 3 and `sql` 2

#### Scenario: Default broadcast of a parent measure
- **WHEN** a query has the dimension `skills.value` and the measure `count(*)` with the default `to_many_handling`
- **THEN** each skill row carries 5 and the response has a broadcast warning

### Requirement: Association across a list hop needs no key

With `to_many_handling: associate`, an aggregate homed on `M` SHALL, for each cell, aggregate over the `M` rows associated with the cell's element values, each `M` row once per cell, and SHALL NOT require a key on `M` when every unattributable dimension is reached through list hops of `M`'s own rows. When an unattributable dimension is reached through any other to-many hop, association SHALL keep requiring the existing unique key.

#### Scenario: Contractors per skill
- **WHEN** a query on keyless `contractors` has the dimension `skills.value`, the measure `count(*)` and `to_many_handling: associate`
- **THEN** it returns `go` 2 and `sql` 2 with an associated warning

#### Scenario: Identical parent rows stay distinct
- **WHEN** `contractors` holds two identical rows with skills `['go']` and the same query runs
- **THEN** `go` counts both rows

### Requirement: Element aggregates never fan out parent values

An aggregate over `L.value` and an aggregate over `M`'s own rows SHALL be computable in one query, each over its own home rows.

#### Scenario: Parent and element aggregates together
- **WHEN** a query has the dimension `team` and the measures `count(*)`, `sum(rate)` and `count_distinct(skills.value)`
- **THEN** team A returns 2, 30 and 2, and team B returns 3, 120 and 1

### Requirement: List children compose like any child

Element access SHALL work at the end of a to-one join path (`contractors.skills.value` from a model joined to-one to `contractors`), on derived list columns, and on list-typed columns of query stages, including a stage's `collect` result.

#### Scenario: Through a to-one join
- **WHEN** `assignments` joins `contractors` to-one and a query on `assignments` filters `contractors.skills.value = 'sql'`
- **THEN** it keeps the assignments of Ann and Bob

#### Scenario: A collected list in a later stage
- **WHEN** stage `s` groups `contractors` by `team` with `collect(skills.value)` named `team_skills`, and the next stage filters `contains(team_skills, 'go')`
- **THEN** both teams are kept, and `team_skills.value` is usable as a dimension of that stage

### Requirement: List column names do not collide with hop names

Saving a model SHALL fail when one of its list columns has the name of a hop reachable from that model — an edge name, or the name of a model one of its incident edges reaches. Saving a model whose new join creates such a hop SHALL fail the same way. The error SHALL name the column and the hop.

#### Scenario: A list column named like a joined model
- **WHEN** `contractors` has a list column `skills` and a join to a model named `skills`, and it is saved
- **THEN** the save fails naming `skills`

#### Scenario: The collision created from the other side
- **WHEN** `contractors` has a list column `skills` and a model `skills` is saved with a join to `contractors`
- **THEN** that save fails naming `contractors.skills`

### Requirement: Element access is emitted inside the parent's own rows

On every database with native lists, the list child SHALL be read from the parent's own row, in the same relation, never by joining the parent back on a key. The result SHALL be the same on Postgres, DuckDB, BigQuery, Snowflake, ClickHouse and Trino.

#### Scenario: Emission per dialect
- **WHEN** the query with the filter `skills.value = 'go'` and the dimension-and-count query above are rendered for every native-list dialect
- **THEN** none of the statements joins `contractors` to itself, and the DuckDB and Postgres executions return the stated values
