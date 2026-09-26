## ADDED Requirements

### Requirement: Self-referencing query-backed models are rejected at save time
Saving a query-backed model whose `source_queries` reference the model itself, directly or
through other query-backed models, SHALL fail with the query-backed cycle error naming the ordered
cycle path. **BREAKING**: such models previously saved through a cached-SQL short-circuit.

#### Scenario: Direct self-reference
- **WHEN** a query-backed model `A` whose stage sources `A` is saved
- **THEN** the save fails with the cycle error naming `A -> A`

#### Scenario: Transitive self-reference
- **WHEN** query-backed model `A` sources `B` and `B` sources `A`, and `A` is saved
- **THEN** the save fails with the cycle error naming the path
