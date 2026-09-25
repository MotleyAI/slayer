## Purpose

Defines what consuming a stored query-backed model as a relation means: its stages are spliced into
the consuming statement, which renders as one flat `WITH` with the same values and result keys as
the explicit splice.

## ADDED Requirements

### Requirement: Consuming a query-backed model equals splicing its stages
Consuming a stored query-backed model as a relation — as the `source_model`, as a stage's
source, or as a join / cross-model target — SHALL return the same values and result keys as the
same statement with that model's stages written into the query list as named stages. Its final
stage SHALL be readable under the model's own name; its private stage names SHALL stay local to it.
The same model consumed from several places SHALL be spliced once.

#### Scenario: Parity as the source
- **WHEN** a query's `source_model` is a stored multi-stage query-backed model
- **THEN** its rows and result keys equal the explicit splice and the hand-computed values, on SQLite and DuckDB

#### Scenario: Parity as a stage source
- **WHEN** a non-root stage's source is a stored query-backed model
- **THEN** its rows and result keys equal the explicit splice

#### Scenario: Parity as a join target
- **WHEN** a stored model's join targets a stored query-backed model and a query reads a column through that hop, or aggregates across it
- **THEN** its rows and result keys equal the explicit splice

#### Scenario: A model consumed twice
- **WHEN** two stages of one statement read the same stored multi-stage query-backed model
- **THEN** the statement executes and returns the hand-computed values

#### Scenario: Two models sharing a private stage name
- **WHEN** one statement consumes two stored query-backed models that each have a private stage named `x`, alongside a user stage also named `x`, and all three are read
- **THEN** the statement executes and returns the hand-computed values

#### Scenario: Dependency through a join target only
- **WHEN** a spliced stage depends on another only through `joins[].target_model` and the input list is supplied in reverse order
- **THEN** it executes and returns the hand-computed values

### Requirement: One flat WITH
A statement consuming query-backed models SHALL render as one statement with a single top-level
`WITH` and no `WITH` nested inside any CTE or subquery. CTEs originating in an embedded statement
(a model's own `sql` containing `WITH`) SHALL be hoisted with scope-correct renaming, so no two
statements' private CTE names collide and nested shadowing is preserved.

#### Scenario: No nested WITH
- **WHEN** a statement consumes a stored multi-stage query-backed model, in every supported dialect
- **THEN** the emitted SQL has exactly one `WITH`, at the top level

#### Scenario: Embedded WITH is renamed scope-correctly
- **WHEN** a `sql`-backed model whose SQL contains `WITH` is read by two stages, or its SQL reuses a CTE name in a nested scope, as a table alias, or recursively
- **THEN** the statement executes and returns the hand-computed values

### Requirement: Metadata flows through a spliced model
A spliced query-backed model SHALL expose the same default time dimension, column labels,
descriptions, formats and grain to its consumer as the explicit splice.

#### Scenario: Metadata parity
- **WHEN** a consumer reads a query-backed model whose final stage has a time dimension and labelled, formatted columns
- **THEN** time defaulting, response column metadata and join cardinality match the explicit splice

### Requirement: Variables layer lexically across nesting
Each spliced stage SHALL resolve query variables from, lowest to highest precedence: its source
model's defaults, the enclosing query-backed models innermost to outermost, the outer query, the
stage itself, and runtime values. The same model consumed from contexts whose effective variables
disagree on a placeholder it uses SHALL fail closed with an error naming the model.

#### Scenario: Each layer overrides the one below
- **WHEN** a placeholder is defined with conflicting values at each layer in turn
- **THEN** the highest layer present wins

#### Scenario: Conflicting contexts fail closed
- **WHEN** one statement consumes the same query-backed model from two contexts whose variables disagree on a placeholder the model uses
- **THEN** it fails with an error naming the model

### Requirement: Extensions over a spliced model
A root `ModelExtension` over a stored query-backed model SHALL apply its columns and joins exactly
once. A stage-level `ModelExtension` adding measures over a query-backed model SHALL stay rejected.

#### Scenario: Root extension applies once
- **WHEN** a query's source is a `ModelExtension` adding a column and a join over a stored query-backed model
- **THEN** the column and the join are each present once and the values match the explicit splice

#### Scenario: Stage-level measure extension stays rejected
- **WHEN** a non-root stage's source is a `ModelExtension` adding measures over a stored query-backed model
- **THEN** it is rejected as before

### Requirement: Warnings raised inside a spliced model are visible
Every warning a spliced model's stages raise (a to-many broadcast, a stale spelling, any other
typed warning) SHALL reach the consuming query's response, naming the stage by its private name and
the query-backed model it belongs to; the consumer's own warnings SHALL be unchanged.

#### Scenario: A broadcast inside a query-backed model is visible
- **WHEN** a stored query-backed model's stage broadcasts across a to-many join and a query consumes the model
- **THEN** the response carries that broadcast warning, labelled with the stage and the model

#### Scenario: Consumer warnings unchanged
- **WHEN** a consumer query that itself warns reads a query-backed model
- **THEN** its own warnings equal those of the explicit splice

### Requirement: Query-backed cycles are rejected
A query-backed model that references itself directly or transitively SHALL raise one cycle error,
naming the ordered cycle path, whether the cycle is reached by executing a consumer, by running a
model by name, by `save_model`, or by column-type discovery. No cached SQL SHALL be used to break a
cycle.

#### Scenario: Direct and transitive cycles
- **WHEN** `A` consumes `A`, or `A` consumes `B` which consumes `A`, and a consumer of `A` executes, `A` runs by name, `A` is saved, or `A`'s column types are requested
- **THEN** each raises the same cycle error naming the path
