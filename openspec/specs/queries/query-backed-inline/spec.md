# queries/query-backed-inline Specification

## Purpose
Defines what consuming a stored query-backed model as a relation means: its stages are spliced into
the consuming statement, which renders as one flat `WITH` with the same values and result keys as
the explicit splice.

## Requirements

### Requirement: Consuming a query-backed model equals splicing its stages
Consuming a stored query-backed model as a relation — as the `population`, as a stage's
source, or as a join / cross-model target — SHALL return the same values and result keys as the
same statement with that model's stages written into the query list as named stages. Its final
stage SHALL be readable under the model's own name; its private stage names SHALL stay local to it.
The same model consumed from several places SHALL be spliced once. A query-backed model is spliced
only when the statement reads it; reaching it through stored joins alone neither splices it nor
contributes its warnings.

#### Scenario: Parity as the source
- **WHEN** a query's `population` is a stored multi-stage query-backed model
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
- **WHEN** a spliced stage depends on another only through `relationships[].to` and the input list is supplied in reverse order
- **THEN** it executes and returns the hand-computed values

#### Scenario: Reaching is not reading
- **WHEN** a query-backed model's stage source stored-joins that model, or an unread (even broken, or warning) query-backed model sits in the join graph, and a consumer executes
- **THEN** it executes with the hand-computed values, reports no cycle, and the unread model contributes no SQL or warning

#### Scenario: Reads through stored joins at any depth
- **WHEN** a non-root stage, a derived column's definition, or another query-backed model's stage reads a query-backed model only through a stored join
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
A spliced query-backed model SHALL expose the same column labels, descriptions, formats and
grain to its consumer as the explicit splice. Its default time dimension SHALL be its final
stage's source-model default time dimension, as before splicing; a user stage has none.

#### Scenario: Metadata parity
- **WHEN** a consumer reads a query-backed model whose final stage has a time dimension and labelled, formatted columns
- **THEN** response column metadata and join cardinality match the explicit splice

#### Scenario: Time defaulting through a spliced model
- **WHEN** a consumer applies `last` with no time dimension of its own to a query-backed model whose final stage projects its source's default time dimension
- **THEN** that column ranks, with the same values as before splicing

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
An extension of a stored query-backed model SHALL apply its measures and joins exactly
once, to the model's spliced output, in every stage of a query list — whether the stage reads
the model as its source, as a join target or along a path — with the values an equivalent
extension of an explicit sibling stage holding the same queries produces.

#### Scenario: Root extension applies once
- **WHEN** a query's source is a stored query-backed model that the query extends with a measure and a join
- **THEN** the measure and the join are each present once and the values match the explicit splice

#### Scenario: Stage-level measure extension stays rejected
- **WHEN** a non-root stage reads a stored query-backed model and extends it with a measure that the stage selects
- **THEN** the stage executes, and the measure's value equals the same measure declared by an extension of an explicit sibling stage holding the model's queries

### Requirement: Warnings raised inside a spliced model are visible
Every warning a spliced model's stages raise (a to-many broadcast, a stale spelling, any other
typed warning) SHALL reach the consuming query's response, naming the stage by its private name and
the query-backed model it belongs to; the consumer's own warnings SHALL be unchanged.

#### Scenario: A broadcast inside a query-backed model is visible
- **WHEN** a stored query-backed model's stage under `to_many_handling: "broadcast"` broadcasts across a to-many join and a query consumes the model
- **THEN** the response carries that broadcast warning, labelled with the stage and the model

#### Scenario: Consumer warnings unchanged
- **WHEN** a consumer query that itself warns reads a query-backed model
- **THEN** its own warnings equal those of the explicit splice

### Requirement: Query-backed cycles are rejected
A query-backed model that references itself directly or transitively SHALL raise one cycle error,
naming the ordered cycle path, whether the cycle is reached by executing a consumer, by running a
model by name, by `save_dataset`, or by column-type discovery. No cached SQL SHALL be used to break a
cycle. A planning failure unrelated to a cycle SHALL keep its own error.

#### Scenario: Direct and transitive cycles
- **WHEN** `A` consumes `A`, or `A` consumes `B` which consumes `A`, and a consumer of `A` executes, `A` runs by name, `A` is saved, or `A`'s column types are requested
- **THEN** each raises the same cycle error naming the path

### Requirement: Saved relation relationships survive splicing

A stored query-backed model consumed as a relation SHALL retain that model's own declared relationships, with keys resolved against its final-stage output columns, alongside the edges its final stage inherits (`queries/multi-stage` › Stages inherit edges on pass-through keys), a declared relationship winning over an inherited one (`queries/multi-stage` › Declared edges win over inherited ones). Those relationships SHALL remain available when the relation is a population, a relationship target or an extended source. An ordinary query stage SHALL expose its output columns and the edges it inherits, and no other relationship of its backing source.

#### Scenario: Joined segment filter on a saved relation
- **WHEN** a saved customer-total relation declares a customer-key relationship and a query filters to gold customers
- **THEN** only the related gold customers contribute, with the same values as an equivalent relationship declared solely on a query extension, on SQLite and DuckDB

#### Scenario: Joined dimensions and aggregate attribution
- **WHEN** a query reads a dimension or aggregate through a saved relation's declared relationship
- **THEN** the relationship determines the rows and attribution instead of being treated as an unrelated dataset

#### Scenario: A saved relation consumed through another model
- **WHEN** another dataset relates to a saved query-backed relation and a query traverses onward through that relation's own relationship
- **THEN** the declared relationships remain available with logical key spelling and correct values

#### Scenario: Extending a saved relation
- **WHEN** a query extension adds a distinct named relationship to a saved relation
- **THEN** both its stored relationships and the extension's relationships remain available

#### Scenario: Repeating an existing saved relationship
- **WHEN** an extension over a saved query-backed relation repeats an exactly identical declared relationship
- **THEN** that edge is applied once, without introducing an ambiguous parallel path

#### Scenario: Legal join targets do not collide with temporary identities
- **WHEN** a saved relation declares a relationship to a legal dataset named `_stage`
- **THEN** both unjoined and joined consumers execute without a false self-join error

#### Scenario: Ordinary stage isolation
- **WHEN** an ordinary query stage is consumed by a later stage
- **THEN** its output columns and the edges its pass-through keys inherit are visible, and a relationship of its backing source whose key it does not pass through is not

#### Scenario: A saved relation inherits like a stage
- **WHEN** a saved query-backed model grouped by `orders.customer_id` declares no relationship and another query reads `customers.region` from it
- **THEN** it reaches `customers` through the inherited edge, with the values the same stages give as an explicit query list

### Requirement: Inspect lists a saved relation's inherited edges
`inspect` and the dataset summary of a saved query-backed model SHALL list, beside its
declared relationships, the edges its final stage inherits, each marked as inherited and
naming the source edge and the dataset it was declared on, in the same shape on MCP, REST
and the CLI. The listing SHALL be derived under the model's default variables and never
stored. When the model's stages cannot be planned — a cycle, an invalid key, a required
variable without a default, or any other planning error — inspection SHALL still succeed,
listing no inherited edge and carrying that typed error's message.

#### Scenario: Inherited edges are listed
- **WHEN** a saved query-backed model grouped by `orders.customer_id` is inspected
- **THEN** its relationships include the edge to `customers`, marked inherited from the `orders` → `customers` relationship

#### Scenario: A declared relationship absorbs the inherited one in the listing
- **WHEN** that model also declares the identical relationship to `customers`
- **THEN** inspection lists that edge once, as declared

#### Scenario: Planning failures do not fail inspection
- **WHEN** a saved query-backed model whose stages form a cycle, read an invalid key, or need a variable with no default is inspected
- **THEN** inspection succeeds, lists no inherited edge, and carries the typed error's message, on MCP, REST and the CLI alike
