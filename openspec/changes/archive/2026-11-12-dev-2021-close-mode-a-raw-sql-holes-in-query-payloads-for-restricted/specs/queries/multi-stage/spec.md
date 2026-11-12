## MODIFIED Requirements

### Requirement: Stages are ordered by every sibling reference
A multi-stage query list SHALL be ordered so each stage follows every sibling it
references — as its `source_model`, as an extension's `source_name`, or as a
`joins[].target_model` of one of its extensions —
regardless of the order the non-root stages were supplied in. The last entry SHALL stay
the root. Duplicate stage names, an unnamed non-root stage, a self-reference, a non-root
stage referencing the root, and a cycle SHALL each be rejected with an error
naming the offending stages.

#### Scenario: A stage joining a sibling that reads another sibling
- **WHEN** the list `[x (source customers), c (source x), b (source orders, extended with a join to c), root (source b)]` is executed
- **THEN** it returns the hand-computed result instead of failing to resolve `c`

#### Scenario: Supply order of non-root stages does not matter
- **WHEN** the non-root stages of that list are supplied in any permutation, root last
- **THEN** every permutation returns the same result

#### Scenario: Sibling reached through an inline model
- **WHEN** a stage's source is an inline model whose joins reference a sibling stage
- **THEN** the list is rejected at construction with the inline-model error, and the sibling is reached instead through an extension of a named source joining it

#### Scenario: Sibling referenced inside nested `source_queries`
- **WHEN** a stage's source is an inline model carrying `source_queries` (which could only reach a sibling through nested `source_queries`)
- **THEN** the query is rejected at construction with the inline-model error, with a hint to write those queries as named stages

#### Scenario: One stage reading several siblings
- **WHEN** a stage's source is one sibling and its extensions join two other siblings, or extend a third
- **THEN** it is ordered after all of them and executes correctly

#### Scenario: Malformed lists are rejected
- **WHEN** a list has an unnamed non-root stage, a stage referencing itself, a non-root stage referencing the root, or a cycle
- **THEN** it is rejected with an error naming the offending stages

### Requirement: Stage names are query-local
A stage name SHALL be visible only within its own query list, where a name the
query writes (`source_model`, an extension's `source_name`, the `joins[].target_model`
of an extension) SHALL resolve to the stage in preference to a
same-named model. A stored definition — a stored model's joins, a stored
query-backed model's `source_queries` — SHALL never see a stage name: its
references resolve against stored models (and, inside a stored query-backed model,
its own private stages) only. A stage name SHALL NOT capture a physical table the
statement reads. Result keys, warnings and errors SHALL spell stages by the user's
name only.

#### Scenario: A stored join resolves to the model beside a same-named stage
- **WHEN** a list has a stage named `customers` (over `orders`) and a stage over `orders` asks for `customers.tier` through `orders`' stored join to the model `customers`
- **THEN** `customers.tier` reads the model and the result equals the hand-computed amount by tier

#### Scenario: A query-written join to a stage wins over a same-named stored edge
- **WHEN** a stage extends `orders` with a join to the stage `customers` while `orders` also stores an edge to the model `customers`, and asks for `customers.<column>`
- **THEN** the hop reaches the stage

#### Scenario: A stage named like a physical table does not hide it
- **WHEN** a stage is named after the physical table of a model that another stage reaches through a stored join
- **THEN** that stage reads the table, and the result equals the hand-computed value

#### Scenario: A stored query-backed model's private stage names are local
- **WHEN** a stored query-backed model has a private stage named like a storage model that one of its stages reaches through a stored join
- **THEN** the join reads the storage model
- **WHEN** a consumer query references a stored query-backed model's private stage name
- **THEN** it does not reach the private stage

#### Scenario: User spelling in results and messages
- **WHEN** any multi-stage query executes, warns or fails
- **THEN** its result keys, warnings and errors name stages by the user's spelling and never show an internal identity

#### Scenario: An inferred population is never a stage
- **WHEN** a rootless stage's population is inferred to a model that shares its name with a stage in the list
- **THEN** the stage reads the model

## REMOVED Requirements

### Requirement: Inline query-backed sources are rejected
**Reason**: Every inline model is rejected (`queries/population` › A population is named, and typed at construction), so the narrower rule for inline query-backed sources and its carve-out for inline table-backed and `sql`-backed models are obsolete; a stored document holding any inline source is unloadable (`models/stored-upgrade` › Stored extension columns are inlined).
**Migration**: Name a stored model or stage in `source_model`; write an inline model's queries as named stages of the query list.
