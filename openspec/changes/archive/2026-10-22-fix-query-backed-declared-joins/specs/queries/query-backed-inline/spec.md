## ADDED Requirements

### Requirement: Saved relation relationships survive splicing

A stored query-backed model consumed as a relation SHALL retain that model's own declared joins, with keys resolved against its final-stage output columns. Those relationships SHALL remain available when the relation is a source, a join target or an extended source. Ordinary query-stage outputs SHALL remain flat and SHALL NOT inherit joins from their backing source.

#### Scenario: Joined segment filter on a saved relation
- **WHEN** a saved customer-total relation declares a customer-key join and a query filters to gold customers
- **THEN** only the related gold customers contribute, with the same values as an equivalent relationship declared solely on a query extension, on SQLite and DuckDB

#### Scenario: Joined dimensions and aggregate attribution
- **WHEN** a query reads a dimension or aggregate through a saved relation's declared join
- **THEN** the relationship determines the rows and attribution instead of being treated as an unrelated dataset

#### Scenario: A saved relation consumed through another model
- **WHEN** another model joins a saved query-backed relation and a query traverses onward through that relation's own join
- **THEN** the declared relationships remain available with logical key spelling and correct values

#### Scenario: Extending a saved relation
- **WHEN** a query extension adds a distinct named join to a saved relation
- **THEN** both its stored joins and the extension's joins remain available

#### Scenario: Repeating an existing saved relationship
- **WHEN** an extension over a saved query-backed relation repeats an exactly identical declared join
- **THEN** that edge is applied once, without introducing an ambiguous parallel path

#### Scenario: Legal join targets do not collide with temporary identities
- **WHEN** a saved relation declares a join to a legal model named `_stage`
- **THEN** both unjoined and joined consumers execute without a false self-join error

#### Scenario: Ordinary stage isolation
- **WHEN** an ordinary query stage is consumed by a later stage
- **THEN** only its projected output schema is visible and its backing source's joins are not inherited
