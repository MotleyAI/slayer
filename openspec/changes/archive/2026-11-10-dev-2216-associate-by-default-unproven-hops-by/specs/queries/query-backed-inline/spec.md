## MODIFIED Requirements

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
