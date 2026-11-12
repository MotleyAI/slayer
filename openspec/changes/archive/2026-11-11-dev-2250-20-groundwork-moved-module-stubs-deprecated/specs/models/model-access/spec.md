## MODIFIED Requirements

### Requirement: Narrowed access is reported

A model's access is narrowed when it reads, transitively, a tagged model whose tags do not include all of its own (an untagged model reading a tagged one is narrowed). Saving a narrowed model SHALL emit a structured `narrowed_access` warning naming the models that narrow it. Saving a tagged model SHALL emit the warning for each model that becomes narrowed because it reads it. A save through MCP or REST SHALL return each such warning in its response. Model validation SHALL report every narrowed model. A narrowed model SHALL still save.

#### Scenario: Saving a public model that reads a tagged one warns

- **WHEN** an untagged query-backed model reading `hr` is saved
- **THEN** the save succeeds and emits a `narrowed_access` warning naming `hr`

#### Scenario: Retagging a model warns about its readers

- **WHEN** `orders` is read by an untagged model and `orders` is saved with `access_tags=["ops"]`
- **THEN** the save emits a `narrowed_access` warning naming the reader

#### Scenario: Model validation lists narrowed models

- **WHEN** model validation runs over a datasource holding a narrowed model
- **THEN** the report lists it as narrowed, naming the models that narrow it

#### Scenario: A REST model save returns the warning

- **WHEN** an untagged query-backed model reading `hr` is saved through `POST /models`, and again through `PUT /models/{name}`
- **THEN** each response carries a `warnings` list holding the structured `narrowed_access` warning naming `hr`
