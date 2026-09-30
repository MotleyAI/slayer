## ADDED Requirements

### Requirement: A saved query's inferred populations are pinned at save

Saving a query-backed model — through `save_model`, `create_model_from_query`, or an edit of its
`source_queries` on any surface — SHALL store every stage that omits `source_model` with its
inferred population written as its `source_model`; stages naming `source_model` SHALL be stored as
written. A stage whose inferred population model shares its name with a stage of the same saved
query SHALL be refused at save with an error naming the stage and advising to rename it. A stored
query-backed model with an unpinned stage (saved before pinning) SHALL, when run by name with a
refinement, first pin that stage's population as inferred from the unrefined stage, refusing the
same name collision; run without a refinement it SHALL execute as before.

#### Scenario: Pinned on save

- **WHEN** a query-backed model is saved whose stages omit `source_model`
- **THEN** the stored stages carry the inferred models as `source_model`, and running it by name returns the same rows as before saving

#### Scenario: A refinement never moves the population

- **WHEN** a saved query whose stage omitted `source_model` runs with a refinement whose extra dimension would, written into an unpinned query, infer a different population
- **THEN** the refined run keeps the pinned population

#### Scenario: Name collision refused at save

- **WHEN** a saved query's stage omitting `source_model` infers a model named like another stage of the same saved query
- **THEN** the save fails with an error naming the stage and advising to rename it

#### Scenario: Legacy unpinned model refined

- **WHEN** a stored query-backed model saved without pinned stages runs by name with a refinement
- **THEN** the population is the one inferred from the unrefined stage
- **WHEN** it runs by name without a refinement
- **THEN** it returns the same result as before this change
