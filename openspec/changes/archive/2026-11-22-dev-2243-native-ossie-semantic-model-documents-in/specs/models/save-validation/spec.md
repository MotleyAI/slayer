## ADDED Requirements

### Requirement: Saves judge what they add or change
A save through SLayer SHALL apply its save-time checks to every definition the save adds or changes, and SHALL run the broken-dependents analysis over every stored definition that depends on what changed, as saves do today. A definition the save neither adds nor changes, and which depends on nothing the save changes, SHALL NOT be judged, so a stored definition that fails every query (allowed in a user-written document, `models/semantic-model-documents` › Entities SLayer cannot evaluate yet still load) never blocks an unrelated save. A definition SLayer is asked to add or change that is guaranteed to fail every query SHALL be refused, whether or not Ossie defines it.

#### Scenario: An unrelated edit beside a windowed metric
- **WHEN** a stored document holds a model-scoped metric using `OVER`, and `edit_dataset` changes the description of one of its datasets
- **THEN** the save succeeds and the windowed metric is unchanged

#### Scenario: Authoring a windowed metric is refused
- **WHEN** `edit_semantic_model` adds a model-scoped metric whose expression uses `OVER`
- **THEN** the save fails with a typed error naming the metric and the window function

#### Scenario: Renaming a referenced field warns about dependents
- **WHEN** a save renames the document field `orders.amount` that a model-scoped metric reads
- **THEN** the save reports the metric as a broken dependent, exactly as a save of a standalone dataset reports broken dependents today
