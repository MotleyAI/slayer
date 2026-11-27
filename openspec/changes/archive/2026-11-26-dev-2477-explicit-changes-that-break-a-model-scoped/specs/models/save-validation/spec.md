## MODIFIED Requirements

### Requirement: Saves judge what they add or change
A save through SLayer SHALL apply its save-time checks to every definition the save adds or changes, and SHALL run the broken-dependents analysis over every stored definition that depends on what changed, as saves do today; a save that breaks a stored model-scoped metric SHALL be refused as `models/semantic-model-documents` › Explicit changes never break a model-scoped metric specifies. A definition the save neither adds nor changes, and which depends on nothing the save changes, SHALL NOT be judged, so a stored definition that fails every query (allowed in a user-written document, `models/semantic-model-documents` › Entities SLayer cannot evaluate yet still load) never blocks an unrelated save. A definition SLayer is asked to add or change that is guaranteed to fail every query SHALL be refused, whether or not Ossie defines it.

#### Scenario: An unrelated edit beside a windowed metric
- **WHEN** a stored document holds a model-scoped metric using `OVER`, and `edit_dataset` changes the description of one of its datasets
- **THEN** the save succeeds and the windowed metric is unchanged

#### Scenario: Authoring a windowed metric saves; an unwindowable function is refused
- **WHEN** `edit_semantic_model` adds a model-scoped metric whose expression uses `OVER`
- **THEN** the save succeeds and a query naming the metric returns its value; a metric windowing a function Ossie does not window (`last(orders.amount, orders.order_date) OVER ()`) fails the save with a typed error naming the metric and the function

#### Scenario: Renaming a referenced field warns about dependents
- **WHEN** a save renames the document field `orders.amount` that a model-scoped metric reads
- **THEN** the save is refused naming the metric and `orders.amount`, and nothing is stored
