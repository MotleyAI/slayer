## ADDED Requirements

### Requirement: The semantic-model tools fit the surface rules
The tools `create_semantic_model`, `edit_semantic_model` and `delete_semantic_model` SHALL obey every rule of this capability: their descriptions SHALL fit the client description budget, and every parameter SHALL be described in the input schema. `edit_dataset` and `create_dataset` SHALL describe their `semantic_model` behaviour, and `delete_semantic_model` SHALL state that it deletes the document's datasets.

#### Scenario: The new tools fit the budget
- **WHEN** the MCP server lists its tools
- **THEN** each semantic-model tool's description fits the budget and every parameter carries a description

### Requirement: Query guidance names bare model-scoped metrics and AGG
The query tool's schema guidance SHALL state, once, on the field that owns metric references, that a bare name is a query alias or a model-scoped metric and that `AGG(dataset.metric)` evaluates a dataset metric; a help topic SHALL hold the long-form reference for model-scoped metrics, reachable from the server instructions.

#### Scenario: A documented bare-metric query works as documented
- **WHEN** the documented example query naming a model-scoped metric by bare name runs on the demo
- **THEN** it returns the documented result
