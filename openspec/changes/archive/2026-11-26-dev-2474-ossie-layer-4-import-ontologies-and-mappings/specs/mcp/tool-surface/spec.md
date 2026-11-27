## ADDED Requirements

### Requirement: The ontology tools fit the surface rules
The tools `create_ontology`, `edit_ontology`, `delete_ontology`, `create_ontology_mapping`, `edit_ontology_mapping` and `delete_ontology_mapping` SHALL obey every rule of this capability: their descriptions SHALL fit the client description budget, and every parameter SHALL be described in the input schema. `inspect`'s `entity_type` description SHALL list `ontology` and `ontology_mapping`.

#### Scenario: The ontology tools fit the budget
- **WHEN** the MCP server lists its tools
- **THEN** each ontology tool's description fits the budget, every parameter carries a description, and `inspect`'s `entity_type` description names `ontology` and `ontology_mapping`
