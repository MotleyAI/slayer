## ADDED Requirements

### Requirement: Search and validate_datasets describe their ontology use within the budget
The `search` tool's description of `entities` SHALL name the `concept:<Concept>` and `concept:<Ontology>.<Concept>` references, and the `validate_datasets` description SHALL state that it also reports ontology findings (identifier, multiplicity, constraint and required-link checks, and what is not checked), each within the client description budget.

#### Scenario: The descriptions fit and name the ontology use
- **WHEN** the MCP server lists its tools
- **THEN** `search`'s `entities` description names `concept:<Concept>`, `validate_datasets`' description names ontology findings, and both fit the budget
