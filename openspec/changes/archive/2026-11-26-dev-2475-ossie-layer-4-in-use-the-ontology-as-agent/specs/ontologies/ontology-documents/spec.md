## MODIFIED Requirements

### Requirement: Ontologies and mappings are reachable from every surface
Ontologies and mappings SHALL be listed, read, created, replaced and deleted through the Python storage API, the Python client (with synchronous variants), REST (`/ontologies`, `/ontologies/{name}`, `/ontology-mappings`, `/ontology-mappings/{data_source}/{name}`), the CLI (`slayer ontologies list|show|create|replace|delete` and `slayer ontology-mappings list|show|create|replace|delete --datasource`) and MCP (`create_ontology`, `edit_ontology`, `delete_ontology`, `create_ontology_mapping`, `edit_ontology_mapping`, `delete_ontology_mapping`). MCP `inspect` SHALL accept the entity types `ontology` and `ontology_mapping`: for an ontology, compact, the description and its concept graph as `ontologies/ontology-use` specifies, and full, that plus the stored text; for a mapping, compact, the description, the concept-mapping names and whether it resolves with its problems, and full, the stored text. `edit_ontology` SHALL edit the description, `ai_context`, `prefixes` and document `requires`, add, replace or remove whole concepts by name, and add, replace or remove relationships by `Concept.name`; `edit_ontology_mapping` SHALL edit the description and the references, and add, replace or remove concept mappings by concept. Every create, replace and edit SHALL run the checks of this capability, and every surface SHALL report a refusal with every problem and return the save's warnings.

#### Scenario: An agent edits one concept
- **WHEN** `edit_ontology` replaces concept `Carrier` of `Flights` with a version adding relationship `country`
- **THEN** the stored ontology differs from before only in that concept, and `flights_mapping` still resolves

#### Scenario: REST create is refused with every problem
- **WHEN** a mapping with two unknown fields is posted to `/ontology-mappings`
- **THEN** the response is a 400 listing both fields, and nothing is stored

#### Scenario: CLI round trip
- **WHEN** `slayer ontologies create flights.ontology.yaml` runs, then `slayer ontologies show Flights`
- **THEN** the output is the file's text

#### Scenario: Compact ontology inspect is the concept graph
- **WHEN** ontology `Flights` is inspected compact
- **THEN** the output carries the concept graph and not the stored text
