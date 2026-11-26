## ADDED Requirements

### Requirement: Ontology and mapping hooks default to unsupported
A backend SHALL persist ontologies and mappings through hooks to save a document's text, read it, list document identities, and delete a document, for ontologies keyed by name and for mappings keyed by datasource and name. A backend that does not implement them SHALL keep working for datasets, datasources, semantic-model documents and memories: listing ontologies or mappings SHALL return none, and every operation that would write or read one SHALL fail with a typed error naming the backend and stating that it does not support ontology documents. A backend implementing the hooks SHALL change its `graph_fingerprint` whenever an ontology or mapping changes.

#### Scenario: A backend without the ontology hooks
- **WHEN** a custom backend registered with `register_storage` implements the dataset, datasource, memory and semantic-model hooks only, and `create_ontology` is called against it
- **THEN** its datasets and semantic-model documents still load, listing ontologies returns none, and `create_ontology` fails with the typed unsupported error naming the backend
