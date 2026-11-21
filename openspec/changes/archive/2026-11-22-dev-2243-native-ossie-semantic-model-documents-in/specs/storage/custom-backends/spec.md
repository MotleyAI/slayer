## ADDED Requirements

### Requirement: Semantic-model hooks default to unsupported
A backend SHALL persist semantic-model documents through hooks to save a document's text, read it, list document identities, delete a document, and read, patch and store one document atomically. A backend that does not implement them SHALL keep working for datasets, datasources and memories: listing documents SHALL return none, and every operation that would write or read a document SHALL fail with a typed error naming the backend and stating that it does not support semantic-model documents. A backend implementing the hooks SHALL change its `graph_fingerprint` whenever a document's content or membership changes.

#### Scenario: A backend without the hooks
- **WHEN** a custom backend registered with `register_storage` implements only the dataset, datasource and memory hooks, and `create_semantic_model` is called against it
- **THEN** its datasets still load and answer queries, listing documents returns none, and `create_semantic_model` fails with the typed unsupported error naming the backend
