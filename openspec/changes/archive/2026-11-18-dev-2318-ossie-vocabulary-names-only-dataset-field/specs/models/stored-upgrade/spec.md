## MODIFIED Requirements

### Requirement: A store written by this release reads on the previous release

Every persisted model, saved query, datasource and memory, in every storage backend, SHALL omit a field whose value is unset (`None`). A backend's own document layout (a YAML memory's file name and body) SHALL be unchanged. A store written by this release is not readable by SLayer 1.x: its documents use the Ossie vocabulary.

#### Scenario: Memory with an ordered query

- **WHEN** a memory whose query orders by a field without `nulls` is saved to YAML and to SQLite
- **THEN** neither stored form contains a `nulls` key

#### Scenario: No unset field in any kind

- **WHEN** a model, a saved query, a datasource and a memory are saved to each backend
- **THEN** no stored value is `null`
