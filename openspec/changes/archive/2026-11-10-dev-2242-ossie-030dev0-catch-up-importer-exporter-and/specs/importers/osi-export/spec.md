## MODIFIED Requirements

### Requirement: Export writes one current-spec document per datasource

`slayer export-osi --datasource X` and the Python export API SHALL write the datasource's models as one flat current-spec Ossie document (`version: 0.3.0.dev0`) that validates against the pinned upstream Ossie schema. The document's `name` and `description` SHALL come from the datasource, or from the models' shared imported semantic-model metadata when every exported model carries the same one. Datasets SHALL be ordered by model name, and fields, metrics and relationships in a deterministic order, so that exporting unchanged models twice produces identical documents. Export SHALL NOT access the database directly; any metadata it needs SHALL be read through the query engine.

#### Scenario: Exported document validates
- **WHEN** a datasource with table-backed, sql-mode and query-backed models, joins and measures is exported
- **THEN** the document declares `version: 0.3.0.dev0` and validates against the pinned Ossie schema

#### Scenario: Deterministic output
- **WHEN** the same unchanged models are exported twice
- **THEN** the two documents are byte-identical

#### Scenario: No direct database access for table and sql-mode models
- **WHEN** a datasource holding only table-backed and sql-mode models is exported through an engine whose database client fails on every call
- **THEN** the export succeeds

#### Scenario: Imported source columns round trip
- **WHEN** a document whose dataset-scoped metrics read `source.quantity` (through a hidden helper) and `source.amount` (through a pass-through field) is imported, exported with extensions, validated, re-imported and exported again
- **THEN** both exports are byte-identical, the helper column is hidden after re-import, every extension fingerprint matches, and both measures return the same values as before
