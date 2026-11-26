## ADDED Requirements

### Requirement: Ontologies and mappings export as stored
`slayer export-osi --ontology NAME` SHALL write the stored ontology `NAME`, and `slayer export-osi --mapping NAME --datasource DS` the mapping `NAME` of `DS`, each as its stored text (converted to JSON only when the output file is JSON); the Python export API SHALL offer the same. An ontology stored from a document with embedded `ontology_mappings` SHALL export without them, its mappings exporting as standalone mapping documents. `--ontology` SHALL NOT be combined with `--datasource` or `--datasets`, and `--mapping` SHALL require `--datasource`. An unknown name SHALL be an error naming it.

#### Scenario: Ontology export is byte-identical
- **WHEN** `flights.ontology.yaml` is imported and exported with `--ontology Flights` to YAML
- **THEN** the output is byte-identical to the file

#### Scenario: Upgraded embedded mapping exports standalone
- **WHEN** `flights.yaml` is imported into datasource `flights` and exported with `--mapping flights_mapping --datasource flights`
- **THEN** the output is a mapping document that validates against the vendored `mapping.json`, with `ontology_ref.name: Flights` and `semantic_model_ref.name: Flights semantic model`

#### Scenario: Unknown ontology
- **WHEN** `export-osi --ontology Nope` runs
- **THEN** it fails naming `Nope`
