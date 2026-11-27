## MODIFIED Requirements

### Requirement: Invalid documents are refused with every reason
A document that is not valid Ossie SHALL be refused with every reason, each naming its entity: a root mixing both semantic-model shapes, a root that is none of a semantic-model, ontology or mapping document, an empty or malformed `semantic_model`, a wrapper declaring `0.3.0.dev0`, a key Ossie does not define, a duplicate name (compared as Ossie compares identifiers), a key or relationship naming an undeclared field, mismatched relationship key lengths, a relationship naming a dataset outside the document, a metric reference that breaks Ossie's metric scoping, an expression with neither `sql` nor a `dialects` entry, and an extension whose `data` is not a JSON object. Ontology and mapping documents SHALL be refused by the rules of the `ontologies/ontology-documents` capability. A file whose YAML or JSON repeats an explicit mapping key, at any depth, SHALL be refused naming every repeated key and its line, as upstream `validate.py` refuses it; repeated YAML merge directives SHALL be allowed and a key a merge supplies SHALL be overridable. A scalar the YAML loader cannot build SHALL be a reason naming the value and its line, never an unhandled error. When the text cannot be read into a document (unparseable, a repeated key, an unbuildable scalar), those reasons SHALL be the only ones reported for the file. A refused document SHALL store nothing; the other files of the same import SHALL still be imported, and the command SHALL exit non-zero when any file was refused.

#### Scenario: Every reason is listed
- **WHEN** a document has a duplicate field name in `orders` and a relationship naming the undeclared field `customer_ref`
- **THEN** the import refuses it, naming both problems, and the other files of the import are stored

#### Scenario: Mixed shape
- **WHEN** a file holds both `semantic_model: [...]` and `datasets: [...]` at its root
- **THEN** that file is refused with an error naming it, the other files in the same import are stored, and the command exits non-zero

#### Scenario: Empty legacy wrapper
- **WHEN** a file holds `semantic_model: []`
- **THEN** that file is refused with an error naming it

#### Scenario: Wrapped 0.3 document
- **WHEN** a file declares `version: 0.3.0.dev0` and holds `semantic_model: [...]`, and another file in the same import is a valid flat document
- **THEN** the wrapped file is refused with an error naming it and saying `0.3.0.dev0` documents are flat, and the flat file is stored

#### Scenario: Ontology document
- **WHEN** a file holds none of root `datasets`, `semantic_model`, `ontology`, or the mapping keys `concept_mappings`, `ontology_ref` and `semantic_model_ref`
- **THEN** that file is refused with an error naming it as none of a semantic-model, ontology or mapping document

#### Scenario: Every repeated key is listed
- **WHEN** a YAML file's dataset `orders` declares `name` twice and its field `amount` declares `expression` twice, and another file in the same import is valid
- **THEN** the first file is refused naming both repeated keys with their lines, nothing of it is stored, the other file is stored, and the command exits non-zero

#### Scenario: A repeated key in JSON
- **WHEN** a JSON document repeats the key `source` in a dataset
- **THEN** the import refuses it naming `source` and its line

#### Scenario: Repeated merge directives are allowed
- **WHEN** a YAML document's field merges two anchors with two `<<` entries and overrides a merged key
- **THEN** the document imports, the field taking the overriding value

#### Scenario: An unbuildable scalar is a reason
- **WHEN** a YAML document's dataset `description` is the untagged value `2024-02-30`
- **THEN** the import refuses the file naming the value and its line, and the command exits non-zero without a traceback

#### Scenario: A repeated key is refused in flat and legacy documents
- **WHEN** a flat `0.3.0.dev0` document, and separately a legacy `semantic_model:` document, each repeat the key `source` in one dataset
- **THEN** each file is refused with an error naming the key `source`, and neither stores anything

### Requirement: Upstream examples import
Every file of the upstream Ossie `examples/` directory at the pinned commit SHALL be vendored unchanged. Each core semantic-model example SHALL import as a native document and load; each ontology and mapping example SHALL import by the rules of the `ontologies/ontology-documents` capability, with no refusal.

#### Scenario: Renamed-fields example
- **WHEN** `renamed_fields.yaml` is imported
- **THEN** datasets `orders` and `customers` load, and `gross_revenue` returns the sum of `o_totalprice`

#### Scenario: TPC-DS example
- **WHEN** `tpcds_semantic_model.yaml` is imported
- **THEN** its five datasets load, and the report lists exactly its three window-function metrics as not evaluable yet

#### Scenario: Flights semantic model
- **WHEN** `flights.semantic_model.yaml` is imported
- **THEN** its six datasets load and the report lists nothing as not evaluable

#### Scenario: Ontology examples
- **WHEN** each of these runs is imported into its own empty store: `flights.ontology.yaml`, `flights.semantic_model.yaml` and `flights.mapping.yaml` into datasource `flights`; `flights.yaml` into datasource `flights`; `sap_business_partner.ontology.yaml`, `sap_business_partner.semantic_model.yaml` and `sap_business_partner.mapping.yaml` into datasource `sap`; `foaf_owl_import.yaml`
- **THEN** nothing is refused; the stores hold ontology `Flights` with mapping `flights_mapping` in `flights` (twice), ontology `sap_business_partner` with mapping `sap_business_partner_mapping` in `sap`, and ontology `foaf`; every mapping resolves; and the `flights.yaml` run reports its embedded mapping as an upgrade

## ADDED Requirements

### Requirement: Ontology and mapping documents import with their references
`import-osi` SHALL store an ontology document as a global ontology, whatever `--datasource` names, and a mapping document under `--datasource`. Within one import, ontology documents SHALL be imported first, semantic-model documents next and mapping documents last, so a mapping resolves against documents of the same import whatever the file order. A mapping whose ontology or semantic model was refused in the same import SHALL be refused, naming that document. Re-importing an ontology or mapping SHALL replace the stored one, subject to the integrity rules of the `ontologies/ontology-documents` capability.

#### Scenario: File order does not matter
- **WHEN** a folder holding `a.mapping.yaml`, `b.semantic_model.yaml` and `c.ontology.yaml`, where the mapping references the other two, is imported into datasource `ds`
- **THEN** all three are stored and the mapping resolves

#### Scenario: A refused ontology refuses its mapping
- **WHEN** an import holds an ontology whose concept extends an undeclared concept, and a mapping referencing that ontology
- **THEN** both are refused, the mapping's reason naming the refused ontology, and the command exits non-zero

### Requirement: Embedded ontology mappings are upgraded
An ontology document carrying the deprecated `ontology_mappings` SHALL import as follows: each entry's inline `semantic_model` SHALL be imported into `--datasource` as a semantic-model document by the rules for those documents; each entry SHALL become a standalone mapping in `--datasource` named by the entry's `name`, whose `ontology_ref.name` is the ontology's `name`, whose `semantic_model_ref.name` is the inline semantic model's `name`, and whose `concept_mappings` are the entry's; the ontology SHALL be stored without `ontology_mappings`. The report SHALL list exactly one upgrade entry naming the document. When the inline semantic model or the resulting mapping is refused, the whole document SHALL be refused and nothing of it stored.

#### Scenario: flights.yaml is upgraded
- **WHEN** `flights.yaml` is imported into datasource `flights` of an empty store
- **THEN** ontology `Flights` is stored without `ontology_mappings`, semantic model `Flights semantic model` and mapping `flights_mapping` are stored in `flights`, the mapping's concept mappings equal those of `flights.mapping.yaml`, and the report lists one upgrade entry naming `flights.yaml`
