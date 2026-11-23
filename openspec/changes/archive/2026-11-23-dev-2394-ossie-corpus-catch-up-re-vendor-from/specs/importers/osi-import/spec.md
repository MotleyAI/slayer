## MODIFIED Requirements

### Requirement: Invalid documents are refused with every reason
A document that is not valid Ossie SHALL be refused with every reason, each naming its entity: a root mixing both shapes or holding neither, an empty or malformed `semantic_model`, a wrapper declaring `0.3.0.dev0`, a key Ossie does not define, a key repeated within one mapping, a scalar its YAML tag cannot construct, a duplicate name (compared as Ossie compares identifiers), a key or relationship naming an undeclared field, mismatched relationship key lengths, a relationship naming a dataset outside the document, a metric reference that breaks Ossie's metric scoping, an expression with neither `sql` nor a `dialects` entry, and an extension whose `data` is not a JSON object. A refused document SHALL store nothing; the other files of the same import SHALL still be imported, and the command SHALL exit non-zero when any file was refused.

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
- **WHEN** a file is an Ossie ontology or ontology-mapping document (no root `datasets` and no `semantic_model`)
- **THEN** that file is refused with an error naming it as not a semantic-model document

#### Scenario: A repeated key is refused in flat and legacy documents
- **WHEN** a flat `0.3.0.dev0` document, and separately a legacy `semantic_model:` document, each repeat the key `source` in one dataset
- **THEN** each file is refused with an error naming the key `source`, and neither stores anything
