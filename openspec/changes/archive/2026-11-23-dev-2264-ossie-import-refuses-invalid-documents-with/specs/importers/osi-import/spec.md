## MODIFIED Requirements

### Requirement: Invalid documents are refused with every reason
A document that is not valid Ossie SHALL be refused with every reason, each naming its entity: a root mixing both shapes or holding neither, an empty or malformed `semantic_model`, a wrapper declaring `0.3.0.dev0`, a key Ossie does not define, a duplicate name (compared as Ossie compares identifiers), a key or relationship naming an undeclared field, mismatched relationship key lengths, a relationship naming a dataset outside the document, a metric reference that breaks Ossie's metric scoping, an expression with neither `sql` nor a `dialects` entry, and an extension whose `data` is not a JSON object. A file whose YAML or JSON repeats an explicit mapping key, at any depth, SHALL be refused naming every repeated key and its line, as upstream `validate.py` refuses it; repeated YAML merge directives SHALL be allowed and a key a merge supplies SHALL be overridable. A scalar the YAML loader cannot build SHALL be a reason naming the value and its line, never an unhandled error. When the text cannot be read into a document (unparseable, a repeated key, an unbuildable scalar), those reasons SHALL be the only ones reported for the file. A refused document SHALL store nothing; the other files of the same import SHALL still be imported, and the command SHALL exit non-zero when any file was refused.

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
