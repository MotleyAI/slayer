## Purpose

Reads, checks, stores, edits and exports Ossie Layer 4 ontology documents and the mapping documents that populate an ontology from one semantic model's datasets, keeping every stored document as written and every stored mapping valid against what it references.

## ADDED Requirements

### Requirement: Ontology and mapping documents are recognised by Ossie's keys
A document with a root `ontology` key SHALL be an ontology document. A document with root `concept_mappings`, `ontology_ref` and `semantic_model_ref` SHALL be a mapping document. Both SHALL declare `version: 0.3.0.dev0`; any other version SHALL be refused naming the version.

#### Scenario: A mapping document is recognised
- **WHEN** `flights.mapping.yaml` is saved as a mapping
- **THEN** it is accepted as a mapping document

#### Scenario: Wrong version
- **WHEN** an otherwise valid ontology document declares `version: 0.2.0`
- **THEN** it is refused naming the version

### Requirement: Ontologies are checked when saved
An ontology document SHALL be refused when saved, with every problem listed and each naming its concept, relationship or role, when it fails any of: validation against the vendored upstream `ontology/ontology.json` (with every schema reference resolved locally, never over the network); any check upstream Ossie's validator (`validation/validate.py`, `validate_ontology`) makes; a concept named in `extends` or a role that is neither declared nor one of the built-ins `Any`, `Boolean`, `Date`, `DateTime`, `Decimal`, `Float`, `Integer`, `String`; a concept declared twice; a relationship name repeated within its concept; two roles of one relationship with the same effective name, where the first role's effective name is its concept's name and each further role's is its `name`, else its concept's name; an `extends` cycle; a ValueType extending an EntityType or not reaching a built-in; an EntityType extending a ValueType or a built-in; an `iri` written as a QName whose prefix is not declared in `prefixes`, classified as upstream's validator classifies it; an `identify_by` entry that does not name a binary relationship declared on the concept or a concept it extends. Names SHALL be compared as Ossie compares identifiers. A refused ontology SHALL store nothing.

#### Scenario: Every problem is listed
- **WHEN** an ontology declares concept `Order` extending undeclared `Thing`, and concept `Line` whose `identify_by` names `order`, a ternary relationship
- **THEN** it is refused naming both problems, and nothing is stored

#### Scenario: Inherited identifier
- **WHEN** `sap_business_partner.ontology.yaml`, whose `Customer` extends `BusinessPartner` and declares `identify_by: [ nr ]` with `nr` declared on `BusinessPartner`, is saved
- **THEN** it is accepted

#### Scenario: Ambiguous roles
- **WHEN** a relationship `ships_to` under `Store` has the single role `concept: Store` with no `name`
- **THEN** the ontology is refused naming `Store.ships_to` and the repeated role name `Store`

#### Scenario: Full IRIs and QNames
- **WHEN** an ontology declares prefix `foaf` and carries the iris `foaf:Person`, `http://schema.org/Person`, `urn:isbn:0451450523` and `ex:Thing`
- **THEN** it is refused naming only the undeclared prefix `ex`

#### Scenario: Value type cycle
- **WHEN** `A` extends `B` and `B` extends `A`
- **THEN** the ontology is refused naming the cycle

### Requirement: Rule expressions are kept, not interpreted
`requires` and `derived_by` expressions, at document, concept and relationship level, SHALL be stored and exported exactly as written. No check SHALL interpret them beyond the upstream schema's rules (non-empty, listed once).

#### Scenario: Flights rules are kept
- **WHEN** `flights.ontology.yaml` is saved and exported
- **THEN** it is accepted, and the export carries `Delay == AVG[Flight.departure_delay WHERE Airport == Flight.route.departure GROUP BY Airport]` and `COUNT[Airport] > 0` as written

### Requirement: Mapping references resolve
A mapping SHALL belong to one datasource. Its `ontology_ref.name` SHALL name a stored ontology. Its `semantic_model_ref.name` SHALL name a semantic-model document stored in the mapping's datasource, else the datasource itself, meaning every dataset of the datasource; when a document and the datasource share the name, the document SHALL be meant. A reference's `iri` SHALL be kept and never fetched. A reference that does not resolve SHALL refuse the mapping, naming the reference and, for `semantic_model_ref`, the datasource's semantic-model documents and its own name as the candidates.

#### Scenario: A document is referenced
- **WHEN** datasource `flights` holds semantic-model document `Flights semantic model` and `flights.mapping.yaml` is saved in `flights` after ontology `Flights` is stored
- **THEN** the mapping resolves against that document's datasets

#### Scenario: The datasource is referenced
- **WHEN** datasource `shop` holds standalone datasets `orders` and `customers` and no document, and a mapping in `shop` names `semantic_model_ref: {name: shop}` with expressions over `orders`
- **THEN** the mapping resolves against `shop`'s datasets

#### Scenario: Unknown semantic model
- **WHEN** a mapping in datasource `flights` names `semantic_model_ref: {name: Flights model}`
- **THEN** it is refused naming `Flights model` and the candidates `Flights semantic model` and `flights`

### Requirement: Mapping structure follows the ontology
A mapping SHALL be refused, with every problem listed and each naming its path in the document, when it fails validation against the vendored upstream `ontology/mapping.json` or any check upstream Ossie's validator (`validate_mapping`) makes, or when: a concept it names is not declared; an object mapping's `concept` is neither the concept expected at its position nor a subtype of it; a relationship it names is not declared on the concept at its position or a concept it extends, a qualified `Concept.name` naming that concept or one it extends; an object mapping uses `expression` for an entity type without a simple identifier (exactly one `identify_by` relationship, binary, whose last role reaches a value type); an object mapping's `referent_mappings` do not name each `identify_by` relationship of its concept exactly once; a link-mapping node at depth k names a relationship whose arity is not k. The concept expected at a position SHALL be: the concept mapping's concept for an object mapping and for a link mapping at depth 1; the last role's concept of the relationship a link-mapping node at depth k > 1 names; the last role's concept of the identifying relationship for nested `referent_mappings`.

#### Scenario: Every upstream mapping example resolves
- **WHEN** `flights.mapping.yaml` and `sap_business_partner.mapping.yaml` are saved after their ontologies and semantic models
- **THEN** both are accepted, including `Runway`'s compound identifier, the unary link mappings `Flight.diverted` and `CustomerSalesArea.delivery_blocked`, and the qualified referent `BusinessPartner.nr`

#### Scenario: Wrong link level
- **WHEN** a link mapping names the binary relationship `Flight.route` at depth 1
- **THEN** the mapping is refused naming the path, the relationship, its arity 2 and the depth 1

#### Scenario: Missing identifier part
- **WHEN** an object mapping for `Runway` (identified by `designator` and `airport`) gives only a `designator` referent
- **THEN** the mapping is refused naming `Runway` and the missing `airport`

### Requirement: Mapping expressions are row-level Layer 1 over the referenced semantic model
Every mapping `expression` SHALL be read as `<value>` or `<value> WHERE ( <predicate> )`, where the optional predicate keeps only the rows for which it holds. Value and predicate SHALL each be a Layer 1 expression over the fields of the resolved semantic model's datasets, every field qualified by its dataset, with no aggregate and no window; the predicate SHALL be boolean. An expression that does not parse, names an unknown dataset or field, aggregates, uses a window, or has a non-boolean predicate SHALL refuse the mapping, naming its path.

#### Scenario: Filtered expressions
- **WHEN** the flights mapping's `FLIGHT.id WHERE ( FLIGHT.diverted == TRUE )` and the SAP mapping's `CUSTOMER_SALES_AREA.SALES_ORG WHERE ( CUSTOMER_SALES_AREA.DELIVERY_BLOCK <> '' )` are resolved
- **THEN** both are accepted, each as its value with its row filter

#### Scenario: Unknown field
- **WHEN** a mapping expression reads `FLIGHT.departure_delay`, which `FLIGHT` does not declare
- **THEN** the mapping is refused naming the path and the field

#### Scenario: Aggregate
- **WHEN** a mapping expression reads `AVG(FLIGHT.dep_delay)`
- **THEN** the mapping is refused naming the path

### Requirement: Mapping expression types fit the concept
An expression's type SHALL be compatible with the built-in its expected concept reaches through `extends` (for an entity mapped by `expression`, the built-in of its simple identifier's value type): `Integer`, `Decimal` and `Float` accept any numeric type; `Boolean` a boolean; `String` a string; `Date` a date; `DateTime` a timestamp or a date; `Any` every type. An incompatible known type SHALL refuse the mapping, naming the path, the type and the concept. An expression whose type is unknown SHALL be accepted, and the save SHALL report one informational entry per mapping listing those expressions' paths.

#### Scenario: Incompatible type
- **WHEN** `FLIGHT.dep_delay` is declared a string and a mapping maps it to `Delay`, which reaches `Decimal`
- **THEN** the mapping is refused naming the path, the string type and `Delay`

#### Scenario: Untyped fields
- **WHEN** `flights.mapping.yaml` is saved against `flights.semantic_model.yaml`, whose fields declare no datatypes
- **THEN** it is accepted, and one informational entry lists the expressions whose type is unknown

### Requirement: Ontologies and mappings are stored verbatim
Ontologies SHALL be stored once per store, by name; mappings SHALL be stored per datasource, by name. A document's name SHALL be the `name` it declares, its identity in Ossie (a reference's `name` must equal the referenced document's own `name`); saving it under any other name SHALL be refused, naming both names. Each SHALL be stored as the text it was saved with, and read and exported as that text. Saving a document under a stored name SHALL replace it. Deleting a datasource SHALL delete its mappings.

#### Scenario: Byte round trip
- **WHEN** `foaf_owl_import.yaml`, with its comments, `prefixes`, `iri` values and `annotations`, is saved and exported
- **THEN** the export is byte-identical to the file

#### Scenario: Saved under another name
- **WHEN** `flights.ontology.yaml`, which declares `name: Flights`, is saved as ontology `Airlines`, or `flights.mapping.yaml` is saved as mapping `other_mapping`
- **THEN** the save is refused naming the declared and the requested name, and nothing is stored

#### Scenario: Datasource delete
- **WHEN** datasource `flights` holding mapping `flights_mapping` is deleted
- **THEN** the mapping is deleted and ontology `Flights` is kept

### Requirement: Explicit changes never break a stored mapping
An explicit change SHALL be refused when, after it, a stored mapping would fail the mapping checks of this capability: saving, editing or deleting an ontology, a semantic-model document or a dataset, and deleting a field. The refusal SHALL list every mapping it would break and each problem. When a broken mapping entry is hidden from the caller by access tags, the refusal SHALL be a hidden-content conflict that names no hidden content. Database-driven changes (ingest, live-schema refinement) SHALL apply, warn naming each mapping they break, and leave that mapping unresolved. An unresolved mapping SHALL be listed and inspected with its problems, and SHALL be reported on load as an unloadable document. Saving the mapping again SHALL re-check it. Deleting a mapping SHALL always be allowed.

#### Scenario: Ontology delete is refused
- **WHEN** ontology `Flights` is deleted while datasource `flights` holds `flights_mapping`
- **THEN** the delete is refused naming `flights_mapping`, and both documents are kept

#### Scenario: Dataset edit is refused
- **WHEN** field `dep_delay` is removed from `FLIGHT` while `flights_mapping` maps it
- **THEN** the edit is refused naming `flights_mapping` and the path that reads `FLIGHT.dep_delay`

#### Scenario: Live schema drops a field
- **WHEN** live-schema refinement removes `FLIGHT.dep_delay`, which `flights_mapping` maps
- **THEN** the refinement applies with a warning naming `flights_mapping`, the mapping is listed as unresolved with that problem, and loading reports it as unloadable

### Requirement: Mapping entries hidden by access tags are redacted
A caller SHALL read a mapping without each `concept_mappings` entry whose expressions, at any depth and including predicates, read a dataset hidden from that caller. An edit touching a hidden entry SHALL be refused as a hidden-content conflict naming no hidden content. A save by such a caller SHALL keep the hidden entries in place. Ontologies SHALL be visible to every caller.

#### Scenario: A hidden dataset's entry is redacted
- **WHEN** a caller who cannot see dataset `AIRCRAFT` reads `flights_mapping`
- **THEN** the `Manufacturer`, `Model` and `Aircraft` entries are absent, and every other entry is present

#### Scenario: A redacted save keeps the hidden entries
- **WHEN** that caller changes the mapping's description and saves it
- **THEN** the stored mapping keeps the `Manufacturer`, `Model` and `Aircraft` entries

### Requirement: Ontologies and mappings are reachable from every surface
Ontologies and mappings SHALL be listed, read, created, replaced and deleted through the Python storage API, the Python client (with synchronous variants), REST (`/ontologies`, `/ontologies/{name}`, `/ontology-mappings`, `/ontology-mappings/{data_source}/{name}`), the CLI (`slayer ontologies list|show|create|replace|delete` and `slayer ontology-mappings list|show|create|replace|delete --datasource`) and MCP (`create_ontology`, `edit_ontology`, `delete_ontology`, `create_ontology_mapping`, `edit_ontology_mapping`, `delete_ontology_mapping`). MCP `inspect` SHALL accept the entity types `ontology` and `ontology_mapping`: compact, the description and the concept or concept-mapping names, and for a mapping whether it resolves with its problems; full, the stored text. `edit_ontology` SHALL edit the description, `ai_context`, `prefixes` and document `requires`, add, replace or remove whole concepts by name, and add, replace or remove relationships by `Concept.name`; `edit_ontology_mapping` SHALL edit the description and the references, and add, replace or remove concept mappings by concept. Every create, replace and edit SHALL run the checks of this capability, and every surface SHALL report a refusal with every problem.

#### Scenario: An agent edits one concept
- **WHEN** `edit_ontology` replaces concept `Carrier` of `Flights` with a version adding relationship `country`
- **THEN** the stored ontology differs from before only in that concept, and `flights_mapping` still resolves

#### Scenario: REST create is refused with every problem
- **WHEN** a mapping with two unknown fields is posted to `/ontology-mappings`
- **THEN** the response is a 400 listing both fields, and nothing is stored

#### Scenario: CLI round trip
- **WHEN** `slayer ontologies create flights.ontology.yaml` runs, then `slayer ontologies show Flights`
- **THEN** the output is the file's text

### Requirement: Ossie's validator is the floor
Every vendored upstream ontology and mapping example SHALL be accepted, and every ontology or mapping document that upstream Ossie's vendored validator reports an error for (warnings excluded) SHALL be refused, including the ontology and mapping cases harvested from upstream's validation tests.

#### Scenario: Harvested upstream cases
- **WHEN** each harvested upstream ontology and mapping case is saved
- **THEN** SLayer refuses every case the vendored validator reports an error for
