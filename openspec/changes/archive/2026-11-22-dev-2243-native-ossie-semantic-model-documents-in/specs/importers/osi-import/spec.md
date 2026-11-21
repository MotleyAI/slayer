## ADDED Requirements

### Requirement: Import stores the document natively, verbatim
`slayer import-osi <path> --datasource X` SHALL store each valid current-shape document it reads as a native semantic-model document of datasource `X` (`models/semantic-model-documents`), named by the document's `name`, with its text unchanged. It SHALL NOT introspect the datasource and SHALL NOT need it to be reachable, except to upgrade a legacy document ("Legacy wrapper documents keep importing"). Importing a document over an existing document of the same name SHALL replace it. Whether SLayer can evaluate each entity SHALL NOT be checked at import: an entity SLayer cannot evaluate yet SHALL be stored and listed in the import report with the reason.

#### Scenario: Import keeps comments and needs no database
- **WHEN** a valid document with comments is imported into a datasource whose database is unreachable
- **THEN** the import succeeds and the stored text equals the file's text

#### Scenario: A windowed metric is reported, not refused
- **WHEN** upstream's `tpcds_semantic_model.yaml` is imported
- **THEN** the document is stored whole and the report lists its windowed metrics as not evaluable yet

### Requirement: Re-import replaces datasets converted in 1.x
When a document is imported into a datasource holding standalone datasets that a 1.x import created from a document of the same `name`, the import SHALL replace those datasets with the native document. Each model-scoped metric the 1.x import attached to an anchor dataset SHALL become the document's model-scoped metric and SHALL be listed in the report as moved. SLayer-only content of a replaced dataset that the new document does not express SHALL be carried into `SLAYER` entries of the corresponding document entity, never dropped; content that names an entity the new document no longer declares SHALL be listed in the report. References to the replaced datasets by name SHALL keep resolving.

#### Scenario: An anchored metric moves into the document scope
- **WHEN** a 1.x import stored `orders.revenue_per_customer` from a document whose top-level metric `revenue_per_customer` it anchored on `orders`, and that document is imported again
- **THEN** `revenue_per_customer` answers by bare name, `orders.revenue_per_customer` no longer exists, and the report lists the move

## MODIFIED Requirements

### Requirement: Current-spec documents import
The importer SHALL accept a flat Ossie document that holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `nonportable_extensions`). A document declaring `0.3.0.dev0` SHALL be stored as written after it passes the validity check ("Invalid documents are refused with every reason"). A flat document declaring an earlier version (`1.0`, `0.1.0`, `0.1.1` or `0.2.0.dev0`) SHALL be upgraded as Ossie's migration guidance prescribes — its `version` set to `0.3.0.dev0`, `custom_extensions` converted, root-level `dialects` and `vendors` declarations removed — then validated and stored, and the report SHALL list each change made. A document declaring any other version SHALL be refused with an error naming the version.

#### Scenario: Flat document imports end to end
- **WHEN** a file holds `version: 0.3.0.dev0`, `name: shop`, two datasets `orders` and `customers` with declared fields, a relationship `orders → customers`, and a model-scoped metric `SUM(orders.amount)`
- **THEN** the document `shop` is stored with its text unchanged, the datasource exposes datasets `orders` and `customers` with a many-to-one relationship from `orders` to `customers`, and the model-scoped metric answers by bare name

#### Scenario: Earlier flat document keeps importing
- **WHEN** the same document declares `version: 0.2.0.dev0`
- **THEN** it is stored declaring `0.3.0.dev0`, exposes the same datasets, relationship and metric, and the report lists the version change

#### Scenario: Unknown version
- **WHEN** a flat document declares `version: 0.4.0`
- **THEN** the import refuses it with an error naming the version and stores nothing for it

#### Scenario: Document-level extensions kept
- **WHEN** a document carries `nonportable_extensions: [{vendor_name: DBT, data: {project_name: tpcds_analytics}}]` at its root
- **THEN** the stored document keeps that entry unchanged

### Requirement: Legacy wrapper documents keep importing
A document whose root holds a `semantic_model` key and that does not declare `0.3.0.dev0` SHALL be treated as a legacy document. Each model of the wrapper (each element of an array, or the single object) SHALL be split into its own flat `0.3.0.dev0` document, carrying the model's content and every non-portable extension, as Ossie's migration guidance prescribes; no model SHALL be dropped and no existing document SHALL be overwritten by a split without the import naming it. A legacy dataset exposes its introspected source columns as well as its fields, so for each legacy dataset the importer SHALL introspect the datasource and write each source column that no field declares as a declared field reading that column, typed from the database; this upgrade alone SHALL require a reachable datasource. The report SHALL list each split and each field added. A legacy model-level metric's bare reference that exactly one dataset of the model declares (as a field, or as a column added by this upgrade) SHALL be written as `dataset.name`; a metric holding an ambiguous or unresolvable bare reference SHALL be stored as written and reported as not evaluable. The report SHALL list each rewritten metric.

#### Scenario: Legacy array with two models
- **WHEN** a file holds `semantic_model: [ {name: a, datasets: [...]}, {name: b, datasets: [...]} ]`
- **THEN** the datasource gains the flat documents `a` and `b`, and the report lists the split

#### Scenario: Legacy object-valued wrapper
- **WHEN** a file holds `semantic_model: {name: a, datasets: [...]}`
- **THEN** the datasource gains the flat document `a`

#### Scenario: Wrapper without a version
- **WHEN** a file holds `semantic_model: [...]` and no `version`
- **THEN** its models are upgraded as legacy, each stored declaring `0.3.0.dev0`

#### Scenario: Introspected columns become declared fields
- **WHEN** a legacy dataset `orders` declares only the field `order_id` and its source also has the column `amount`
- **THEN** the stored document's `orders` declares a field `amount` reading `amount`, and the report lists it

#### Scenario: Unqualified legacy metrics are qualified
- **WHEN** a legacy model declares `orders` with field `amount` and the model-level metric `total_amount: SUM(amount)`
- **THEN** the stored document's metric reads `SUM(orders.amount)`, it answers by bare name, and the report lists the rewrite

### Requirement: Invalid documents are refused with every reason
A document that is not valid Ossie SHALL be refused with every reason, each naming its entity: a root mixing both shapes or holding neither, an empty or malformed `semantic_model`, a wrapper declaring `0.3.0.dev0`, a key Ossie does not define, a duplicate name (compared as Ossie compares identifiers), a key or relationship naming an undeclared field, mismatched relationship key lengths, a relationship naming a dataset outside the document, a metric reference that breaks Ossie's metric scoping, an expression with neither `sql` nor a `dialects` entry, and an extension whose `data` is not a JSON object. A refused document SHALL store nothing; the other files of the same import SHALL still be imported, and the command SHALL exit non-zero when any file was refused.

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

### Requirement: Legacy custom_extensions keep importing
The importer SHALL read the legacy key `custom_extensions`, whose entries carry `data` as a JSON-encoded string, at every level that carries `nonportable_extensions` (document, dataset, field, metric, relationship), whatever version the document declares. Each legacy entry's string SHALL be decoded and stored as a `nonportable_extensions` entry with that object, after the entity's existing entries, as part of the upgrade the report lists. A legacy entry whose string does not decode to a JSON object SHALL refuse the document with that reason. A document that uses the legacy key anywhere SHALL add exactly one informational entry to the report, naming the document.

#### Scenario: SLayer 1.8 export restores
- **WHEN** a document declaring `0.3.0.dev0` carries its `SLAYER` payloads under `custom_extensions` as JSON strings
- **THEN** the stored document carries the same payloads as `nonportable_extensions` objects, its datasets read back with the same SLayer structure as the same document written with `nonportable_extensions`, and the report holds one informational entry naming the document

#### Scenario: Legacy key at every level
- **WHEN** a document carries a legacy `ACME` entry with `data: '{"k": 1}'` on its root, a dataset, a field, a metric and a relationship
- **THEN** each stored entity carries an `ACME` `nonportable_extensions` entry with `data` equal to `{k: 1}`, no `custom_extensions` key remains, and the report holds exactly one informational entry for the legacy key

#### Scenario: Both keys on one entity
- **WHEN** a field carries `nonportable_extensions: [{vendor_name: ACME, data: {a: 1}}]` and `custom_extensions: [{vendor_name: OTHER, data: '{"b": 2}'}]`
- **THEN** the stored field carries both entries under `nonportable_extensions`, `ACME` first

#### Scenario: Legacy string that is not an object
- **WHEN** a legacy entry's `data` is `'[1, 2]'` or `'not json'`
- **THEN** the import refuses the document, naming the entity and vendor

### Requirement: Extension data must be a JSON object
A non-portable extension entry SHALL be accepted only when its `data` is an object whose keys are strings and whose values, at every depth, are JSON values: strings, finite numbers, booleans, null, arrays and objects. A document holding an entry whose `data` is not an object, or holds a date, a timestamp, a non-finite number or a non-string key at any depth, SHALL be refused, naming the entity and vendor.

#### Scenario: Unquoted date under data
- **WHEN** a dataset's entry carries `data: {released: 2024-01-01}` with the date unquoted in YAML
- **THEN** the import refuses the document, naming the dataset and the vendor

#### Scenario: Nested non-finite number
- **WHEN** an entry carries `data: {stats: {ratio: .inf}}`, `{stats: {ratio: -.inf}}` or `{stats: {ratio: .nan}}`
- **THEN** the import refuses the document, naming the entity and the vendor

#### Scenario: Nested timestamp or non-string key
- **WHEN** an entry carries `data: {log: {at: 2024-01-01T00:00:00Z}}` or `data: {codes: {1: one}}`
- **THEN** the import refuses the document, naming the entity and the vendor

#### Scenario: Data that is not an object
- **WHEN** an entry carries `data: [1, 2]` or `data: text`
- **THEN** the import refuses the document, naming the entity and the vendor

### Requirement: The shipped example is a valid current-spec document
The worked example `docs/examples/13_osi_import/shop.osi.yaml` SHALL validate against the pinned upstream Ossie schema, SHALL import as a native document, and SHALL answer every query of its example notebook.

#### Scenario: Example validates and imports
- **WHEN** the worked example is validated against the pinned schema and imported into the demo DuckDB
- **THEN** validation passes, it is stored as a document, and its example queries return results

### Requirement: Upstream examples import
Every file of the upstream Ossie `examples/` directory at the pinned commit SHALL be vendored unchanged. Each core semantic-model example SHALL import as a native document and load; each ontology or ontology-mapping example SHALL be refused with an error naming it as not a semantic-model document.

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
- **WHEN** `flights.yaml`, `flights.ontology.yaml` and `flights.mapping.yaml` are imported
- **THEN** each is refused with an error naming it as not a semantic-model document

## REMOVED Requirements

### Requirement: Expressions carry an optional portable form and optional dialect entries
**Reason**: Documents are stored as written; Ossie's expression object is the stored field and metric `expression` (`models/field-expressions`), and an expression with neither form is an invalid document.
**Migration**: None; invalid expressions are refused by "Invalid documents are refused with every reason".

### Requirement: Unreadable expressions are reported, never raised
**Reason**: Import no longer parses expressions into SLayer formulas.
**Migration**: An entity SLayer cannot evaluate loads and fails only when read (`models/semantic-model-documents` › Entities SLayer cannot evaluate yet still load).

### Requirement: Selected expressions are rendered for the target datasource
**Reason**: Nothing is rendered at import; the stored expression runs per `models/field-expressions`, translated in memory at load when it has no runnable form.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Current-spec models expose their declared fields
**Reason**: Exposure is a property of the stored document, not of an import.
**Migration**: `models/semantic-model-documents` › Datasets decompose into native datasets exposing exactly their declared fields; duplicate names refuse the document at import.

### Requirement: Field expressions are validated against the live source
**Reason**: Import no longer introspects; a field reading a missing column is schema drift, found by `validate-datasets`.
**Migration**: Run `slayer validate-datasets`.

### Requirement: Keys and relationships name declared fields
**Reason**: Undeclared key names make a document invalid, refused at import; relationships on computed fields are now representable (`models/join-keys`).
**Migration**: "Invalid documents are refused with every reason" and `models/semantic-model-documents`.

### Requirement: Primary keys
**Reason**: Import no longer introspects primary keys; a document's `primary_key` is read as written.
**Migration**: `models/semantic-model-documents` › Datasets decompose into native datasets exposing exactly their declared fields.

### Requirement: Metric name resolution
**Reason**: Metric scoping violations make a document invalid; valid references resolve natively.
**Migration**: "Invalid documents are refused with every reason" and `queries/model-scoped-metrics`.

### Requirement: Identifier case in metric references
**Reason**: Native resolution follows `queries/identifier-case`; nothing is rewritten at import.
**Migration**: `queries/identifier-case`.

### Requirement: Legacy documents keep lenient resolution
**Reason**: Legacy documents are upgraded to the current shape at import, their introspected columns declared as fields.
**Migration**: "Legacy wrapper documents keep importing".

### Requirement: Dataset-scoped metrics become measures on their dataset
**Reason**: Dataset-scoped metrics are the dataset's metrics in the stored document.
**Migration**: `models/semantic-model-documents` › Datasets decompose into native datasets exposing exactly their declared fields.

### Requirement: FILTER aggregates
**Reason**: Evaluated natively from the stored expression.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Aggregate operands
**Reason**: Evaluated natively; no hidden helper fields are added to a stored document.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Field datatypes
**Reason**: A declared `datatype` is the field's type; an undeclared one is unknown and resolved by the engine.
**Migration**: `models/semantic-model-documents` › Fields without a datatype load with an unknown type.

### Requirement: Time-dimension role
**Reason**: Read from the stored document at decomposition.
**Migration**: `models/semantic-model-documents` › Datasets decompose into native datasets exposing exactly their declared fields.

### Requirement: Metric datatypes
**Reason**: A metric's `datatype` is kept in the stored document.
**Migration**: `models/semantic-model-documents` › Top-level metrics decompose into model-scoped metrics.

### Requirement: List aggregation inputs
**Reason**: Evaluated natively from the stored expression.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Array functions map to the list library
**Reason**: Evaluated natively from the stored expression.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: SLAYER extensions restore SLayer structure
**Reason**: `SLAYER` entries are read natively from the stored document on every load, not restored once at import.
**Migration**: `models/semantic-model-documents` › Dataset tools edit a document's datasets through the document.

### Requirement: Restoration is validated per entity
**Reason**: Superseded by native decomposition.
**Migration**: `models/semantic-model-documents`.

### Requirement: Restored source structure
**Reason**: Query-backed and `sql`-mode sources are read natively from `SLAYER` entries.
**Migration**: `models/semantic-model-documents` › Dataset tools edit a document's datasets through the document.

### Requirement: SLayer-only columns and measures
**Reason**: Read natively from `SLAYER` entries.
**Migration**: `models/semantic-model-documents` › Dataset tools edit a document's datasets through the document.

### Requirement: Statistical and approximate aggregates
**Reason**: Evaluated natively from the stored expression.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Single-field unique keys mark the column unique
**Reason**: Read at decomposition.
**Migration**: `models/semantic-model-documents` › Datasets decompose into native datasets exposing exactly their declared fields.

### Requirement: Explicit time role is recorded
**Reason**: The stored document keeps `dimension.is_time` as written.
**Migration**: `models/semantic-model-documents`.

### Requirement: Source columns in dataset-scoped metrics
**Reason**: Evaluated natively from the stored expression.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Duplicate output labels in query sources
**Reason**: Import no longer introspects query sources; an ambiguous source column fails the query that reads it.
**Migration**: None.

### Requirement: Date literals in imported predicates keep Ossie's meaning
**Reason**: Evaluated natively from the stored expression.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: Expression objects are stored as written
**Reason**: Subsumed: the whole document is stored as written.
**Migration**: "Import stores the document natively, verbatim".

### Requirement: An expression with no runnable form is translated
**Reason**: The translation happens in memory at load and is never stored.
**Migration**: `models/semantic-model-documents` › Every construct the 1.x importer converted keeps its meaning natively.

### Requirement: A metric's entry for the target's dialect is not imported
**Reason**: The stored document keeps every dialect entry as written; which form runs is `models/field-expressions`.
**Migration**: `models/field-expressions` › A metric is defined by one expression.

## RENAMED Requirements

- FROM: `### Requirement: Invalid document shapes are skipped with a warning`
- TO: `### Requirement: Invalid documents are refused with every reason`
