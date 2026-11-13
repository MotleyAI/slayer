## MODIFIED Requirements

### Requirement: Current-spec documents import

The importer SHALL accept a flat Ossie document that holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `nonportable_extensions`). Each such document SHALL be imported as one semantic model with the current-spec rules, whatever version it declares. The versions `1.0`, `0.1.0`, `0.1.1`, `0.2.0.dev0` and `0.3.0.dev0` SHALL parse without a warning; any other declared version SHALL be warned about and still parsed.

#### Scenario: Flat document imports end to end
- **WHEN** a file holds `version: 0.3.0.dev0`, `name: shop`, two datasets `orders` and `customers` with declared fields, a relationship `orders → customers`, and a model-scoped metric `SUM(orders.amount)`
- **THEN** the import produces models `orders` and `customers`, a many-to-one join from `orders` to `customers`, and the measure on `orders`, with no unconverted entries and no version warning

#### Scenario: Earlier flat document keeps importing
- **WHEN** the same document declares `version: 0.2.0.dev0`
- **THEN** it imports with the same models, join and measure, and no version warning

#### Scenario: Unknown version
- **WHEN** a flat document declares `version: 0.4.0`
- **THEN** a warning names the version and the document imports

#### Scenario: Document-level extensions kept
- **WHEN** a document carries `nonportable_extensions: [{vendor_name: DBT, data: {project_name: tpcds_analytics}}]` at its root
- **THEN** every imported model records that entry, with `data` as the object `{project_name: tpcds_analytics}`

### Requirement: SLAYER extensions restore SLayer structure

The importer SHALL read non-portable extensions with `vendor_name: SLAYER` and apply the SLayer content their `data` object carries: join type, name, cardinality and declaring model; measure formula, model, name, label, type and metadata; column display attributes, metadata, type and value/filter split; model hidden flag, access tags, default time dimension, metadata, custom aggregations, source structure, and SLayer-only columns and measures. Content that duplicates Ossie content (a measure's formula and model, a column's type and value/filter split, a model's source structure) SHALL be restored only when the payload's fingerprint matches the entity's Ossie content as found in the document; otherwise the Ossie content SHALL be imported as it would be without the extension, and the report SHALL hold an informational entry naming the entity. A payload that is malformed, of an unknown version, or attached to an entity it does not describe SHALL be reported and ignored, and the import SHALL NOT raise. Non-portable extensions of other vendors SHALL be stored on the entity with their `data` object unchanged.

#### Scenario: Unedited formula restored
- **WHEN** an exported metric `aov` with `sql: SUM(amount) / COUNT(*)` carries a `SLAYER` extension recording the formula `revenue / n` and a matching fingerprint
- **THEN** the imported measure `aov` has the formula `revenue / n`

#### Scenario: Edited metric falls back to its SQL
- **WHEN** the same metric's `sql` was edited to `SUM(amount) / NULLIF(COUNT(*), 0)` after export
- **THEN** the imported measure is converted from the edited SQL and the report holds an informational entry naming `aov`

#### Scenario: Inner join restored
- **WHEN** a relationship carries a `SLAYER` extension recording an inner join declared on `orders`
- **THEN** the imported join from `orders` is an inner join

#### Scenario: One-to-many declaration restored
- **WHEN** a relationship `from: orders, to: customers` carries a `SLAYER` extension recording a one-to-many join declared on `customers`
- **THEN** `customers` has a one-to-many join to `orders` and `orders` has no join to `customers`

#### Scenario: Malformed payload
- **WHEN** a dataset's `SLAYER` extension `data` object lacks the payload's `kind`
- **THEN** the dataset imports as it would without the extension and the report names it

#### Scenario: Foreign extension stored as an object
- **WHEN** a field carries `nonportable_extensions: [{vendor_name: ACME, data: {k: 1, nested: {flag: true}}}]`
- **THEN** the imported column records that entry with `data` equal to `{k: 1, nested: {flag: true}}`

## ADDED Requirements

### Requirement: Legacy custom_extensions keep importing

The importer SHALL read the legacy key `custom_extensions`, whose entries carry `data` as a JSON-encoded string, at every level that carries `nonportable_extensions` (document, dataset, field, metric, relationship), whatever version the document declares. Each legacy entry's string SHALL be decoded and then treated exactly as a `nonportable_extensions` entry with that object; an entity carrying both keys SHALL have the entries of both read. A legacy entry whose string does not decode to a JSON object SHALL be dropped and reported. A document that uses the legacy key anywhere SHALL add exactly one informational entry to the report, naming the document and suggesting a re-export.

#### Scenario: SLayer 1.8 export restores
- **WHEN** a document declaring `0.3.0.dev0` carries its `SLAYER` payloads under `custom_extensions` as JSON strings, with matching fingerprints
- **THEN** the import restores the same SLayer structure as for the same document written with `nonportable_extensions`, and the report holds one informational entry naming the document

#### Scenario: Legacy key at every level
- **WHEN** a document carries a legacy `ACME` entry with `data: '{"k": 1}'` on its root, a dataset, a field, a metric and a relationship
- **THEN** each entity records an `ACME` entry with `data` equal to `{k: 1}`, and the report holds exactly one informational entry for the legacy key

#### Scenario: Both keys on one entity
- **WHEN** a field carries `nonportable_extensions: [{vendor_name: ACME, data: {a: 1}}]` and `custom_extensions: [{vendor_name: OTHER, data: '{"b": 2}'}]`
- **THEN** the imported column records both entries

#### Scenario: Legacy string that is not an object
- **WHEN** a legacy entry's `data` is `'[1, 2]'` or `'not json'`
- **THEN** that entry is dropped, the report names the entity and vendor, and the rest of the document imports

### Requirement: Extension data must be a JSON object

A non-portable extension entry SHALL be kept only when its `data` is an object whose keys are strings and whose values, at every depth, are JSON values: strings, finite numbers, booleans, null, arrays and objects. An entry whose `data` is not an object, or holds a date, a timestamp, a non-finite number or a non-string key at any depth, SHALL be dropped and reported with its entity and vendor; the entity and the rest of the document SHALL import as without that entry.

#### Scenario: Unquoted date under data
- **WHEN** a dataset's entry carries `data: {released: 2024-01-01}` with the date unquoted in YAML
- **THEN** the entry is dropped, the report names the dataset and vendor, and the dataset imports

#### Scenario: Nested non-finite number
- **WHEN** an entry carries `data: {stats: {ratio: .inf}}`, `{stats: {ratio: -.inf}}` or `{stats: {ratio: .nan}}`
- **THEN** the entry is dropped and reported

#### Scenario: Nested timestamp or non-string key
- **WHEN** an entry carries `data: {log: {at: 2024-01-01T00:00:00Z}}` or `data: {codes: {1: one}}`
- **THEN** the entry is dropped and reported

#### Scenario: Data that is not an object
- **WHEN** an entry carries `data: [1, 2]` or `data: text`
- **THEN** the entry is dropped and reported, and the other entries on the same entity are kept
