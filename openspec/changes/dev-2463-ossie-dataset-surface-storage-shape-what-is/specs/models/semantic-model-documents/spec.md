## MODIFIED Requirements

### Requirement: A semantic-model document is a stored entity bound to its datasource by location
Storage SHALL persist semantic-model documents per datasource, identified by `(data_source, name)`, in the layout of `models/stored-format`: in YAML storage in `semantic_models/<data_source>/`, and in SQLite storage as document text kept verbatim. The document's datasource SHALL be the one it is stored under; an Ossie document carries no connection. The document's identity SHALL be the `name` its content declares, any non-empty string; its file name SHALL follow `models/stored-format`. A document SHALL be listable, readable as its stored text, replaceable and deletable through storage. The datasource path component SHALL obey the existing datasource path rules.

#### Scenario: A document file binds to its directory's datasource
- **WHEN** `semantic_models/shop/sales.yaml` holds a valid Ossie document named `sales` declaring datasets `orders` and `customers`
- **THEN** the datasource `shop` lists the datasets `orders` and `customers`, and the document `sales` is listed for `shop`

#### Scenario: SQLite stores the text verbatim
- **WHEN** a document whose YAML text carries comments is saved to SQLite storage and read back
- **THEN** the text read back equals the text saved, comments included

#### Scenario: A document name that is not a file name
- **WHEN** a document named `Sales / EMEA` is saved to datasource `shop`
- **THEN** it is listed for `shop` as `Sales / EMEA` and reads back with that name

### Requirement: Only Ossie's current document shape is accepted natively
A stored model document SHALL be an Ossie document of version `0.3.0.dev0`, either flat (`version`, `name`, a non-empty `datasets`) or a split model document as Ossie's multi-file models define it (`version`, `name`, a non-empty `dataset_refs`, and optionally a non-empty `datasets`), and optionally `description`, `ai_context`, `relationships`, `metrics` and `nonportable_extensions`, with every nested entity in Ossie's shape; a stored dataset document SHALL hold exactly `version` and `dataset`. A key Ossie does not define, at any level, SHALL make the document unloadable with a typed error naming the document and the key; so SHALL an unknown `version`, the legacy `semantic_model` wrapper, and the legacy `custom_extensions` key. The error for an earlier shape SHALL name `slayer import-osi` as the upgrade path. A document whose text repeats a key within one mapping, or holds a scalar its YAML tag cannot construct (such as a malformed timestamp), SHALL be invalid with a typed reason naming the key or the value, whichever SLayer surface reads the text (storage, import, export); a YAML merge key (`<<`) SHALL keep YAML 1.1 precedence (earlier merge sources win, explicit keys beat merged ones) as upstream's loader does. An unloadable document SHALL follow `models/document-isolation`.

#### Scenario: An unknown key refuses the document
- **WHEN** a stored document's dataset carries the key `grain`
- **THEN** loading the datasource reports the document as unloadable, naming `grain`, and none of its datasets are visible

#### Scenario: A legacy wrapper names the upgrade path
- **WHEN** a file in `semantic_models/shop/` holds a `semantic_model:` wrapper
- **THEN** the document is unloadable with an error naming `slayer import-osi`

#### Scenario: A repeated key refuses the document
- **WHEN** a stored document's dataset `orders` declares `source: db.s.orders` and then `source: db.s.other`
- **THEN** the document is unloadable with an error naming the repeated key `source`, and none of its datasets are visible

#### Scenario: A malformed timestamp refuses the document
- **WHEN** a stored document holds the value `!!timestamp not-a-date`
- **THEN** the document is unloadable with a typed error naming the value, and loading the datasource does not crash

#### Scenario: Merge keys keep their precedence
- **WHEN** a document's field mapping is `<<: [*first, *second]` where both anchors set `description`
- **THEN** the field loads with the description from `*first`

#### Scenario: A split model document loads
- **WHEN** `semantic_models/shop/` holds a model document `sales` with `dataset_refs` naming `orders.dataset.yaml` and `customers.dataset.yaml`, both valid dataset documents
- **THEN** the datasource `shop` lists the datasets `orders` and `customers` of document `sales`

### Requirement: Ossie's version and SLayer's stored version never collide
A document's `version` SHALL be read only as the Ossie specification version. SLayer's versioning of what it stores SHALL be the `version` of each `SLAYER` payload, as `models/stored-format` specifies; a document whose entities carry no `SLAYER` entry SHALL be read as current. Migrations SHALL apply only to `SLAYER` payloads, and a load SHALL never write the document back.

#### Scenario: A hand-written document needs no SLayer entry
- **WHEN** a document with `version: 0.3.0.dev0` and no `SLAYER` entry is loaded
- **THEN** it loads as current and its file is unchanged afterwards

### Requirement: Dataset tools edit a document's datasets through the document
Every operation that writes a dataset SHALL determine the model that holds it and SHALL patch only the affected entities of that model's documents: `create_dataset`, `edit_dataset`, `delete_dataset`, the REST and CLI dataset writes, field sampling updates, drift cleanup, cardinality persistence and ingestion upserts. Content Ossie can express SHALL be written as Ossie core; SLayer-only content (row filters, `hidden`, access tags, custom aggregations, query-backed or `sql`-mode sources, field formats and granularities, and any definition that is not valid Ossie core in its position) SHALL be written in a `SLAYER` entry of that entity's `nonportable_extensions`, never approximated in core. A relationship edited on a dataset SHALL be written to its model's top-level `relationships`, or, when it relates datasets of two models, to the `SLAYER` entry of the model holding its `from` dataset. Reading the patched documents SHALL yield the dataset exactly as written through the tool. A dataset created without a `semantic_model` SHALL belong to its datasource model (`models/stored-format`); on a backend that does not implement the semantic-model hooks it SHALL be standalone.

#### Scenario: Adding a SLayer-only filter to a document dataset
- **WHEN** `edit_dataset` adds the row filter `status <> 'test'` and `hidden: true` to the document dataset `orders`
- **THEN** the document's `orders` entry gains a `SLAYER` extension entry carrying the filter and the flag, its Ossie fields are unchanged, and `orders` reads back with the filter applied

#### Scenario: A new relationship lands at the document's top level
- **WHEN** `edit_dataset` adds a relationship from document dataset `orders` to document dataset `stores`
- **THEN** the document's top-level `relationships` gains that relationship and no relationship is written inside the `orders` entry

#### Scenario: A dataset without a document joins the datasource model
- **WHEN** `create_dataset` saves `returns` to datasource `shop` without a `semantic_model`
- **THEN** `returns` belongs to the datasource model `shop`

### Requirement: Surfaces create, list, inspect, edit and delete documents
MCP SHALL offer `create_semantic_model` (from Ossie document content, or from `datasets` naming datasets of any model of the datasource to move into the new document), `edit_semantic_model` (document fields, model-scoped metrics, relationships, and `add_datasets` moving datasets of any other model of the datasource in) and `delete_semantic_model`; `inspect` SHALL accept `entity_type="semantic_model"`, and `datasets_summary` SHALL show each dataset's document and each datasource's model-scoped metrics. REST SHALL expose `/semantic-models` (list, get by `{data_source}/{name}`, create, replace, delete); the Python client SHALL offer the same operations, sync and async; the CLI SHALL offer `slayer semantic-models list|show|delete`. Every surface SHALL treat the datasource model as a document. Moving a dataset between documents SHALL keep its content exactly, per "A dataset moves between models by its reference" of `models/stored-format`.

#### Scenario: Native authoring without an Ossie file
- **WHEN** a user with datasets `orders` and `customers` in the datasource model `shop` calls `create_semantic_model(name="sales", data_source="shop", datasets=["orders", "customers"])` and then `edit_semantic_model` adding the metric `revenue_per_customer`
- **THEN** both datasets answer queries exactly as before, now from document `sales`, and the metric answers by bare name
