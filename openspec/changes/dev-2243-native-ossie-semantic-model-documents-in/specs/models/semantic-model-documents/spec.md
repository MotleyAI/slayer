## Purpose

Stores Ossie semantic-model documents natively, as written, and decomposes each into the native datasets, relationships and model-scoped metrics of its datasource, so that every valid Ossie document is valid SLayer configuration with Ossie's meaning.

## ADDED Requirements

### Requirement: A semantic-model document is a stored entity bound to its datasource by location
Storage SHALL persist a semantic-model document per datasource, identified by `(data_source, name)`: in YAML storage at `semantic_models/<data_source>/<name>.yaml` beside `models/<data_source>/`, and in SQLite storage in a `semantic_models` table holding the document text verbatim. The document's datasource SHALL be the one it is stored under; an Ossie document carries no connection. The document's `name` SHALL equal its storage name. A document SHALL be listable, readable as its stored text, replaceable and deletable through storage. The path components SHALL obey the existing datasource and dataset path rules.

#### Scenario: A document file binds to its directory's datasource
- **WHEN** `semantic_models/shop/sales.yaml` holds a valid Ossie document named `sales` declaring datasets `orders` and `customers`
- **THEN** the datasource `shop` lists the datasets `orders` and `customers`, and the document `sales` is listed for `shop`

#### Scenario: SQLite stores the text verbatim
- **WHEN** a document whose YAML text carries comments is saved to SQLite storage and read back
- **THEN** the text read back equals the text saved, comments included

### Requirement: Only Ossie's current document shape is accepted natively
A stored document SHALL be a flat Ossie document of version `0.3.0.dev0`: `version`, `name`, a non-empty `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics` and `nonportable_extensions`, with every nested entity in Ossie's shape. A key Ossie does not define, at any level, SHALL make the document unloadable with a typed error naming the document and the key; so SHALL an unknown `version`, the legacy `semantic_model` wrapper, and the legacy `custom_extensions` key. The error for an earlier shape SHALL name `slayer import-osi` as the upgrade path. An unloadable document SHALL follow `models/document-isolation`.

#### Scenario: An unknown key refuses the document
- **WHEN** a stored document's dataset carries the key `grain`
- **THEN** loading the datasource reports the document as unloadable, naming `grain`, and none of its datasets are visible

#### Scenario: A legacy wrapper names the upgrade path
- **WHEN** a file in `semantic_models/shop/` holds a `semantic_model:` wrapper
- **THEN** the document is unloadable with an error naming `slayer import-osi`

### Requirement: Ossie's version and SLayer's stored version never collide
A document's `version` SHALL be read only as the Ossie specification version. SLayer's stored-version counter for a document SHALL live in a document-level `nonportable_extensions` entry with `vendor_name: SLAYER`; a document without such an entry SHALL be read as current. Migrations SHALL apply only to `SLAYER` payloads, and a load SHALL never write the document back.

#### Scenario: A hand-written document needs no SLayer entry
- **WHEN** a document with `version: 0.3.0.dev0` and no `SLAYER` entry is loaded
- **THEN** it loads as current and its file is unchanged afterwards

### Requirement: Loading a document never introspects and never writes
Reading a document from storage SHALL NOT contact the datasource and SHALL NOT write to storage.

#### Scenario: Loading with the database unreachable
- **WHEN** the datasource's database is unreachable and the datasource's datasets are listed
- **THEN** the document's datasets are listed and no connection is attempted

### Requirement: Datasets decompose into native datasets exposing exactly their declared fields
Each Ossie dataset SHALL be visible as a native dataset of the document's datasource, through every operation that reads datasets (get, load, list, inspect, query, search), exposing exactly its declared fields. Its `source` SHALL be read as a table reference or a query as Ossie defines; each field's `expression` as the field's expression (`models/field-expressions`); `primary_key` as the primary-key flag on each named field (a composite key marks every member); a `unique_keys` entry naming one field as that field's `unique` flag; `dimension.is_time` and Ossie's default from `datatype` as the field's time role; dataset-scoped `metrics` as the dataset's metrics. A `unique_keys` entry naming several fields SHALL be kept in the document and SHALL NOT be used as a uniqueness proof. A dataset declaring no fields SHALL load exposing nothing. An entity's `description`, `label`, `ai_context` and non-portable extensions of any vendor SHALL be kept in the document unchanged.

#### Scenario: Undeclared source columns stay invisible
- **WHEN** dataset `orders` has source `sales.public.orders` with columns `o_orderkey` and `o_totalprice`, and declares only the field `order_id` with expression `o_orderkey`
- **THEN** `orders` exposes the field `order_id` only, and a query reading `orders.o_totalprice` fails as an unknown field

#### Scenario: A composite primary key marks every member
- **WHEN** a dataset declares `primary_key: [order_id, line_number]`
- **THEN** both fields carry the primary-key flag and neither is a sole identifier

### Requirement: Relationships decompose into named many-to-one relationships
Each top-level relationship SHALL become a relationship on its `from` dataset, named by the relationship's `name`, targeting `to`, keyed by `from_columns` and `to_columns` (names of declared fields), with cardinality `many_to_one` and a LEFT join. The declared cardinality SHALL prove the hop to-one as `models/join-cardinality` specifies. Relationships of standalone datasets SHALL keep their own declared cardinality. A relationship SHALL name only datasets of its own document.

#### Scenario: A declared relationship proves determination
- **WHEN** a document relates `orders` to `customers` by `orders_to_customers` on `[customer_id]` → `[id]`, and `customers` declares no primary key
- **THEN** a query of `sum(orders.amount)` by `customers.region` evaluates each order once, as attributable

### Requirement: Top-level metrics decompose into model-scoped metrics
Each top-level metric SHALL become a model-scoped metric of the document's datasource (`queries/model-scoped-metrics`), named by its `name`, with its `expression`, `description`, `datatype` and `ai_context` kept.

#### Scenario: A document metric answers by bare name
- **WHEN** a document declares the top-level metric `revenue_per_customer` with expression `SUM(orders.amount) / COUNT(DISTINCT customers.id)`
- **THEN** a query naming the metric `revenue_per_customer` returns that metric's value

### Requirement: Every construct the 1.x importer converted keeps its meaning natively
Each Ossie construct `slayer import-osi` converted in 1.x SHALL evaluate natively, from the stored expression, with the meaning the 1.x conversion gave it: `agg(x) FILTER (WHERE p)` for every supported aggregation (including `COUNT(*)`, `COUNT(DISTINCT x)` and percentiles) as the aggregation over the rows where `p` holds, never removing result groups; `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP`, `CORR`, `COVAR_SAMP`, `COVAR_POP` and `APPROX_COUNT_DISTINCT`; `ARRAY_AGG(DISTINCT x)` as `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` as `contains(a, v)`, `ARRAY_LENGTH(a)` and `CARDINALITY(a)` as `length(a)`, with SLayer's NULL conventions; non-bare aggregate operands; `COUNT(list_field)` counting non-NULL lists; date literals compared with temporal fields as instants (a date-only literal at its midnight); and `source.column` in dataset-scoped metrics. A field expression with no form SLayer can run on the datasource SHALL be translated in memory at load from the first readable candidate (top-level `sql`, an `OSSIE_SQL_2026` entry, an `ANSI_SQL` entry, then any readable SQL dialect entry, document order within a step) and SHALL NOT be written back. A construct the 1.x importer reported as unconverted SHALL load per "Entities SLayer cannot evaluate yet still load".

#### Scenario: A FILTER aggregate keeps every group
- **WHEN** a dataset metric `paid` has expression `SUM(amount) FILTER (WHERE status = 'paid')` and a query groups it by `orders.region` where one region has no paid orders
- **THEN** that region's row is present with NULL for `orders.paid`

#### Scenario: An array function maps to the list library
- **WHEN** a field `tag_count` has expression `CARDINALITY(tags)`
- **THEN** it returns the count of non-NULL elements of `tags`, and NULL for a NULL list

### Requirement: Entities SLayer cannot evaluate yet still load
A field or metric of a valid document whose expression SLayer cannot evaluate yet — a window function, or a model-scoped metric Ossie itself rejects at query time — SHALL load with a structured load warning naming the entity and the reason, visible on `inspect`, `datasets_summary` and search results. Every query reading the entity SHALL fail with a typed error naming it and the reason; no other entity SHALL be affected.

#### Scenario: A windowed field loads and fails only when read
- **WHEN** a document declares a field `running_total` with expression `SUM(amount) OVER (ORDER BY order_date)` beside a field `amount`
- **THEN** the document loads, `inspect` shows the warning on `running_total`, a query of `sum(sales.amount)` succeeds, and a query reading `sales.running_total` fails with a typed error naming `running_total`

### Requirement: Dataset and model-scoped metric names are unique per datasource
Within one datasource, dataset names SHALL be unique across all documents and standalone datasets together, and model-scoped metric names SHALL be unique across all documents; a model-scoped metric name SHALL NOT equal a dataset name. Names SHALL compare as `queries/identifier-case` specifies. A save that would introduce a duplicate SHALL be refused with a typed error naming both owners. When stored files collide, the colliding entity SHALL be unloadable in every owner, and so SHALL every entity of the same document that depends on it (a relationship to or from it, a metric reading it), each with a diagnostic naming the cause; every other entity SHALL load. Nothing SHALL silently win a collision.

#### Scenario: A colliding save is refused
- **WHEN** the datasource holds a standalone dataset `orders` and a document declaring a dataset `ORDERS` is saved
- **THEN** the save fails with a typed error naming the standalone dataset and the document

#### Scenario: Hand-placed collisions quarantine dependents
- **WHEN** two stored documents both declare a dataset `orders`, and the first also declares a relationship from `orders` to `customers` and a metric reading `orders.amount`
- **THEN** `orders` is unloadable in both documents, the relationship and the metric are unloadable with diagnostics naming the collision, and `customers` loads

### Requirement: Dataset tools edit a document's datasets through the document
Every operation that writes a dataset SHALL determine whether the dataset is standalone or owned by a document, and for an owned dataset SHALL patch only the affected entities of the owning document: `create_dataset` with a `semantic_model`, `edit_dataset`, `delete_dataset`, the REST and CLI dataset writes, field sampling updates, drift cleanup, cardinality persistence and ingestion upserts. Content Ossie can express SHALL be written as Ossie core; SLayer-only content (row filters, `hidden`, access tags, custom aggregations, query-backed or `sql`-mode sources, field formats and granularities, and any definition that is not valid Ossie core in its position) SHALL be written in a `SLAYER` entry of that entity's `nonportable_extensions`, never approximated in core. A relationship edited on a document dataset SHALL be written to the document's top-level `relationships`. Reading the patched document SHALL yield the dataset exactly as written through the tool. A dataset created without a `semantic_model` SHALL be standalone.

#### Scenario: Adding a SLayer-only filter to a document dataset
- **WHEN** `edit_dataset` adds the row filter `status <> 'test'` and `hidden: true` to the document dataset `orders`
- **THEN** the document's `orders` entry gains a `SLAYER` extension entry carrying the filter and the flag, its Ossie fields are unchanged, and `orders` reads back with the filter applied

#### Scenario: A new relationship lands at the document's top level
- **WHEN** `edit_dataset` adds a relationship from document dataset `orders` to document dataset `stores`
- **THEN** the document's top-level `relationships` gains that relationship and no relationship is written inside the `orders` entry

### Requirement: Saves keep the document's text outside what was edited
A save SHALL change only the nodes of the document that the edit changes. Comments, key order, quoting, blank lines and formatting of every untouched part SHALL be kept, in YAML and SQLite storage alike; a JSON document SHALL keep its key order.

#### Scenario: A comment survives an unrelated edit
- **WHEN** a document carries the comment `# owned by finance` above dataset `customers`, and `edit_dataset` changes the description of dataset `orders`
- **THEN** the saved text differs from the original only in the description of `orders`

### Requirement: Concurrent writes to one document never lose an update
Writes to one document SHALL be serialised: each write SHALL read, patch and store the document as one atomic step against the latest stored text, in YAML storage under a per-document lock and in SQLite storage in one immediate transaction.

#### Scenario: Two edits to different datasets of one document
- **WHEN** two `edit_dataset` calls change the descriptions of datasets `orders` and `customers` of one document concurrently
- **THEN** the stored document carries both new descriptions

### Requirement: Document writes invalidate every graph cache
Any change to a document's content or membership, by SLayer or by an external edit of the stored file, SHALL change the storage's `graph_fingerprint`, so engine bundles and access-filtered views rebuild. A custom backend implementing the semantic-model hooks SHALL meet the same contract.

#### Scenario: An external edit is seen on the next query
- **WHEN** a document file is edited outside SLayer to add a field, and a query reads the new field
- **THEN** the query resolves the new field without restarting the engine

### Requirement: Deleting document entities runs the dataset cascades
Deleting a document dataset SHALL run the same cascade as deleting a standalone dataset (its embeddings, its memory entities, relationships of other datasets that target it) and SHALL remove its relationships from the document; model-scoped metrics reading it SHALL get the broken-dependent warning saves give today. Deleting a document SHALL delete each of its datasets through that cascade, plus the embeddings of the document and of its model-scoped metrics.

#### Scenario: Deleting a document dataset strips memory entities
- **WHEN** a memory links the entity `shop.orders` and the document dataset `orders` is deleted
- **THEN** the memory no longer lists `shop.orders` and the document no longer declares `orders` or its relationships

### Requirement: Access filtering covers documents
An access-filtered view SHALL read every document dataset the caller cannot access as deleted, prune relationships into it, and prune every model-scoped metric that reads it through any aggregate component or through other model-scoped metrics, transitively. A pruned metric SHALL fail as an unknown metric with no suggestion naming a hidden entity. A write through the view SHALL merge onto the full document, never dropping or exposing hidden entities.

#### Scenario: A metric over a hidden dataset is invisible
- **WHEN** a caller cannot access dataset `payroll`, and the model-scoped metric `cost_per_head` reads `payroll.salary`
- **THEN** the caller's query naming `cost_per_head` fails as an unknown metric that suggests no hidden name

### Requirement: Fields without a datatype load with an unknown type
A field that declares no `datatype` SHALL load with an unknown type that no check or coercion reads. The engine SHALL resolve unknown types before binding a query that reads them, once per engine and storage fingerprint, without blocking the event loop, from the live type of the field's expression; the resolved type SHALL NOT be persisted. A save-time check that needs an unknown type SHALL be deferred to the query-time check.

#### Scenario: An untyped numeric field aggregates
- **WHEN** a document field `amount` with expression `amount` declares no `datatype` and its source column is a decimal
- **THEN** `sum(orders.amount)` returns the column's sum and the document's text is unchanged

### Requirement: Surfaces create, list, inspect, edit and delete documents
MCP SHALL offer `create_semantic_model` (from Ossie document content, or from `datasets` naming standalone datasets to move into the new document), `edit_semantic_model` (document fields, model-scoped metrics, relationships, and `add_datasets` moving standalone datasets in) and `delete_semantic_model`; `inspect` SHALL accept `entity_type="semantic_model"`, and `datasets_summary` SHALL show each dataset's document and each datasource's model-scoped metrics. REST SHALL expose `/semantic-models` (list, get by `{data_source}/{name}`, create, replace, delete); the Python client SHALL offer the same operations, sync and async; the CLI SHALL offer `slayer semantic-models list|show|delete`. Moving a standalone dataset into a document SHALL keep its content exactly, through the mapping of "Dataset tools edit a document's datasets through the document".

#### Scenario: Native authoring without an Ossie file
- **WHEN** a user with standalone datasets `orders` and `customers` calls `create_semantic_model(name="sales", data_source="shop", datasets=["orders", "customers"])` and then `edit_semantic_model` adding the metric `revenue_per_customer`
- **THEN** both datasets answer queries exactly as before, now from document `sales`, and the metric answers by bare name

### Requirement: Search indexes documents and model-scoped metrics
Search SHALL index each document (its name, `description` and `ai_context`) and each model-scoped metric (its name, description and `ai_context`, canonical id `<data_source>.<metric>`), and SHALL refresh them on the same events that refresh datasets.

#### Scenario: A model-scoped metric is found by its synonym
- **WHEN** model-scoped metric `revenue_per_customer` carries the `ai_context` synonym `ARPU` and a search asks for `ARPU`
- **THEN** the results include `shop.revenue_per_customer`

### Requirement: The Ossie corpus loads, compiles and round-trips
A corpus vendored with provenance pinning the upstream apache/ossie commit SHALL consist of every semantic-model document of upstream `examples/`, `compliance/portability/` (m0–m8), the `compliance/wide_table/` models and `compliance/query/scott`, plus every document upstream's `validation/tests` assert as valid, harvested into fixture files. For every corpus document: upstream `validation/validate.py` SHALL pass; the native load SHALL succeed; every dataset-scoped and model-scoped metric SHALL bind and compile to SQL, except entities SLayer cannot evaluate yet, which SHALL raise their typed error; storing, loading, saving unchanged and exporting a single-document datasource SHALL each reproduce the document's text byte for byte.

#### Scenario: Every corpus document round-trips
- **WHEN** each corpus document is imported into its own empty datasource and exported
- **THEN** the export equals the original text byte for byte

### Requirement: Upstream's wide-table cases are the query oracle
Every case of upstream `compliance/wide_table/cases` SHALL run against its named fixture model, stored as a native document over its SQL data loaded into in-process DuckDB. A row case (`a*`) SHALL return exactly the expected rows under the default conformance; a case that depends on the root-less row rule SHALL be a strict expected failure naming DEV-2212. An error case (`e*`) SHALL be a strict expected failure naming DEV-2217 until the strict binding raises Ossie's error codes.

#### Scenario: The model-scoped metric components case
- **WHEN** case `a08_model_metric_components` runs on the `retail` fixture
- **THEN** the result equals its expected rows

### Requirement: SLayer's round trip grades portable on upstream's ladder
`slayer ossie-roundtrip <in> <out>` SHALL import the input document natively into scratch storage and export it to `<out>`. Upstream's portability grader, vendored with the corpus, SHALL grade every element of rungs m0–m8 `PORTABLE` through it.

#### Scenario: The ladder grades portable
- **WHEN** the grader runs over m0–m8 through `slayer ossie-roundtrip`
- **THEN** every element is graded `PORTABLE`
