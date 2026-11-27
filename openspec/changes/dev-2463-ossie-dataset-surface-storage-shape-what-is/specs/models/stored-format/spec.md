## Purpose

Defines what the YAML and SQLite stores write for datasets, relationships and models: Ossie model and dataset documents laid out as Ossie multi-file models, one datasource model per datasource, and the stored form of every Ossie construct, fixed so that a store never needs another format migration.

## ADDED Requirements

### Requirement: Every stored dataset is an Ossie dataset in its datasource's folder
The YAML store SHALL keep each datasource's datasets, relationships and models in `semantic_models/<data_source>/`, as Ossie documents recognised by content:
- a **model document** is a flat Ossie document (`datasets`) or a split model document (`dataset_refs`, optionally with inline `datasets`), as Ossie's multi-file models define it;
- a **dataset document** holds `version` and one `dataset`, and is listed in `dataset_refs` of exactly one model document of the same folder.

A dataset a save adds to a model SHALL be stored inline in its `datasets` when the model document is flat, and as a new dataset document listed in its `dataset_refs` when the model document is split; the datasource model is always split.

The SQLite store SHALL hold the same documents, as their text exactly as written, in a `documents` table keyed by datasource and the file name the YAML store would use. Datasource configs, the datasource priority, memories and embeddings are not part of this layout and SHALL keep their 2.0 storage.

#### Scenario: A standalone dataset is a dataset document
- **WHEN** `create_dataset` saves dataset `orders` of datasource `shop` without a `semantic_model`
- **THEN** `semantic_models/shop/orders.dataset.yaml` holds `version` and the dataset `orders` as an Ossie dataset, and the model document `shop` lists it in `dataset_refs`

#### Scenario: SQLite holds the same texts
- **WHEN** the same save runs against a SQLite store
- **THEN** the `documents` table holds the text of `orders.dataset.yaml` and of the model document `shop`, under those file names for datasource `shop`

### Requirement: The datasource model holds every dataset not in a named document
The model whose `name` equals its datasource's name SHALL be the datasource model. It SHALL hold every dataset of the datasource that no other model holds, each as a dataset document. Relationships among its datasets SHALL be its core `relationships`. A relationship between datasets of two different models SHALL be held in the `SLAYER` entry of the model document holding its `from` dataset. The datasource model SHALL be created by the first save that adds a dataset to it, and SHALL be deleted when its last dataset leaves it. It SHALL be a semantic model like any other: listed, inspected, edited (description, `ai_context`, model-scoped metrics, relationships) and deleted, with the same cascade, through every semantic-model surface. Creating a document whose name equals that of an existing datasource model (`create_semantic_model`, or an import adding a document) SHALL be refused as a name collision; replacing the datasource model's text SHALL be a replace like any other, and saving a document named like the datasource when it has no datasource model SHALL create it.

#### Scenario: A model-scoped metric over standalone datasets
- **WHEN** datasource `shop` holds datasets `orders` and `customers` in no named document, and `edit_semantic_model(name="shop", data_source="shop")` adds the metric `revenue_per_customer` with expression `SUM(orders.amount) / COUNT(DISTINCT customers.id)`
- **THEN** a query naming `revenue_per_customer` returns its value, and `semantic-models list` lists `shop`

#### Scenario: The last dataset leaving deletes the datasource model
- **WHEN** the only dataset of datasource model `shop` is deleted
- **THEN** the datasource model `shop` no longer exists

### Requirement: A dataset moves between models by its reference
Moving a dataset from one model of a datasource to another (`create_semantic_model` with `datasets` and `edit_semantic_model` with `add_datasets`, each taking a dataset from any model of the datasource, the datasource model included) SHALL move its `dataset_refs` entry and the relationships it takes part in, and SHALL leave the text of its dataset document unchanged. A dataset declared inline in a model document SHALL stay inline when the model is edited.

#### Scenario: A move keeps the dataset file
- **WHEN** `create_semantic_model(name="sales", data_source="shop", datasets=["orders"])` moves `orders` out of the datasource model `shop`
- **THEN** the text of `orders.dataset.yaml` is unchanged, `sales` lists it in `dataset_refs`, `shop` no longer does, and `orders` answers queries as before

#### Scenario: A move back into the datasource model
- **WHEN** `edit_semantic_model(name="shop", data_source="shop", add_datasets=["orders"])` runs while `orders` belongs to `sales`
- **THEN** `orders` belongs to the datasource model `shop`, and the text of its dataset document is unchanged

### Requirement: Stored file names are derived from names and recorded in the reference
A new model document SHALL be stored as `<stem>.semantic_model.yaml` and a new dataset document as `<stem>.dataset.yaml`. `<stem>` SHALL be the entity's name when the name is made only of ASCII letters, digits, `_` and `-`, and otherwise the name with every other UTF-8 byte percent-encoded. A stem longer than 120 bytes, one equal to a Windows-reserved device name, or one equal, ignoring case, to an existing entry of the folder SHALL be cut to fit and get `-` followed by the first 12 hexadecimal digits of the SHA-256 of the name. The file name SHALL be recorded, percent-encoded as a single path segment, as the dataset document's `iri`, and SHALL be kept while the entity keeps its name. A model document stored by 2.0 SHALL keep its file name.

#### Scenario: A name that is not a file name
- **WHEN** a dataset named `orders/2024 Q1` is saved
- **THEN** its file is `orders%2F2024%20Q1.dataset.yaml`, its `iri` is that file name percent-encoded once more, and the dataset reads back named `orders/2024 Q1`

#### Scenario: Names differing only in case
- **WHEN** datasets `"Orders"` and `orders` are saved to one datasource on a case-insensitive file system
- **THEN** they get two different files, each reached through its own `iri`

### Requirement: Writes to a datasource's documents are serialised and recoverable
Every write to a datasource's documents SHALL hold one lock per datasource folder in the YAML store, and SHALL run in one immediate transaction in the SQLite store, across choosing file names, checking ownership, and creating, patching, renaming, moving and deleting documents. A YAML write that changes several files SHALL record a journal in the folder before its first file write and remove it after its last. A load that finds a pending journal SHALL read the store as if the journaled operation had completed, without writing; the next write to the datasource, or `slayer storage upgrade`, SHALL complete it. A dataset document SHALL be written before the reference to it is added, and its reference SHALL be removed before the file is deleted.

#### Scenario: Concurrent creation in two models
- **WHEN** two saves create a dataset named `Orders` in model `sales` and a dataset named `orders` in the datasource model at the same time
- **THEN** both datasets are stored in different files and neither save overwrites the other's file

#### Scenario: An interrupted move
- **WHEN** a move of `orders` from model `shop` to model `sales` stops after any one of its file writes
- **THEN** the next load lists `orders` in `sales` only, and the next write to the datasource leaves the files in that state

### Requirement: A model that does not resolve is isolated
A model document whose datasets do not resolve SHALL be unloadable with a typed error naming the model document and each file involved, following `models/document-isolation`. That covers a `dataset_refs` entry whose `iri` is not one path segment of the folder; one naming a file that is missing, differs in case from the folder's entry, is not a regular file, or leads out of the folder; one naming a file that is not YAML or JSON, or not a dataset document; one whose dataset `version` or `name` differs from the model or the reference; and two references to one file. A dataset document listed by two models SHALL follow the collision rule of `models/semantic-model-documents`. A dataset document no model lists SHALL be ignored by loads, with a structured load warning naming its file.

#### Scenario: A reference to a missing file
- **WHEN** model `sales` lists `iri: returns.dataset.yaml` and that file does not exist
- **THEN** `sales` is unloadable with an error naming `sales` and `returns.dataset.yaml`, and the datasource's other models load

#### Scenario: A stray dataset document
- **WHEN** `semantic_models/shop/` holds `draft.dataset.yaml` that no model lists
- **THEN** no dataset `draft` is visible, and loading the datasource warns naming `draft.dataset.yaml`

### Requirement: Each Ossie construct is stored as Ossie defines it
Every dataset, field, metric, relationship and model SHALL be stored with its Ossie-expressible content in Ossie core and its SLayer-only content in its `SLAYER` entry, never approximated in core:
- `source`: a string, the table reference spelled per `models/table-names` or the query; the queries of a query-backed dataset and every row filter in the `SLAYER` entry;
- `primary_key` and `unique_keys` at dataset level, composite keys included;
- `nonportable_extensions` at model, dataset, field, metric and relationship level, every other vendor's entry kept as written;
- `ai_context` at every level, as written;
- `dimension.is_time` as written, and absent when it was never set;
- every `expression.dialects` entry, including entries for dialects SLayer cannot run; an entry SLayer derives in memory at load SHALL never be stored;
- `datatype` as written; a list type as `Opaque` with the list type in the `SLAYER` entry;
- a field's `label` in core; a metric's label in its `SLAYER` entry.

SLayer's versioning of what it stores SHALL be the `version` of each `SLAYER` payload; a document whose entities carry no `SLAYER` entry SHALL read as current. Each `SLAYER` entry SHALL be `{vendor_name: SLAYER, data}` with `data` holding `version` (4), `kind`, `entity`, `fingerprint` (fingerprint version 1, over the payload and the entity's core content) and, by kind:
- `field`: `field` with `expression`, `sql`, `filter`, `datatype`, `hidden`, `format`, `granularity`, `allowed_aggregations`, `meta`, `element_type`, `db_type`, `sampled`, `sampled_values`, `distinct_count`;
- `dataset`: `source` (`sql_table`, `source_kind`, `sql`, `filters`, `source_queries`, `query_variables`), `hidden`, `access_tags`, `default_time_dimension`, `meta`, `aggregations`, `fields`, `metrics`, `relationships` (SLayer-only members) and `enforced_keys`;
- `metric`: `dataset` and `metric`;
- `relationship`: `declarations`, each a `dataset` and a `relationship`;
- `semantic_model`: `metrics` (model-scoped metrics core cannot hold), `relationships` (relationships whose `from` dataset it holds and whose `to` dataset another model holds) and `documents` (boundaries of a merged export).

A version-3 payload SHALL read as version 4 without `source_kind`.

This shape SHALL NOT change without a new payload version that every later release reads.

#### Scenario: Constructs survive a round trip
- **WHEN** an Ossie document whose datasets carry a string `source`, a composite `primary_key`, a two-field `unique_keys` entry, a `SNOWFLAKE` extension on a field, `ai_context` on a relationship, `is_time: false` on a `Date` field, a `BIGQUERY` dialect entry, a `Decimal`, a `Time` and a `DateTimeTz` field and a field `label` is imported, stored in YAML and in SQLite, loaded, saved unchanged and exported
- **THEN** each of these appears in the export exactly as in the original

#### Scenario: A load's derived dialect entry is not stored
- **WHEN** a field whose only expression is a `SNOWFLAKE` entry is loaded on a DuckDB datasource, and its description is edited
- **THEN** the stored field holds its original expression and no other dialect entry

### Requirement: Names are stored as written
Every name SHALL be stored exactly as written, as any non-empty string, with no length limit. A dataset, field, metric or relationship whose name SLayer cannot reference yet SHALL load inert, with a structured load warning naming it, visible on `inspect` and `datasets_summary`: no query reads it, search does not list it, the rest of its model loads, and its text round-trips. A save SLayer authors that adds such a name SHALL be refused with a typed error naming it.

#### Scenario: A dotted field name round-trips
- **WHEN** an Ossie document declares a field named `amount.usd` beside a field `amount`
- **THEN** the document loads with a warning naming `amount.usd`, a query of `sum(orders.amount)` succeeds, and exporting reproduces `amount.usd` unchanged

### Requirement: Every stored model is Ossie-valid
For every model a store holds, its resolved document SHALL pass the vendored `ossie-schema.json` and upstream `validation/validate.py`, and each of its dataset documents' `dataset` SHALL pass the vendored schema's `Dataset` definition.

#### Scenario: A store passes upstream's validator
- **WHEN** a store holding a datasource model, a split named model and a flat named model is resolved model by model
- **THEN** upstream's validator reports no error for any resolved document

### Requirement: The format-freeze corpus loads with no migration
A committed corpus of a YAML store and a SQLite store written in this format, covering every construct of "Each Ossie construct is stored as Ossie defines it", the datasource model, a split and a flat named model, and names that are not file names, SHALL load in every later version with no upgrade warning, and a save of each unchanged document SHALL reproduce its stored text byte for byte.

#### Scenario: The corpus is current
- **WHEN** the format-freeze corpus is opened and each document is loaded and saved unchanged
- **THEN** no upgrade warning is emitted and every stored text is unchanged
