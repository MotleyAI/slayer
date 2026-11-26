# storage/custom-backends Specification

## Purpose
Defines the contract a third-party `StorageBackend` subclass relies on: the hooks it implements and the shapes they return.

## Requirements

### Requirement: The library-granularity settle reads only stored datasource documents

The one-time step that renames library-named granularities before a backend's first model or memory load SHALL read the backend's stored datasource documents directly, and SHALL NOT call `list_datasources` or `get_datasource`. A backend that exposes no stored datasource documents, which is the default for a `StorageBackend` subclass, SHALL settle nothing, without overriding any hook. The YAML and SQLite stores SHALL expose exactly the datasource documents their `get_datasource` reads, and SHALL still rename before the first model or memory load. A stored datasource document that cannot be parsed, or that parses to a mapping which is not a valid datasource, SHALL NOT prevent the settle from renaming another datasource.

#### Scenario: A backend that builds its own datasources pays nothing

- **WHEN** a `StorageBackend` subclass that overrides `get_datasource` and `list_datasources` and stores no raw datasource documents loads models and memories through the base load pipeline, model first on one fresh instance and memory first on another
- **THEN** the settle runs on each instance and neither `list_datasources` nor `get_datasource` is called

#### Scenario: The built-in stores still rename first

- **WHEN** a YAML or SQLite store whose datasource defines the granularity `contains` loads a model, or a memory, before anything else
- **THEN** the loaded document references `contains_granularity` and the stored datasource defines `contains_granularity`

#### Scenario: An unparseable datasource document does not block the settle

- **WHEN** a YAML or SQLite store holds a datasource document that does not parse beside a datasource defining `contains`, and loads a model first
- **THEN** the load succeeds and `contains` is renamed to `contains_granularity`

#### Scenario: An invalid datasource mapping does not block the settle

- **WHEN** a YAML or SQLite store holds a datasource document that parses to a mapping which is not a valid datasource beside a datasource defining `contains`, and loads a model first
- **THEN** the load succeeds and `contains` is renamed to `contains_granularity`

#### Scenario: A YAML datasource file the store does not read is left alone

- **WHEN** a YAML store holds `datasources/shop.yml` defining the granularity `contains` and no `datasources/shop.yaml`, and loads a model first
- **THEN** `shop.yml` is unchanged and no `shop.yaml` is created

### Requirement: A backend persists datasources through its write hook

`save_datasource` SHALL validate the datasource's custom granularities and then call the backend's `_save_datasource_impl`. A `StorageBackend` subclass SHALL be instantiable only when a class before `StorageBackend` in its MRO implements `_save_datasource_impl`. A `save_datasource` override SHALL run as written and SHALL NOT be adapted, and defining a backend SHALL emit no `FutureWarning`.

#### Scenario: A new-style backend is untouched
- **WHEN** a `StorageBackend` subclass implementing `_save_datasource_impl` is defined
- **THEN** no `FutureWarning` is emitted and `save_datasource` runs the base template

#### Scenario: A save_datasource override on a bundled-backend subclass is untouched
- **WHEN** a subclass of a bundled backend overrides `save_datasource` and calls `super().save_datasource()`
- **THEN** no `FutureWarning` is emitted and the override runs as written, reaching the bundled backend's write through the base template

#### Scenario: A 0.9.x-style backend does not instantiate
- **WHEN** a `StorageBackend` subclass implements the 0.9.12 hooks, including `save_datasource`, without `_save_datasource_impl`, and is instantiated
- **THEN** instantiation raises a `TypeError` naming the abstract `_save_datasource_impl`

### Requirement: _list_memories_rows returns memories and failed rows

A backend's `_list_memories_rows` SHALL return `(memories, failed_rows)`; `load_memories` SHALL return that pair, and `list_memories` SHALL return the memories and skip and report the failed rows.

#### Scenario: A new-style backend keeps per-document failure reporting
- **WHEN** a backend whose `_list_memories_rows` returns `(rows, failed_rows)` holds one loadable and one unloadable memory
- **THEN** no `FutureWarning` is emitted, `load_memories` returns the loadable memory with the unloadable one as a failed row, and `list_memories` skips and reports it

### Requirement: Semantic-model hooks default to unsupported
A backend SHALL persist semantic-model documents through hooks to save a document's text, read it, list document identities, delete a document, and read, patch and store one document atomically. A backend that does not implement them SHALL keep working for datasets, datasources and memories: listing documents SHALL return none, and every operation that would write or read a document SHALL fail with a typed error naming the backend and stating that it does not support semantic-model documents. A backend implementing the hooks SHALL change its `graph_fingerprint` whenever a document's content or membership changes.

#### Scenario: A backend without the hooks
- **WHEN** a custom backend registered with `register_storage` implements only the dataset, datasource and memory hooks, and `create_semantic_model` is called against it
- **THEN** its datasets still load and answer queries, listing documents returns none, and `create_semantic_model` fails with the typed unsupported error naming the backend

### Requirement: Ontology and mapping hooks default to unsupported
A backend SHALL persist ontologies and mappings through hooks to save a document's text, read it, list document identities, and delete a document, for ontologies keyed by name and for mappings keyed by datasource and name. A backend that does not implement them SHALL keep working for datasets, datasources, semantic-model documents and memories: listing ontologies or mappings SHALL return none, and every operation that would write or read one SHALL fail with a typed error naming the backend and stating that it does not support ontology documents. A backend implementing the hooks SHALL change its `graph_fingerprint` whenever an ontology or mapping changes.

#### Scenario: A backend without the ontology hooks
- **WHEN** a custom backend registered with `register_storage` implements the dataset, datasource, memory and semantic-model hooks only, and `create_ontology` is called against it
- **THEN** its datasets and semantic-model documents still load, listing ontologies returns none, and `create_ontology` fails with the typed unsupported error naming the backend
