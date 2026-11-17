## Purpose

SLayer's names in Ossie's vocabulary (dataset, field, metric, relationship, expression, population) across stored documents, queries, Python names and every entry point, and how each pre-2.0 spelling keeps working, with a deprecation, until SLayer 3.0.

## ADDED Requirements

### Requirement: Datasets are written in Ossie's vocabulary

A dataset SHALL hold its fields under `fields`, its metrics under `metrics` and its relationships under `relationships`. A field and a metric SHALL carry their type under `datatype`, a metric and a custom aggregation their definition under `expression`, and a relationship its target dataset under `to`. A dataset's `filters`, `aggregations`, `sql`, `sql_table`, `source_queries`, a relationship's `join_pairs`, and a field's `sql` SHALL keep their names.

#### Scenario: A dataset is dumped
- **WHEN** a dataset with a field, a metric, a custom aggregation and a relationship is serialized
- **THEN** it carries `fields`, `metrics`, `relationships`, `datatype`, `expression` and `to`, and none of `columns`, `measures`, `joins`, `type`, `formula` or `target_model`

#### Scenario: Kept keys are unchanged
- **WHEN** a dataset with row filters and a relationship is serialized
- **THEN** it carries `filters` and the relationship's `join_pairs` under those names

### Requirement: Queries are written in Ossie's vocabulary

A query SHALL name its population `population`, its metrics `metrics` and its row filters `filter` (a list of conditions, as before). A query's metric entry SHALL carry its definition under `expression`, a time dimension and an order item SHALL name what they read `field`, a field reference SHALL name its dataset `dataset`, and a query extension SHALL carry `metrics` and `relationships`. This SHALL hold for a query, every stage of a multi-stage query, a nested query, a refinement and a REST query request.

#### Scenario: A query is dumped
- **WHEN** a two-stage query with a population, metrics, a filter, a time dimension, an order item and an extension is serialized
- **THEN** every stage carries `population`, `metrics`, `filter`, the time dimension's and order item's `field`, and the extension's `metrics` and `relationships`, and none of `source_model`, `measures`, `filters`, `dimension`, `column` or `joins`

#### Scenario: A refinement uses the new keys
- **WHEN** a saved query is run by name with a refinement carrying `metrics` and `filter`
- **THEN** the refinement is applied as before the rename

### Requirement: The rename changes no result

A query SHALL return the same rows, values, result keys and column order whether it is written in the pre-2.0 or the Ossie vocabulary, and the same as before this change.

#### Scenario: Old and new spellings give the same result
- **WHEN** a query is run once in the pre-2.0 spelling and once in the new spelling
- **THEN** both responses carry identical rows, values and result keys, equal to those recorded before the rename

#### Scenario: A multi-stage query keeps its keys
- **WHEN** a multi-stage query with metric entries, field references and an order item is run in both spellings
- **THEN** both responses carry identical result keys, equal to those recorded before the rename

#### Scenario: A refinement matches the same entries
- **WHEN** a saved query is run by name with a refinement written in the pre-2.0 spelling and with one written in the new spelling
- **THEN** both runs return identical results

### Requirement: Response metadata names metrics

A response's `attributes` SHALL describe the query's metric columns under `metrics`, and a serialized response SHALL carry no `measures` key in `attributes`. The response's `columns` SHALL keep its name. The HTTP client SHALL read `attributes.metrics`.

#### Scenario: Attributes name metrics
- **WHEN** a query with one metric is run and its response is serialized
- **THEN** `attributes.metrics` describes that metric's column and `attributes` has no `measures` key

#### Scenario: The HTTP client reads metrics
- **WHEN** the HTTP client runs a query with one metric against the REST server
- **THEN** its returned response's `attributes.metrics` describes that metric's column

### Requirement: Stored documents migrate to the new vocabulary

A document is stored when it carries a `version` key or is nested in a stored document; any other input is fresh. A stored dataset, saved query or memory written in the pre-2.0 vocabulary SHALL load as its Ossie-vocabulary twin with no deprecation warning, including queries nested in a dataset's `source_queries` or in a memory, whether or not the nested query carries a version of its own. A migrated stored document SHALL be persisted as every other migrated document of its kind is. A stored embedding row of kind `model`, `column` or `measure` SHALL become kind `dataset`, `field` or `metric` without being re-embedded and with its content hash unchanged, in both storage backends, and SHALL stay so across restarts.

#### Scenario: A pre-2.0 dataset loads
- **WHEN** a store holding a dataset with `columns`, `measures`, `joins` (with `target_model`), `type` and `formula` is opened
- **THEN** the dataset loads with `fields`, `metrics`, `relationships` (with `to`), `datatype` and `expression`, and no `FutureWarning` is emitted

#### Scenario: A nested stored query without a version
- **WHEN** a stored dataset's `source_queries` holds a query with `source_model` and `measures` and no `version` key
- **THEN** it loads with `population` and `metrics`, and no `FutureWarning` is emitted

#### Scenario: A memory's query migrates
- **WHEN** a stored memory holds a query with `filters` and a metric entry carrying `formula`
- **THEN** the query loads with `filter` and a metric entry carrying `expression`, and no `FutureWarning` is emitted

#### Scenario: Stored embedding kinds are renamed in place
- **WHEN** a YAML and a SQLite store whose embeddings hold rows of kinds `model`, `column` and `measure` are opened, closed and opened again
- **THEN** every row reads with kind `dataset`, `field` or `metric`, no entity is re-embedded, and every content hash is unchanged

#### Scenario: The 1.x upgrade stores still open
- **WHEN** the stored fixtures written by 0.10.2 and by 1.8.2 are opened
- **THEN** every document loads in the new vocabulary and its queries return the results they returned before

### Requirement: Pre-2.0 spellings in fresh input convert with a deprecation

Fresh input carrying a pre-2.0 spelling SHALL be converted to its Ossie-vocabulary twin, with exactly one `FutureWarning` and one structured `deprecated_input` warning per distinct pre-2.0 spelling per input document, however often that spelling recurs across the document's nested parts (fields, metrics, relationships, extensions, stages, a refinement), each naming the old spelling, its replacement and SLayer 3.0. The converted input SHALL behave, and be stored, exactly as the new spelling. This covers every dataset key and query key renamed here, a metric entry's `formula`, a field reference's `model`, a time dimension's `dimension` and `column`, and an order item's `column`.

#### Scenario: An old query key converts
- **WHEN** a query is submitted with `source_model: "orders"` and `measures: ["sum(amount)"]`
- **THEN** it runs as the query with `population: "orders"` and `metrics: ["sum(amount)"]`
- **AND** the response carries one `deprecated_input` warning for `source_model` and one for `measures`, and one `FutureWarning` is emitted for each

#### Scenario: An old key used many times warns once
- **WHEN** a dataset given as fresh input has five fields that each spell `type`
- **THEN** exactly one `FutureWarning` and one `deprecated_input` warning name `type`

#### Scenario: An old key used across nested parts warns once
- **WHEN** a fresh two-stage query spells `measures` in both stages and in a query extension
- **THEN** the response carries exactly one `deprecated_input` warning naming `measures`, and exactly one `FutureWarning` names it

#### Scenario: Two old forms across nested parts warn once each
- **WHEN** a fresh dataset spells `columns` at its root and `type` on three of its fields
- **THEN** exactly two `FutureWarning`s and two `deprecated_input` warnings are emitted, one naming `columns` and one naming `type`

#### Scenario: An old key repeated in a refinement warns once
- **WHEN** a saved query is run by name with a refinement whose two metric entries each spell `formula`
- **THEN** the response carries exactly one `deprecated_input` warning naming `formula`, and exactly one `FutureWarning` names it

#### Scenario: An old field reference converts
- **WHEN** a query has the dimension entry `{"name": "region", "model": "customers"}`
- **THEN** it runs as `{"name": "region", "dataset": "customers"}` with a `deprecated_input` warning naming `model`

#### Scenario: A converted dataset is saved in the new vocabulary
- **WHEN** a dataset written with `columns` and `target_model` is saved through any surface
- **THEN** the stored document has `fields` and `to`, and the save reports the `deprecated_input` warnings

#### Scenario: A save surface reports the warning
- **WHEN** a dataset with `measures` is created through the MCP tool, the REST route and the Python client
- **THEN** each reports one `deprecated_input` warning naming `measures` and `metrics`

### Requirement: An old and a new spelling together are refused

Input that supplies both the pre-2.0 spelling and the Ossie-vocabulary spelling of one concept, in one object, SHALL be refused with a typed error naming both and saying to keep only the new one.

#### Scenario: Both query keys are given
- **WHEN** a query has both `source_model` and `population`
- **THEN** it is refused with a typed error naming `population` and `source_model`

#### Scenario: Both dataset keys are given
- **WHEN** a dataset has both `columns` and `fields`
- **THEN** it is refused with a typed error naming `fields` and `columns`

### Requirement: Python names follow the vocabulary

The public Python names SHALL follow the vocabulary, among them `SlayerDataset`, `DatasetField`, `DatasetMetric`, `DatasetRelationship`, `DatasetExtension`, `FieldRef`, `DatasetFieldCycleError` and `PopulationRecommendation`, and the public methods of the engine, the storage backends and the HTTP client SHALL say `dataset`, `field` and `population` where they said `model`, `column` and `root model`. Until 3.0 each pre-2.0 class, method and attribute name on SLayer's published list of deprecated names SHALL keep working: a class name SHALL resolve to its replacement, a method SHALL delegate to its replacement, and an attribute SHALL read and write its replacement, each emitting one `FutureWarning` naming the replacement and SLayer 3.0. Each listed name SHALL also be declared deprecated to type checkers, typed as its replacement, and SLayer's own type check SHALL fail on any use of one. A module path renamed here SHALL raise an `ImportError` naming its new path.

#### Scenario: An old class name still imports
- **WHEN** code imports `SlayerModel` from `slayer.core.models`
- **THEN** it receives `SlayerDataset`, and one `FutureWarning` names `SlayerDataset` and SLayer 3.0

#### Scenario: An old method still works
- **WHEN** code calls the storage backend's `get_model` and the engine's `recommend_root_model`
- **THEN** each returns what `get_dataset` and `recommend_population` return, and each emits one `FutureWarning`

#### Scenario: Old attribute names still read
- **WHEN** code reads `.columns` on a dataset and `.source_model` on a query
- **THEN** it receives the values of `.fields` and `.population`, each with one `FutureWarning`

#### Scenario: Old attribute names still write
- **WHEN** code assigns to `.columns` on a dataset
- **THEN** `.fields` holds the assigned value, one `FutureWarning` is emitted, and the dataset's dump carries `fields` and no `columns`

#### Scenario: Every listed name works
- **WHEN** every class, method and attribute name on the published list is used
- **THEN** each reaches its replacement with one `FutureWarning`

#### Scenario: A type checker flags old names
- **WHEN** a module that imports `SlayerModel`, reads `.columns` on a dataset and calls `get_model` on a storage backend is type-checked under SLayer's configuration
- **THEN** each of the three uses is reported as deprecated, naming its replacement

#### Scenario: A renamed module path names its new location
- **WHEN** `slayer.core.join_walker` is imported
- **THEN** an `ImportError` is raised whose message names `slayer.core.join_walker`, its new path and SLayer 2.0

### Requirement: Custom storage backends written for 1.x keep working

A storage backend subclass that overrides the pre-2.0 method names SHALL keep working until 3.0: calls to the new method names SHALL reach its overrides, with one `FutureWarning` per backend class naming the methods to rename.

#### Scenario: A 1.x backend is used unchanged
- **WHEN** a backend subclass overriding `save_model`, `get_model`, `list_models` and `delete_model` is given to the engine
- **THEN** saving, reading, listing and deleting a dataset go through those overrides
- **AND** exactly one `FutureWarning` is emitted for that class

### Requirement: Surfaces follow the vocabulary

The MCP server SHALL register the tools `datasets_summary`, `create_dataset`, `edit_dataset`, `delete_dataset`, `validate_datasets`, `recommend_population` and `ingest_datasource`, and SHALL NOT register their pre-2.0 names. Inside every tool, a pre-2.0 argument name (`model_name`, `model`, `columns`, `measures`, `joins`, `source_model`), a pre-2.0 `inspect` kind or section, and a pre-2.0 help reference (`memory:help.models`, `memory:help.joins`) SHALL convert with a `deprecated_input` warning. The REST API SHALL serve its dataset routes under `/datasets`, and the CLI SHALL offer `datasets`, `validate-datasets`, `recommend-population`, `ingest` and the flags `--dataset`, `--datasets` and `--include-hidden-datasets`. Until 3.0 each pre-2.0 REST route, CLI command and CLI flag SHALL behave as its replacement and report a deprecation: the route with a `Deprecation` response header, plus a `deprecated_input` warning wherever its response carries warnings; the command or flag with one `Deprecated: …` line on stderr. Help topics, tool descriptions and messages SHALL use the new words.

#### Scenario: Only the new tools are listed
- **WHEN** the MCP tool list is read
- **THEN** it contains the seven new tool names and none of `models_summary`, `create_model`, `edit_model`, `delete_model`, `validate_models`, `recommend_root_model` or `ingest_datasource_models`

#### Scenario: An old inspect kind converts
- **WHEN** `inspect` is called with the kind `model`
- **THEN** it returns what the kind `dataset` returns, with a `deprecated_input` warning

#### Scenario: An old tool argument converts
- **WHEN** `edit_dataset` is called with `model_name` and `columns`
- **THEN** it behaves as with `dataset_name` and `fields`, and reports a `deprecated_input` warning for each

#### Scenario: An old help reference resolves
- **WHEN** `inspect` is called with the reference `memory:help.joins`
- **THEN** it returns the topic `memory:help.relationships`, with a `deprecated_input` warning

#### Scenario: Retired help topics leave a warm store
- **WHEN** a store seeded before the rename, holding `help.models` and `help.joins`, is opened and reseeded
- **THEN** neither topic nor its embeddings remain, and a search returns `help.datasets` and `help.relationships` in their place

#### Scenario: An old REST route still answers
- **WHEN** `GET /models` is requested
- **THEN** it returns what `GET /datasets` returns, with a `Deprecation` response header

#### Scenario: An old CLI command still runs
- **WHEN** `slayer models list` is run
- **THEN** it prints what `slayer datasets list` prints, and stderr carries one `Deprecated: …` line

### Requirement: Search speaks the vocabulary

Search results SHALL report the kinds `dataset`, `field` and `metric`, and the search graph SHALL label its nodes `Dataset`, `Field`, `Metric` and `DatasetField` where it labelled them `Model`, `Column`, `Measure` and `ModelColumn`. Until 3.0 a kind filter or a Cypher filter that names a pre-2.0 kind or label SHALL be read as its replacement, with a `deprecated_input` warning.

#### Scenario: Results carry the new kinds
- **WHEN** a search matches a dataset, one of its fields and one of its metrics
- **THEN** the hits' kinds are `dataset`, `field` and `metric`

#### Scenario: An old kind filter converts
- **WHEN** a search is restricted to the kind `column`
- **THEN** it returns what a search restricted to `field` returns, with a `deprecated_input` warning

#### Scenario: An old label in Cypher converts
- **WHEN** a search carries a Cypher filter naming the label `ModelColumn`
- **THEN** it is evaluated as the same filter naming `DatasetField`, with a `deprecated_input` warning

### Requirement: Warnings use the vocabulary

Every structured warning's payload fields and `kind` value SHALL use the new words, among them the kinds `extension_field_inlined` and `spine_relationship_dropped`.

#### Scenario: A warning kind is renamed
- **WHEN** a stored dataset whose relationship into the time spine is dropped is loaded
- **THEN** the warning emitted carries the kind `spine_relationship_dropped` and names the `dataset` and `relationship`

### Requirement: The Ossie export payload carries the vocabulary

The `SLAYER` vendor payload of an exported Ossie document SHALL be written at payload version 2, in the Ossie vocabulary. The importer SHALL read payload versions 1 and 2 with the same result: a version-1 payload's fingerprint SHALL be checked against its content as written, and its pre-2.0 keys SHALL then convert with no deprecation warning.

#### Scenario: An export uses the new keys
- **WHEN** a dataset with a relationship, a SLayer-only field and a SLayer-only metric is exported to Ossie
- **THEN** every `SLAYER` payload is version 2 and carries no pre-2.0 key

#### Scenario: A pre-2.0 export still imports
- **WHEN** an Ossie document exported before this change (payload version 1) is imported
- **THEN** every payload's fingerprint matches, no `FutureWarning` is emitted, and the imported datasets equal those imported from the same datasets exported at version 2
