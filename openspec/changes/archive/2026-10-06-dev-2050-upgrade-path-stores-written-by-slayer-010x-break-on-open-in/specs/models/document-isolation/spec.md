## Purpose

Defines what happens when one stored model or memory cannot be loaded. The failure is typed and names the document. Operations that enumerate documents carry on without it and warn. Operations that pick an answer among documents fail closed.

## ADDED Requirements

### Requirement: A document that cannot be loaded fails with a typed error naming it

Any failure to load one stored model or memory SHALL raise a typed stored-document load error. This covers decoding, migration, live type refinement, validation, and a malformed version. The message SHALL name the document (`<data_source>.<name>` for a model, the memory id for a memory) and the cause, and the original error SHALL be chained as its cause. The error SHALL be a `ValueError`.

#### Scenario: Loading a broken model names it

- **WHEN** a stored model fails validation on load
- **THEN** loading it raises the stored-document load error naming its datasource and name, with the validation error as its cause

#### Scenario: Loading a corrupt document names it

- **WHEN** a stored model's YAML or JSON cannot be decoded
- **THEN** loading it raises the stored-document load error naming it

### Requirement: Enumerating operations skip an unloadable document and warn

Every operation that enumerates stored models or memories SHALL skip a document that cannot be loaded and carry on with the rest. This includes save-time join checks, ingestion and re-ingestion, model and memory listings, catalogs, search, summaries, and built-in model wiring.

The operation SHALL emit a structured `unloadable_document` warning naming the document kind, its datasource and name (or memory id), and the cause. The warning SHALL appear once per unloadable document per operation, both as a Python warning and on the operation's response where it has one. Ingestion SHALL report the unloadable model by its own name, not the model being saved.

Loading the unloadable document directly SHALL still raise its error. A skipped peer's edge names SHALL NOT be checked by save-time join validation.

#### Scenario: One unloadable model does not block saves or re-ingest

- **WHEN** a datasource holds one model that cannot be loaded (a join keyed on an expression column) and other valid models
- **THEN** every valid model re-saves and the datasource re-ingests
- **AND** the ingest report names the unloadable model exactly once

#### Scenario: One invalid memory does not block listing

- **WHEN** a store holds one memory that cannot be loaded, among valid ones, on the YAML or the SQLite backend
- **THEN** listing memories returns every valid memory and warns once about the invalid one
- **AND** fetching the invalid memory by id raises its error

#### Scenario: The warning is emitted once per document per operation

- **WHEN** one operation enumerates a datasource holding an unloadable model several times, as a re-ingest saving many tables does
- **THEN** exactly one `unloadable_document` warning names that model

### Requirement: Model validation reports an unloadable model

Model validation over a datasource SHALL report every unloadable model as an invalid model, with the load error's cause, and SHALL validate the remaining models.

#### Scenario: validate_models lists the unloadable model

- **WHEN** model validation runs over a datasource holding one unloadable model
- **THEN** the report marks that model invalid with its cause, and reports the other models as usual

### Requirement: Operations that pick among models fail closed on an unloadable one

An operation that chooses an answer among a datasource's models SHALL fail with the unloadable model's load error rather than choose without it. This covers population inference, bare-name scoping, root-model recommendation, detection scope, and memory entity resolution.

#### Scenario: Population inference fails closed

- **WHEN** a query without `source_model` infers its population in a datasource holding an unloadable model
- **THEN** the query fails with that model's stored-document load error

#### Scenario: Memory entity resolution fails closed

- **WHEN** a memory's bare entity reference is resolved in a datasource holding an unloadable model
- **THEN** resolution fails with that model's stored-document load error
