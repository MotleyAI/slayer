# compatibility/legacy-call-forms Specification

## Purpose
Defines the client-taking Python entry points that replaced the SLayer 1.5.0 call forms: ingestion, drift validation and importer converters read the database only through a client the caller passes, or one the engine supplies.

## Requirements

### Requirement: The engine supplies its own client for ingestion

`SlayerQueryEngine` SHALL provide `ingest(ds, ...)` and `ingest_sync(ds, ...)` returning the scanned models, and `ingest_idempotent(ds, ...)` and `ingest_idempotent_sync(ds, ...)` returning the idempotent-ingest result written to the engine's storage. Each SHALL accept the same scope arguments as the corresponding free function and read the database through the engine's own client for `ds`.

#### Scenario: One-line ingestion

- **WHEN** `engine.ingest_sync(ds, schema=...)` and `engine.ingest_idempotent_sync(ds)` run
- **THEN** they return the same models and result as the free functions given `client=engine.client_for(ds)`, without a warning

#### Scenario: The engine's client factory is honoured

- **WHEN** an engine built with a `client_factory` runs `ingest_idempotent(ds)`
- **THEN** every database read goes through the factory's client

### Requirement: Database reads go through the caller's client

`ingest_datasource`, `ingest_datasource_report`, `ingest_datasource_async`, `ingest_datasource_report_async`, `ingest_datasource_idempotent` and `validate_datasource` SHALL require a keyword `client` and SHALL read the database only through it; a call without one SHALL raise a `TypeError` before reading anything. `ingest_datasource` and `ingest_datasource_report` SHALL be synchronous and keep their positional scope parameters; `ingest_datasource_idempotent` and `validate_datasource` SHALL be coroutine functions; `validate_datasource` SHALL NOT accept `sql_clients`. A synchronous ingestion call SHALL leave its client usable afterwards. `DbtToSlayerConverter` and `OsiToSlayerConverter` SHALL take every parameter after `data_source` by keyword only and SHALL NOT accept `sa_engine`; their `convert()` SHALL read no database and SHALL use only the metadata a prior `collect_metadata(client=...)` gathered.

#### Scenario: A call without a client is refused
- **WHEN** `ingest_datasource(ds)`, `ingest_datasource_report(ds)`, `ingest_datasource_idempotent(datasource=ds, storage=storage)` or `validate_datasource(datasource=ds, models=models)` is called without `client`
- **THEN** it raises a `TypeError` and no database is read

#### Scenario: A client survives a sync call
- **WHEN** `ingest_datasource(ds, client=engine.client_for(ds))` runs synchronously and the same client is then used by an engine query
- **THEN** the query succeeds

#### Scenario: A 1.5.0 positional converter argument is refused
- **WHEN** a converter is constructed with a third positional argument, as a 1.5.0 `sa_engine` was passed
- **THEN** it raises a `TypeError`, rather than binding the argument to another parameter

#### Scenario: Converters read through the collected metadata only
- **WHEN** a converter runs `collect_metadata(client=engine.client_for(ds))` and then `convert()`
- **THEN** every database read happens through that client during `collect_metadata`, and `convert()` reads none
