## Purpose

Keeps Python code written for SLayer 1.5.0 working, with a deprecation warning until 2.0, and defines the engine-supplied forms that replace those calls.

## ADDED Requirements

### Requirement: 1.5.0 ingestion calls keep working with one warning

`ingest_datasource` and `ingest_datasource_report` SHALL be synchronous and accept their 1.5.0 positional and keyword parameters, plus an optional keyword-only `client`. `ingest_datasource_idempotent` SHALL accept `client` as optional. Each call that omits `client` SHALL return the same result as the same call given an engine-owned client for the same datasource, and SHALL emit exactly one `FutureWarning` that names the call, the replacement (`client=engine.client_for(ds)` or the engine ingestion method) and "SLayer 2.0", attributed to the caller's line — also when the call's coroutine is run through a sync bridge rather than awaited directly.

#### Scenario: Sync ingestion as the 1.5.0 docs wrote it

- **WHEN** `ingest_datasource(datasource=ds, schema=...)` and `ingest_datasource_report(ds)` are called synchronously, with no client, against a file-backed SQLite datasource and a file-backed DuckDB datasource
- **THEN** each returns the same models (and report) as the engine-supplied form on the same datasource, and emits exactly one `FutureWarning` with the remedy text whose reported filename is the calling test file

#### Scenario: Idempotent ingestion as the 1.5.0 docs wrote it

- **WHEN** `run_sync(ingest_datasource_idempotent(datasource=ds, storage=storage))` runs against a file-backed SQLite datasource and a file-backed DuckDB datasource
- **THEN** the stored models and the returned result equal those of the engine-supplied form, and exactly one `FutureWarning` is emitted, attributed to the line that created the coroutine

#### Scenario: The idempotent fallback loads stored legacy models

- **WHEN** the no-client idempotent form runs over a store holding models that need live-schema type refinement on load
- **THEN** the models load and merge as they do through a query engine, rather than raising that they must be loaded through a query engine

### Requirement: 1.5.0 drift validation keeps working with one warning

`validate_datasource` SHALL accept `client` as optional and SHALL accept and ignore `sql_clients`. A call that omits `client` or passes `sql_clients` SHALL return the same entries as the call given an engine-owned client, and SHALL emit exactly one `FutureWarning` per call naming the call, the replacement and "SLayer 2.0", attributed to the caller's line.

#### Scenario: Drift validation without a client

- **WHEN** `validate_datasource(datasource=ds, models=models)` is awaited for a file-backed datasource where one model's table was dropped
- **THEN** it returns the same entries as the engine-client form, including the dropped model, with exactly one `FutureWarning`

#### Scenario: Legacy sql_clients argument

- **WHEN** `validate_datasource` is called with `sql_clients={}` and no client
- **THEN** it returns the same entries as the engine-client form and emits exactly one `FutureWarning`, not two

### Requirement: 1.5.0 converter construction keeps working with one warning

`DbtToSlayerConverter` and `OsiToSlayerConverter` SHALL accept `sa_engine` in its 1.5.0 position (third positional parameter, or keyword). Passing `sa_engine` SHALL emit exactly one `FutureWarning` at construction naming the converter, `collect_metadata(client=...)` and "SLayer 2.0". `convert()` on such a converter without a prior `collect_metadata()` SHALL collect the metadata itself through the caller's SQLAlchemy engine and then convert, returning the same result as the `collect_metadata(client=...)` + `convert()` form on the same database.

#### Scenario: Converter with an in-memory engine, as the 1.5.0 notebooks wrote it

- **WHEN** a converter is constructed with `sa_engine=sa.create_engine("sqlite://")` whose tables were created on that engine, and `convert()` is called directly — once from plain synchronous code and once from inside a running event loop
- **THEN** both conversions see the engine's tables and equal the result of the client-based form, and exactly one `FutureWarning` is emitted per construction

#### Scenario: Converter with a file-backed DuckDB engine

- **WHEN** a converter is constructed with a file-backed DuckDB `sa_engine` and `convert()` is called
- **THEN** the result equals the client-based form on the same database

#### Scenario: The caller's engine survives

- **WHEN** a conversion through a caller's `sa_engine` finishes, or fails with an authentication error
- **THEN** the caller's engine has not been disposed, invalidated or had its pool released by SLayer, and remains usable

### Requirement: New forms never warn

The engine-supplied forms SHALL emit no `FutureWarning`: the sync ingestion functions given `client`, `ingest_datasource_async` and `ingest_datasource_report_async` (both requiring `client`), `ingest_datasource_idempotent` given `client`, `validate_datasource` given `client` and no `sql_clients`, and converters used through `collect_metadata(client=...)` + `convert()`. A sync ingestion call given a client SHALL leave that client usable afterwards.

#### Scenario: Client-supplied calls are silent

- **WHEN** each new form runs with warnings recorded
- **THEN** no `FutureWarning` is recorded

#### Scenario: A client survives a sync call

- **WHEN** `ingest_datasource(ds, client=engine.client_for(ds))` runs synchronously and the same client is then used by an engine query
- **THEN** the query succeeds

### Requirement: The engine supplies its own client for ingestion

`SlayerQueryEngine` SHALL provide `ingest(ds, ...)` and `ingest_sync(ds, ...)` returning the scanned models, and `ingest_idempotent(ds, ...)` and `ingest_idempotent_sync(ds, ...)` returning the idempotent-ingest result written to the engine's storage. Each SHALL accept the same scope arguments as the corresponding free function and read the database through the engine's own client for `ds`.

#### Scenario: One-line ingestion

- **WHEN** `engine.ingest_sync(ds, schema=...)` and `engine.ingest_idempotent_sync(ds)` run
- **THEN** they return the same models and result as the free functions given `client=engine.client_for(ds)`, without a warning

#### Scenario: The engine's client factory is honoured

- **WHEN** an engine built with a `client_factory` runs `ingest_idempotent(ds)`
- **THEN** every database read goes through the factory's client

### Requirement: Fallback engines are short-lived and direct

A call that omits its client SHALL read the database through a query engine created for that call with no client factory, and SHALL close that engine — releasing every client it built — before returning, raising, or being cancelled. A host's client factory on another engine SHALL NOT apply to such a call.

#### Scenario: No connection outlives the call

- **WHEN** each no-client form completes, raises from the database, or is cancelled mid-read
- **THEN** every client the call's engine built has been closed and no connection or engine is leaked

#### Scenario: Another engine's factory is not used

- **WHEN** a process has an engine with a `client_factory` and calls `ingest_datasource(ds)` without a client
- **THEN** the call connects directly with the datasource's own configuration and the factory is not invoked
