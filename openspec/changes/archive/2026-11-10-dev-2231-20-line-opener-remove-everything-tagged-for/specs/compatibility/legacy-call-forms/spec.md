## REMOVED Requirements

### Requirement: 1.5.0 ingestion calls keep working with one warning
**Reason**: Scheduled for removal in SLayer 2.0; the no-client ingestion forms are gone.
**Migration**: Pass `client=engine.client_for(datasource)`, or call `engine.ingest_sync` / `engine.ingest_idempotent` (or their async forms).

### Requirement: 1.5.0 drift validation keeps working with one warning
**Reason**: Scheduled for removal in SLayer 2.0; `validate_datasource` requires its client and no longer takes `sql_clients`.
**Migration**: Pass `client=engine.client_for(datasource)` and drop `sql_clients`.

### Requirement: 1.5.0 converter construction keeps working with one warning
**Reason**: Scheduled for removal in SLayer 2.0; converters no longer take `sa_engine` or collect metadata inside `convert()`.
**Migration**: Construct the converter without `sa_engine`, `await converter.collect_metadata(client=engine.client_for(datasource))`, then call `convert()`.

### Requirement: New forms never warn
**Reason**: With the deprecated forms removed there is no warning left to withhold; the client-taking contract is restated in "Database reads go through the caller's client".
**Migration**: None.

### Requirement: Fallback engines are short-lived and direct
**Reason**: Only the removed no-client forms created fallback engines.
**Migration**: Pass the engine's client; its engine owns and closes it.

## ADDED Requirements

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
