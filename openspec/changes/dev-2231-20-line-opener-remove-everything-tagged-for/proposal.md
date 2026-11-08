## Why

`main` is the 2.0 line since `1.x` was cut at `v1.8.1`, but it still says `version = "1.8.1"`. The removal
tripwire fails for every site tagged `removal="2.0"` as soon as the package's major version is 2 — so the
`2.0.0.dev0` bump and the removal of everything tagged for 2.0 must ship together, as the first PR of the
2.0 line.

## What Changes

- **BREAKING** `ingest_datasource`, `ingest_datasource_report`, `ingest_datasource_idempotent` and
  `validate_datasource` require `client=`; `validate_datasource` no longer takes `sql_clients`. The
  idempotent and validation entry points become plain coroutine functions.
- **BREAKING** `DbtToSlayerConverter` / `OsiToSlayerConverter` drop `sa_engine`; every parameter after
  `data_source` is keyword-only, so `convert()` never collects metadata on its own.
- **BREAKING** `StorageBackend` no longer adapts 0.9.x-style subclasses (a `save_datasource` override without
  `_save_datasource_impl`, a plain-list `_list_memories_rows`).
- **BREAKING** The CLI `--models-dir` flag is gone; `SLAYER_MODELS_DIR` is no longer read.
- **BREAKING** The MCP `inspect_model` tool is gone (use `inspect` with `entity_type="model"`).
- **BREAKING** `MemoryNotFoundError.identifier` is gone (use `memory_id`).
- **BREAKING** The category probes are gone: `get_column_types_sync`, `SlayerSQLClient.get_column_types`,
  `DelegatingSqlClient.get_column_types`, `SlayerQueryEngine.get_column_types`, the 1.6.0 client adapter and
  the dialects' 1.6.0 classification tables. A client must serve `get_result_column_types`.
- **BREAKING** The `postgres` extra installs only psycopg and the `mysql` extra only pymysql (psycopg2,
  asyncpg and aiomysql are no longer installed); the bundled-driver warning is gone.
- The package version becomes `2.0.0.dev0`; the tripwire pins no workaround and keeps its mechanism for
  forms 2.0 deprecates with `removal="3.0"`.
- Stored-data upgrade paths (versioned migrations, the `__` alias rewrite, layout upgrades, type refinement,
  1.6.0-typed stored columns) are unchanged.

## Capabilities

### New Capabilities

### Modified Capabilities

- `compatibility/legacy-call-forms`: the 1.5.0 no-client and `sa_engine` forms and their fallback engines are
  removed; the client-taking forms require their client.
- `compatibility/removal-tripwire`: no workaround is covered at 2.0; the tripwire and marker are stated per
  major version; the per-surface warning requirement is removed.
- `storage/custom-backends`: the 0.9.x adapters are removed; the current hook contract is stated.
- `sql/client-contract`: the 1.5.0 and 1.6.0 category probes are removed; the conformance suite keeps failing
  a 1.6.0-only client.
- `sql/execution`: the bundled non-default driver warning is removed; extras bundle only the default driver,
  which changes the missing-driver hint.
- `models/column-granularity`, `models/list-columns`, `models/sample-profiling`, `queries/time-spine`:
  scenarios naming the removed `inspect_model` tool read `inspect` of a model.

## Impact

- Code: `slayer/engine/ingestion.py`, `slayer/engine/schema_drift.py`, `slayer/engine/query_engine.py`,
  `slayer/dbt/converter.py`, `slayer/osi/converter.py`, `slayer/storage/base.py`,
  `slayer/storage/legacy_backends.py` (deleted), `slayer/cli.py`, `slayer/mcp/server.py`,
  `slayer/usage_reporting/payload.py`, `slayer/core/errors.py`, `slayer/core/deprecation.py`,
  `slayer/async_utils.py`, `slayer/sql/client.py`, `slayer/sql/dialects/{base,postgres,mysql,tsql,snowflake,duckdb,sqlite}.py`.
- Packaging: `pyproject.toml` version and extras; `asyncpg` / `aiomysql` move to the dev group; `poetry.lock`.
- Tests: the compatibility suites for the removed forms are deleted; incidental users are ported; the tripwire
  test's second half goes.
- Docs: getting-started, configuration (storage, sql-clients, datasources), the MCP reference, two notebooks.
- Migration notes for the 2.0 guide are recorded on DEV-2220.
