## Why

Hosts embedding SLayer need to run it through a gateway (e.g. a credential-holding, governed query proxy) instead of SLayer opening its own database connections. Today live database access has ~30 independent entry points (`engine_factory.get_engine` + `sa.inspect` across engine, storage, importers, MCP and CLI), so any partial injection seam leaves bypasses — external PR #530's statements-only factory is silently bypassed once drift validation caches a direct client. Several of those entry points also block the event loop.

## What Changes

- New async `SqlClient` protocol covering statements (`execute`, `get_column_types`, `server_profile`), live metadata (`ping`, `list_schemas`, `default_schema`, `current_catalog`, `list_objects`, `reflect_columns`, `reflect_table_keys`, `table_comment`, `schema_description`) and lifecycle (`aclose`, `close`); `SlayerSQLClient` is the default implementation and absorbs today's reflection code.
- `client_factory` argument on `SlayerQueryEngine`, `SlayerClient` (local mode), `create_app`, `create_mcp_server`, and the Postgres-facade and Flight engine builders. Each engine builds, caches (per datasource name + connection identity) and owns its clients; every database-touching function takes a client.
- Typed client errors (`DatabaseUnavailable`, `AuthenticationRefused`, `ObjectNotFound`); drift deletes only on absence from a successful listing.
- `DelegatingSqlClient` forwarder so a statements-only gateway is a two-method subclass.
- Executable conformance suite (`run_conformance`) with per-dialect fixtures; implementer guide `docs/configuration/sql-clients.md`.
- Legacy stored-model type refinement reads only the model's own table through the loading engine's client via a non-owning per-engine storage view; a load that needs it without an engine raises a typed error.
- No live database access blocks the event loop (folds in DEV-2117); one engine-owned sync bridge `run_sync` closes the engine's clients at the end of each synchronous workflow.
- Join-cardinality profiling stops closing the engine's shared client.
- **BREAKING** (internal Python API): drift, ingestion, importer-introspection and type-refinement functions take a required client argument; `ReflectedColumn.type` is always a `DataType`.

## Capabilities

### New Capabilities
- `sql/client-contract`: the `SqlClient` contract — typed failures, delegation, and the executable conformance suite.

### Modified Capabilities
- `sql/execution`: every live database access goes through an engine-owned client built by an injectable factory; engines own their clients; no live access blocks the event loop; entry points accept the factory.
- `models/stored-upgrade`: legacy live type refinement reads the model's own table through the loading engine's client; loading without an engine raises a typed error.

## Impact

- Code: `slayer/sql/client.py`, `slayer/sql/conformance.py` (new), `slayer/sql/dialects/` (reflection, type mapping moved in from `engine/ingestion.py`), `slayer/sql/sqlite_introspect.py`, `slayer/sql/session_policy.py`, `slayer/engine/query_engine.py`, `schema_drift.py`, `schema_scope.py`, `ingestion.py`, `introspect_utils.py`, `slayer/storage/base.py`, `type_refinement.py`, new non-owning storage view, `slayer/mcp/server.py`, `slayer/api/server.py`, `slayer/client/slayer_client.py`, `slayer/cli.py`, `slayer/dbt/converter.py`, `slayer/osi/converter.py`, `slayer/pg_facade/`, `slayer/flight/`.
- Architecture: `python.sql.arc42.md` §3.13 reworded; `system.arc42.md` §3 gains principle 17 (One database door). No `model.c4` change.
- Docs: new `docs/configuration/sql-clients.md` + `zensical.toml` nav; one sentence each in the three Python-client pages.
- Tests: drift/ingestion/importer tests pass an engine-owned client (mechanical); new law test `tests/test_law_database_door.py`.
