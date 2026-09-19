## Why

CPython 3.13+ reports every `sqlite3` connection that is garbage-collected unclosed as
`ResourceWarning: unclosed database`; the 3.14 unit suite emits 11,311 of them per run (0 on
3.12), and deferred finalization drops them into unrelated tests' `warnings.catch_warnings`
windows. The causes are structural — a never-closing `with sqlite3.connect(...)` idiom repeated at
32 production sites, and SQLAlchemy engines that leave the engine-factory cache or a client without
`dispose()` — so the fix is one door for sqlite connections, one disposing owner per engine, and a
gate that keeps CI red on any recurrence. The issue as filed blamed undisposed executed-test
fixture engines; those are factory-cached and disposed on eviction, and were not the cause.

## What Changes

- **One sqlite door.** `slayer/storage/sqlite_conn.py` — `transaction(db_path, **kw)` (commit on
  success, roll back on error, always close) and `open_connection(db_path, **kw)` (close only, for
  the `BEGIN IMMEDIATE` memory allocator). Every `sqlite3.connect` in `slayer/` and `tests/` routes
  through it (32 production + ~230 test sites).
- **One engine owner.** `engine_factory.reset_cache()` always disposes (**BREAKING** for the
  internal `dispose=` keyword, removed); in-memory SQLite engines are built by one factory builder
  (StaticPool, `check_same_thread=False`) and factory-cached per datasource name; `SlayerSQLClient`
  owns and disposes its private in-memory engine (`close()` plus a finalizer backstop); new
  `SlayerQueryEngine.close()` closes its clients and stays reusable; the client module's second
  engine cache (`_sync_engines`) and the dead `_INLINE_SYNC_DB_TYPES` branches are removed —
  `engine` becomes required on the two sync helpers.
- **The gate.** `pyproject.toml` `filterwarnings` turns `unclosed database` ResourceWarnings and
  pytest's unraisable-exception warnings into errors (inert on 3.11); `tests/conftest.py` disposes
  the factory cache and forces a collection at session end while the hooks are still active.
- **The ratchet.** `tests/test_law_resource_ownership.py`: an import-binding-aware AST check that
  `sqlite3.connect` appears only in the door and `create_engine` only inside `engine_factory`, the
  dialects' `build_engine` hooks, the client's async builder, and the disposing test helper; it also
  pins the gate configuration.
- **One seeded executing-engine context** in `tests/_engine_helpers.py` replaces the 12 duplicated
  `make_exec_engine` / `_engine_for` generators and the law harness's; it closes the query engine
  and invalidates the temp datasource's factory engine before the tempdir is removed.
- **Architecture harnesses.** `architecture/sql.arc42.md` §3 item 14 (one engine owner); new
  `architecture/storage.arc42.md` (one sqlite door) wired in `architecture/index.yaml`. Exact
  diffs need per-change approval before editing.
- **Docs.** One sentence in `docs/getting-started/python.md` on `engine.close()`.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `sql/execution`: ADDED requirements — engines have one disposing owner (factory reset disposes;
  `SlayerQueryEngine.close()` disposes private in-memory engines, is idempotent and leaves the
  engine reusable; an unreferenced engine's private in-memory engine is disposed on collection;
  per-call async teardown spares sync engines) and an in-memory SQLite datasource is one database
  across threads for a given owner and isolated per datasource name in the shared factory.

## Impact

- `slayer/storage/`: new `sqlite_conn.py`; `sidecar_embedding_store.py`, `sqlite_storage.py`,
  `v4_migration.py` route through it.
- `slayer/sql/engine_factory.py`, `slayer/sql/client.py`, `slayer/engine/query_engine.py`: engine
  ownership; `pyproject.toml`; `tests/conftest.py`; `tests/_engine_helpers.py`; 119 test files
  with sqlite/engine sites; 13 fixture roots; new `tests/test_law_resource_ownership.py` and
  ownership/door tests.
- `architecture/sql.arc42.md`, `architecture/storage.arc42.md` (new), `architecture/index.yaml`.
- Cross-repo: the MCP `mcp._slayer_engine.aclose()` teardown contract is unchanged.
