## Why

On Python 3.14, ingesting a SQLite datasource intermittently loses a table's primary key — and with it
the cardinality of every join into that table — without telling anyone. SQLAlchemy's SQLite reflection,
called with a schema, first runs a statement that can never prepare (`… "main".sqlite_temp_master`) and
relies on catching its error; CPython builds that error from the connection's error code after re-taking
the GIL, and when the code reads `SQLITE_OK` the failure surfaces as a `SystemError`, which escapes
SQLAlchemy's fallback and is swallowed by SLayer's best-effort key read. Separately, the in-memory
SQLite engine hands its one connection to several worker threads at once.

## What Changes

- The SQLite dialect reads keys (primary key, foreign keys, unique constraints, unique indexes) and
  columns from SQLite's own catalog (table-valued pragmas, scoped to the table's schema), never through
  SQLAlchemy's Inspector, so no SQLite catalog read depends on a statement failing. Column types stay
  exactly as today.
- The in-memory SQLite engine hands its one connection to one thread at a time: a concurrent checkout
  waits; a same-thread nested checkout fails at once.
- Every key read that fails is reported: on the key-read result of the client contract, on the
  ingestion scan report and idempotent ingest result (with the table's location), and in every ingest
  surface (MCP, CLI, REST). The table is still ingested without that key; a re-ingest fills it.
- New database principle: a database connection is used by one thread at a time, enforced by a law
  test covering concurrent SQLite calls and every engine SLayer builds.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `models/fk-ingestion`: a failed key read is reported on the ingest result and in every ingest
  surface; SQLite keys are read from SQLite's own catalog without a failing statement.
- `database/client-contract`: SQLite columns are read from SQLite's own catalog with unchanged types;
  a key read lists the reads that failed.
- `database/execution`: a database connection serves one thread at a time.

## Impact

- `slayer/sql/dialects/sqlite.py`, `slayer/sql/dialects/reflection.py` (`best_effort`, `TableKeys`,
  `KeyReadFailure`), `slayer/sql/dialects/base.py`, `slayer/sql/dialects/duckdb.py` (failure records).
- `slayer/database/engine_factory.py` (in-memory pool), `slayer/database/client.py` (docstring).
- `slayer/live_schema/ingestion.py`, `slayer/live_schema/schema_drift.py` (`key_read_failures`),
  `slayer/mcp/server.py`, `slayer/cli.py`, `slayer/api/server.py` (rendering).
- `architecture/python.database.arc42.md` (principle 6, principle 3 wording).
- `docs/concepts/ingestion.md`.
- No dependency changes; relies on SQLAlchemy's SQLite type-affinity resolver, pinned by a test.
