## Why

SLayer only reads client data, but nothing below the surfaces enforces that: the facades and the model-SQL save check refuse writes, while every datasource connection is opened read-write and the default SQL client's `execute` admits DDL and DML. Opening connections read-only where the backend supports it, and having the client send only reads, makes "SLayer does not write" an engine-enforced guarantee rather than a convention.

## What Changes

- Every connection SLayer opens to a datasource is read-only wherever its backend is live-tested and supports it, on the synchronous and asynchronous paths and on dialect-built engines:
  - DuckDB file: opened with `read_only`.
  - SQLite file: `PRAGMA query_only = ON` on every new connection, which also covers attached and temporary databases.
  - Postgres:
    - Direct: `default_transaction_read_only` as a startup option on every supported driver.
    - Behind a transaction-mode pooler: each transaction starts `READ ONLY`, at no extra request.
  - MySQL / MariaDB: the session is read-only from connect.
  - ClickHouse: per-request `readonly = 2` when the user's profile is unrestricted (`readonly = 0`). Users already at 1 or 2 are left alone.
  - The remaining backends have no such mode, or are not live-tested (Redshift, deferred to DEV-2228), and keep today's connections.
  - In-memory SQLite and DuckDB databases stay writable.
- **BREAKING** The default SQL client refuses, with a typed error and before sending anything, every statement it cannot classify as a read, except on an in-memory database. This covers DDL, DML, `SELECT … INTO`, `PRAGMA`, `CALL` and `EXPLAIN`.
- **BREAKING** `execute` no longer admits `EXPLAIN`. Plans go only through the client's `explain()`, which refuses a plan statement that is not the dialect's explain of a read.
- **BREAKING** A DuckDB or SQLite datasource whose file does not exist is refused with a typed error before connecting, instead of an empty database being created.
- **BREAKING**
  - A MySQL / MariaDB URL that sets its own `init_command` is refused at engine build.
  - A ClickHouse URL naming a driver other than `clickhouse+http` is refused at engine build.
- `SlayerSQLClient.execute_sync` releases idle connections holding a local database file at the end of every call, on both its synchronous-driver and asynchronous-driver paths. Today only the asynchronous-driver path does, so a DuckDB or SQLite file stayed held.
- The column-type probe keeps its per-probe read-only transaction where it has one today (Postgres, Redshift, Oracle). It is now expressed through the same dialect hook.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `database/execution`: adds the read-only connection requirement and the missing-local-file refusal. The bare client's `execute_sync` joins the local-file release in "Synchronous calls reuse connections". The pooler requirement states read-only transactions.
- `database/client-contract`: the default client refuses non-read statements (except on an in-memory database) and `EXPLAIN` through `execute`, and `explain()` checks its plan statement. "Ordinary statements are unaffected" now covers reads only.

## Impact

- **Code:**
  - `slayer/sql/connection_policy.py`: the one engine-kwargs door applies the read-only hook.
  - `slayer/sql/dialects/{base,postgres,mysql,clickhouse,sqlite,duckdb}.py`: the hook per dialect.
  - `slayer/database/engine_factory.py`: local-database classification, the missing-file refusal and listeners.
  - `slayer/database/client.py`: admission, the `explain()` check, the `execute_sync` release and the type-probe guard via the hook.
- **Architecture:** `architecture/python.database.arc42.md` §3.5 (local-file exception) and a new §3.7 (read-only datasources).
- **Docs:**
  - `docs/database-support.md`: whether each backend's connections are read-only.
  - `docs/configuration/sql-clients.md`: client contract and conformance fixture loading.
- **Tests:**
  - A new law test, plus unit tests for DuckDB, SQLite and Postgres (every driver, and the pooler).
  - Integration tests for MySQL, MariaDB and ClickHouse.
  - Existing tests and fixture generators that write through SLayer-built engines or the default client move their seeding to raw drivers.
- **Users:**
  - A process that writes a DuckDB file while SLayer holds it open must close its write connection before SLayer's next call.
  - Bare-client callers that ran DDL, DML or `EXPLAIN` through `execute` must use their own driver, or `explain()`.
