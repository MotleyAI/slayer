## Why

On ClickHouse the client ran `SET max_execution_time = N` before each query, but the `clickhouse+http` driver sends every statement as its own HTTP request with no session, so the setting never reached the query and ClickHouse queries had no timeout. For a `readonly = 1` user the `SET` failed with `Code: 164 (READONLY)`, so every query failed. Separately, `mariadb` datasources receive MySQL's `SET max_execution_time`, which MariaDB rejects (error 1193), so every MariaDB query fails too.

The root cause is structural: the client picks each database's timeout statement by inline `db_type` branching (forbidden by sql §3.2), duplicated by hand across the sync and async paths. A first fix that rewrote each ClickHouse statement's `SETTINGS` clause via a sqlglot parse/re-emit violated sql §3.1 and §3.13 (the statement reaching the driver was no longer the rendered SQL).

## What Changes

- The statement timeout is applied through dialect hooks on every execution path — synchronous and asynchronous execution and the column-type probe — with no `db_type` branching in the client and no rewriting of the statement.
- ClickHouse: the timeout travels as a per-request HTTP setting on the checked-out connection (restored after the call); the SQL reaches the server unchanged. A `max_execution_time` the SQL itself sets wins.
- ClickHouse `readonly = 1` users: detected once per engine (`getSetting('readonly')`); their queries run without SLayer's timeout and report it.
- MariaDB gets its own timeout statement (`SET max_statement_time = <seconds>`); MariaDB's support tier is unchanged.
- Postgres keeps a best-effort, transaction-local `SET LOCAL statement_timeout`; a rejected `SET` no longer poisons the transaction. `None` and unknown datasource types follow the existing Postgres fallback.
- The column-type probe applies the same timeout (60 s) on every dialect.
- New warning kind `statement_timeout_skipped`, surfaced as a Python warning and on `SlayerResponse.warnings`.
- `SlayerSQLClient.execute` / `execute_sync` return an `ExecutionResult` (rows + warnings) instead of a bare row list (internal API).

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `sql/execution`: the verbatim-execution requirement gains a ClickHouse scenario; new requirements for per-dialect statement timeouts and for reporting a skipped timeout.

## Impact

- Code: `slayer/sql/client.py` (timeout orchestration, `ExecutionResult`), `slayer/sql/dialects/{base,mysql,postgres,clickhouse,snowflake,__init__}.py` (hooks, `MariadbDialect`), `slayer/core/warnings.py` (new payload), `slayer/engine/query_engine.py` and other `execute` callers (`.rows`).
- Removes the ClickHouse SQL rewrite (`_with_ch_statement_timeout`) and its tests.
- Docs: `docs/configuration/datasources.md`, `docs/concepts/queries.md`, warning-kind lists in `docs/reference/`.
- Closes DEV-1977 and DEV-1940. MariaDB Tier 1 promotion is DEV-1975.
