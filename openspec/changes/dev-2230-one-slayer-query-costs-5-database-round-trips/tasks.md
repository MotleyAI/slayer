## 0. Failing tests (pr-tests stage)

- [x] 0.1 Write `tests/test_law_one_request_per_statement.py` (unit; pytest-postgresql with `log_statement=all` and `log_connections=on`, TCP-only per the sandbox note): warm-statement count = 1 on the psycopg async path, the psycopg sync-bridge path and the psycopg2 sync path; one connection across three `execute_sync` calls; a 60 s probe between 120 s queries costs exactly two timeout statements; idle > 60 s (monkeypatched monotonic clock) costs one ping, a fresh connection none. Verify it fails on the current code.
- [x] 0.2 Per-backend statement-count integration tests (one per Tier-1 networked file: MySQL `general_log`, ClickHouse `system.query_log`, Trino `system.runtime.queries`, Snowflake `QUERY_HISTORY_BY_SESSION`, BigQuery client-side request count, SQL Server client-side DBAPI count). Verify they fail on the current code where the backend is runnable locally.
- [x] 0.3 Unit tests for the timeout record (`tests/test_statement_timeout_client.py` and siblings): set once, re-set on change, cleared on failure, rejection remembered and still reported, no rollback after an autocommit rejection, replaced connection re-sends, probe ordering and rollback-in-finally. Verify they fail.
- [x] 0.4 Unit tests for the after-idle ping, Snowflake overrides on `connect` (once per connection), per-dialect transaction mode (engine kwargs / connect args per dialect), and `transaction_pooler` (save refusal on mysql and untyped, build refusal on a non-Postgres backend, cache-key split, transaction-local path, default false on load). Verify they fail.
- [x] 0.5 Unit tests for the bridge loop (`tests/test_async_utils*.py`): reuse across calls, nested call from the bridge thread, contextvars, cancellation and interrupt cleanup, fork during a submit (POSIX only), `run_sync_on_caller_thread` with SQLite thread checks and a raising callback, idempotent shutdown and the leak gate. Verify they fail.
- [x] 0.6 Unit tests for release semantics: end of a sync workflow releases a file-backed DuckDB lock, keeps an in-memory SQLite database, keeps a networked pool; `close()` releases bridge-loop engines; `aclose()` still fully disposes. Verify they fail.
- [x] 0.7 Unit tests for the client door (`SessionStatementRefused`): every refused keyword, any letter case, trailing statement in multi-statement text, literal/identifier/comment negatives, DDL/DML pass-through, nothing sent when refused. Verify they fail.
- [x] 0.8 Ask the user to approve aligning existing tests that pin the old shape (naming each file and change): `pool_pre_ping` asserts (`tests/test_engine_factory.py`, `tests/dialects/test_bigquery.py`, `tests/dialects/test_trino.py`), checkout-listener tests (`tests/test_engine_factory.py::TestSessionOverridesListener`, `tests/integration/test_integration_snowflake.py::test_session_overrides_apply_to_metadata_reads`), per-statement timeout logs (`tests/test_statement_timeout_client.py`, `tests/test_sql_client_snowflake.py::TestSnowflakeStatementTimeout`, `tests/test_dev1933_verbatim_execution.py`), Postgres `SET LOCAL` shape (`tests/dialects/test_statement_timeout_hooks.py`, `tests/integration/test_postgres_statement_timeout.py::test_timeout_is_transaction_local`), and the release-after-each-call scenario tests. Then align them.
- [x] 0.9 Codex-review the tests against this plan and fold the findings.

## 1. Baseline measurement (first pr-implement task, before any production change)

- [x] 1.1 Measure requests and new connections per warm statement on Postgres, MySQL, SQL Server, ClickHouse, Trino, BigQuery and Snowflake, on async / sync / bridged paths; also the Snowflake integration job time. Verify the table is posted as a comment on DEV-2230.
- [x] 1.2 Verify live, per driver, each design D3 assumption: `detect_autocommit_setting` availability (psycopg, psycopg2, asyncpg, pg8000, pymysql, mysqldb, aiomysql); pyodbc `autocommit=True` at connect; Snowflake connector rollback/commit cost and AUTOCOMMIT state; BigQuery/ClickHouse/Trino rollback is local. Record the outcome on DEV-2230; a contradicting result stops for the user.

## 2. Dependencies

- [x] 2.1 Raise `sqlalchemy` to `>=2.0.43,<2.1` in `pyproject.toml` and regenerate the lock; verify `poetry install -E all` succeeds.

## 3. Bridge loop (D1)

- [x] 3.1 Implement the long-lived bridge loop in `slayer/async_utils.py` (lazy start, copied context, nested fallback, interrupt → cancel + bounded cleanup wait, queue-pumping caller-thread variant, owner with idempotent shutdown + atexit, at-fork reset). Verify 0.5 passes.

## 4. Release semantics (D2)

- [x] 4.1 Add the end-of-workflow release (local-file idle connections only, judged by resolved URL + pool type) and use it in `SlayerQueryEngine.run_sync` and `_execute_bridged`; keep `aclose()` full disposal; `close()` and a finalizer release bridge-loop engines. Verify 0.6 passes.

## 5. Connection policy (D3, D5, D6)

- [x] 5.1 Add the one connection-policy helper and per-dialect transaction-mode hooks; route the factory builder, dialect build hooks and the async builder through it; delete every `pool_pre_ping=True`. Verify 0.4 (transaction mode) passes and `grep -rn pool_pre_ping slayer/` is empty.
- [x] 5.2 Move Snowflake `apply_session_overrides` to `connect`; update the build-hook error text. Verify 0.4 (overrides) passes.
- [x] 5.3 Add the after-idle ping listener. Verify 0.4 (ping) passes.

## 6. Timeout once per connection (D4)

- [x] 6.1 Add the timeout-scope hook; Postgres session `SET`; the connection-record value/rejection store; probe ordering with rollback in `finally`. Verify 0.3 passes.

## 7. transaction_pooler (D7)

- [x] 7.1 Add `DatasourceConfig.transaction_pooler`, the save-time check in `StorageBackend.save_datasource`, the build-time backend check, the cache-key leg, and the transactional path. Verify 0.4 (pooler) passes.

## 8. Client door (D9)

- [x] 8.1 Add `SessionStatementRefused` and the tokenizer check on `execute` / `execute_sync`. Verify 0.7 passes.
- [x] 8.2 Commit after every statement that is not a read-only query (DuckDB's driver reports `returns_rows` for DDL and DML, so today they are rolled back); verify `tests/test_session_statement_refused.py::test_ordinary_statements_run[duckdb]` and `::test_duckdb_writes_are_committed` pass.

## 9. Architecture and docs

- [x] 9.1 Apply the approved arc42 edits verbatim: `architecture/python.sql.arc42.md` §3 item 17 (design.md decisions log entry 6) and the `architecture/system.arc42.md` renumber to 24 (entry 7). Verify `poetry run la-arch-check` passes.
- [x] 9.2 Docs, one sentence each: `docs/configuration/datasources.md` (Snowflake overrides once per connection; timeout once per connection; `transaction_pooler` and when to set it) and `docs/configuration/sql-clients.md` (`execute` refuses session and transaction statements). Verify `grep -n "every pool checkout\|per-connection .USE" docs/` finds no stale text.

## 10. Snowflake suite (D8)

- [x] 10.1 Size the reserved-alias sweep's worker count to the pool; one raw connection per seeding fixture. Verify the alias sweep still passes live.
- [ ] 10.2 Measure the Snowflake job time locally and in CI against the 5-minute target; verify `.github/workflows/integration-snowflake.yml` keeps `timeout-minutes: 15`.
- [ ] 10.3 Before closing DEV-2230 (pr-review stage), revisit batching the two sweeps with the user, using the measured times.

## 11. Verification

- [x] 11.1 Re-measure as in 1.1 and post the after table on DEV-2230.
- [x] 11.2 Run the full unit suite, the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) and `poetry run la-arch-check`; all green.
