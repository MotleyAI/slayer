## 1. Tests (pr-tests stage — all fail before implementation)

- [ ] 1.1 Law test `tests/test_law_read_only_datasources.py` (§3.7). It holds three checks, and it is verified by failing on main:
  - For DuckDB file, SQLite file and Postgres (pytest-postgresql), a write fails through every construction path: factory default engine, client async engine (Postgres psycopg/asyncpg), replacement connection after invalidate, and direct use of the returned engine.
  - The default client refuses a non-read statement before sending it.
  - An in-memory SQLite or DuckDB datasource still accepts writes.
- [ ] 1.2 DuckDB/SQLite unit tests for the `database/execution` scenarios:
  - read-only DuckDB file;
  - SQLite main/attached/temp writes refused, with UDFs still working;
  - in-memory writable;
  - caller's engine untouched;
  - "A missing local database file is refused", in every URL form (relative, absolute, `file:` URI, in-memory forms not refused) with no file created.

  Verified by the tests failing on main.
- [ ] 1.3 Postgres unit tests (pytest-postgresql, no integration marker), verified by failing on main:
  - every driver × sync/async, including autocommit;
  - `default_transaction_read_only = on`;
  - URL `options` kept;
  - same statement count as before, from the server statement log (`-c log_statement=all`);
  - pooler `BEGIN READ ONLY` on psycopg/psycopg2/asyncpg, and the `START TRANSACTION READ ONLY` swap on pg8000 with no extra statement.

  Gotcha: sandboxed Postgres needs `-c unix_socket_directories=` (TCP only).
- [ ] 1.4 Client-contract tests for "The default client sends only reads" (every listed statement refused unsent; `EXPLAIN` through `execute` refused; trailing write; keywords in literals/comments; `explain()` refuses a write plan with nothing sent, not even the switch-on; in-memory accepts writes but still refuses `SET`). Also the reworded "Ordinary statements are unaffected" scenario. Verified by failing on main.
- [ ] 1.5 `execute_sync` release test: on a DuckDB file, `SlayerSQLClient.execute_sync`, then a differently-configured `duckdb.connect` in the same process succeeds, and the next `execute_sync` succeeds. Verified by failing on main; the failure was reproduced during planning.
- [ ] 1.6 Engine-build refusal tests:
  - MySQL URL with its own `init_command` refused with a typed error naming the datasource, no connection opened;
  - `clickhouse+native` / `clickhouse+asynch` refused with an unsupported-driver error hinting `clickhouse+http`;
  - SQL Server/Snowflake/BigQuery/Trino/Oracle/Redshift connection arguments and connect-time statements unchanged (emission).

  Verified by failing on main.
- [ ] 1.7 Integration tests in `tests/integration/test_integration_mysql.py`, `test_integration_clickhouse.py` (and the MariaDB suite where it lives):
  - MySQL/MariaDB `INSERT` fails while queries keep their timeout;
  - ClickHouse `readonly` 0 (write fails, timeout applied, profile read once per engine, metadata-first engine read-only);
  - ClickHouse 1 (no setting sent, `readonly_user` warning as before) and 2 (unchanged).

  Verified in the path-gated CI workflows.
- [ ] 1.8 Existing tests that contradict the plan must move their seeding to raw drivers. Ask the user for approval per file before changing each one (`/la:pr` stopping policy). Known set:
  - `tests/fixtures/upgrade/v0_10_2/generate.py:161-169`;
  - `tests/integration/test_in_memory_sqlite.py:22-27` (file-backed, despite its name);
  - `tests/test_session_statement_refused.py:107-156` (`_ORDINARY`, `test_duckdb_writes_are_committed`, the `admit_statement` return assertion);
  - `tests/test_sync_connection_reuse.py:147,166`;
  - `tests/test_statement_timeout_postgres_session.py:169-182`;
  - `tests/test_sql_client.py:676-711` (commit/retry on writes; keep it on in-memory);
  - connect-listener ATTACH/temp-DDL fixtures `tests/_sqlite_catalog_corpus.py:86-104`, `tests/test_duckdb_key_reflection.py:174-187`, `tests/test_stored_upgrade_refinement_gate.py:179`, `tests/test_sql_client_dialects.py:39`, `tests/test_ingestion_schema_qualification.py:251-266`. ATTACH of an existing file works read-only; temp DDL on SQLite does not.
  - `tests/test_demo_jaffle_shop_healing.py:357-370` (same-process DuckDB conflict).

  Verify each by running it after the change.
- [ ] 1.9 Codex-review the tests against this plan (`reviewers.codex`). Verified by every finding being resolved with the user.

## 2. Dialect hooks and classification

- [ ] 2.1 Add the read-only hook family to `SqlDialect` (no-op defaults): engine-argument additions given URL, existing kwargs and async flag; per-connection setup; transaction-start; type-probe guard statement. Also the local-database classification (in-memory / local file path / not local). Verified by the no-op emission tests in 1.6 passing.
- [ ] 2.2 SQLite and DuckDB:
  - classification for every URL form;
  - DuckDB `read_only=True` for files;
  - SQLite `PRAGMA query_only = ON` connect setup.

  `engine_factory._is_in_memory_sqlite` and `holds_local_file` read the classification. Verified by 1.2 passing.
- [ ] 2.3 Postgres: startup option per driver (`options` appended, `server_settings`/`startup_params` merged). Behind a pooler: `postgresql_readonly` for psycopg/psycopg2/asyncpg, and the pg8000 `begin` listener swap. Type-probe guard for Postgres, Redshift and Oracle through the hook. Verified by 1.3 passing.
- [ ] 2.4 MySQL/MariaDB `init_command`, and refusal of a URL that sets its own. Verified by 1.6 and 1.7.
- [ ] 2.5 ClickHouse:
  - move the per-engine server-profile cache out of `client.py` into a module that `engine_factory` and `client` both import;
  - add a connect listener that fills or reads the cache under the profile lock and sets `readonly = 2` in the connection's per-request settings when the level is 0;
  - refuse non-`clickhouse+http` drivers.

  Verified by 1.6, then 1.7 in CI.

## 3. Engine doors and client

- [ ] 3.1 `connection_policy.engine_kwargs` merges the dialect's engine-argument additions with typed per-driver merge rules (other values unchanged). `engine_factory._build_engine` attaches the per-connection and transaction-start listeners. Verified by the 1.1 construction-path matrix.
- [ ] 3.2 Missing-local-file refusal before connecting, in both the sync and async builders, as a typed database-unavailable failure. DuckDB's native missing-database error maps to the same failure. Verified by 1.2.
- [ ] 3.3 `admit_statement`:
  - `EXPLAIN` out of the read leads and `INTO` into the write tokens;
  - new `WriteStatementRefused` (a `SqlClientError`);
  - the in-memory exemption via the classification;
  - `explain()` checks its plan statement before sending anything.

  Verified by 1.4.
- [ ] 3.4 `SlayerSQLClient.execute_sync` releases idle local-file connections at the end of its sync-driver branch, as the bridged branch does. Verified by 1.5.

## 4. Architecture and docs

- [ ] 4.1 `architecture/python.database.arc42.md`: apply the approved §3.5 amendment and the approved §3.7 text verbatim, from design.md decisions 5 and 6. Verified by `poetry run la-arch-check` passing.
- [ ] 4.2 Update `docs/database-support.md`: a per-backend statement of whether connections are read-only and how. Redshift is listed as not read-only, pending DEV-2228. Verified by reading the rendered page.
- [ ] 4.3 Update `docs/configuration/sql-clients.md`:
  - the client admits only reads, except on in-memory databases;
  - `EXPLAIN` goes through `explain()`;
  - load the conformance fixture through your own driver;
  - close your own DuckDB write connection before SLayer's next call.

  Grep `docs/` for other statements that the client runs DDL/DML. Verified by the grep coming back clean.

## 5. Gates

- [ ] 5.1 The full non-integration suite passes: `poetry run pytest -m "not integration"`.
- [ ] 5.2 The local integration suite passes, using the CI invocation from CLAUDE.md.
- [ ] 5.3 `poetry run ruff check slayer/ tests/` is clean.
- [ ] 5.4 `poetry run basedpyright` shows no new errors against the baseline.
- [ ] 5.5 `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass.
