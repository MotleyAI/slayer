## 1. Client — one verbatim door

- [ ] 1.1 Add `_exec_verbatim(conn, sql)` and `_exec_verbatim_async(conn, sql)` to `slayer/sql/client.py`, each calling `conn.exec_driver_sql(sql, execution_options={"no_parameters": True})` and returning the cursor result; verify the module guard test's helper-pair assertion passes.
- [ ] 1.2 Route all fifteen `conn.execute(sa.text(...))` sites through the pair — the four rendered-SQL doors, the timeout SETs in both execute paths and both probe-timeout helpers, and both `SET TRANSACTION READ ONLY` sites — and drop the now-unused `sa.text` usage; verify `grep -c "sa.text(" slayer/sql/client.py` is 0 and the SQLite door tests pass.

## 2. Normative harness & docs (arc42 edit approved 2026-09-18)

- [ ] 2.1 Append item 13 to `architecture/sql.arc42.md` §3 with the approved wording and its `[enforced: test:tests/test_dev1933_verbatim_execution.py]` tag; verify `poetry run python tools/arch_check.py` passes.
- [ ] 2.2 Add one sentence to the Columns section of `docs/concepts/models.md` (column SQL reaches the driver verbatim — `:name` never a bind parameter, `%` never a format directive); verify no new page, so no `zensical.toml` nav change.

## 3. Unit tests — `tests/test_dev1933_verbatim_execution.py`

- [x] 3.1 Module guard: AST of `slayer/sql/client.py` has no `text(` call and no `exec_driver_sql` / connection `execute` call outside `_exec_verbatim` / `_exec_verbatim_async`; verify it fails on the pre-fix module (revert-check). — TestClientModuleGuard, red on the 15 `sa.text` sites.
- [x] 3.2 Door tests on in-memory SQLite via `SlayerSQLClient.execute`, `execute_sync`, `get_column_types`, and direct `_execute_sql_sync` / `_get_column_types_sync`, with SQL holding `'(?i)(?:too complicated|too complex)'` and `strftime('%Y-%m', ...)`; verify each fails with `A value is required for bind parameter 'too'` without the fix.
- [x] 3.3 Async paths via a mock connection: `_execute_sql_async` and `_get_column_types_async` await `exec_driver_sql` with `execution_options={"no_parameters": True}` for every statement including the SETs, and never `execute`; verify the assertions fail on the pre-fix module.
- [x] 3.4 End to end through `SlayerQueryEngine` on SQLite: a `ModelExtension` column whose SQL holds the regex literal, queried as a dimension; verify it fails without the fix. **Calibration:** the plan's example `instr(status, '(?:')` does NOT reproduce the bug — `:` there is followed by `'`, not a word char, so text() ignores it (empirically OK pre-fix). Used the production literal `(?i)(?:too complicated|too complex)` (`:too` triggers) inside a `CASE ... = '<literal>' ...` column instead.
- [x] 3.5 Retarget `tests/test_sql_client_snowflake.py::TestSnowflakeStatementTimeout` mocks from `fake_conn.execute` to `fake_conn.exec_driver_sql` (AsyncMock for the async case), counts and `_extract_text` unchanged; verify the class passes. — retargeted (unstaged: modified file); red pre-fix, green post-fix.

## 4. Integration tests

- [x] 4.1 `tests/integration/test_dev1933_postgres_verbatim.py` (pytest-postgresql): `execute` and `get_column_types` on asyncpg, `execute_sync` on the sync driver, each carrying `~ '(?i)(?:too complicated|too complex)'` and `LIKE '%pend%'`; plus the end-to-end `ModelExtension` `REGEXP_REPLACE(..., '(?i)(?:too complicated|too complex)', ...)` query. Verified locally with the CI invocation: all 4 red pre-fix (`bind parameter 'too'`), all 4 green post-fix (the `%` survives verbatim on asyncpg + the sync driver).
- [x] 4.2 One focused case per Tier-1 suite through its engine fixture: `test_dev1933_regex_literal_extension_column` on `duckdb_env`, `mysql_env`, `clickhouse_env`, `sqlserver_env`, and the Snowflake `sf_storage_with_models` path — a `ModelExtension` column `CASE WHEN status LIKE '%pend%' OR status = '(?i)(?:too complicated|too complex)' THEN 1 ELSE 0 END` (both the `%` and `:too` hazards) grouped as a dimension. DuckDB verified locally (red pre-fix / green post-fix); the rest run in their path-gated CI workflows. **Calibration:** BigQuery omitted — its integration file has no query-execution fixture (ingestion-only), and BigQuery is the lowest-risk dialect (its SQLAlchemy dialect is `named` paramstyle, so `%` was never doubled and is identical before/after). Standing up a full BQ query env + seed for one unverifiable case is disproportionate; if wanted, it belongs in a follow-up, not this change.

## 5. Gate

- [ ] 5.1 `openspec validate dev-1933-slayer-satext-misreads-word-in-rendered-sql-as-a-bind --strict` passes.
- [ ] 5.2 `poetry run pytest -m "not integration" -n auto`, the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run python tools/arch_check.py`, and `poetry run basedpyright` (no new errors vs baseline) all pass.

## 6. Archive (spec-review stage)

- [ ] 6.1 In the archive commit, add `specs: [sql]` to the `sql` node in `architecture/index.yaml`; verify `poetry run python tools/arch_check.py` passes with `openspec/specs/sql/execution/spec.md` on disk.
