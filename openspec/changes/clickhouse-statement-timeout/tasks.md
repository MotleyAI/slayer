## 1. Failing tests (spec-tests)

Approved test changes (spec-plan, 2026-09-25): delete every
`_with_ch_statement_timeout` test in `tests/test_clickhouse_statement_timeout.py`
and rewrite its two type-probe tests against the connection-setting mechanism;
in `tests/integration/test_integration_clickhouse.py::test_readonly_user_own_setting_fails_without_retry`
keep the `READONLY` failure assertion and replace the `_ch_readonly_engines`
assertion; mechanical `.rows` edits at `execute`/`execute_sync` test call sites
(assertions unchanged). Any other test-logic change needs a fresh OK.

- [x] 1.1 Per-dialect unit tests of `statement_timeout_sql`: MySQL `SET max_execution_time = <ms>`, MariaDB `SET max_statement_time = <s>`, Postgres `SET LOCAL statement_timeout = <ms>`, Snowflake unchanged; `dialect_for_ds_type("mariadb")` is `MariadbDialect` while `get_dialect("mysql")` stays `MysqlDialect` — verify they fail now
- [x] 1.2 ClickHouse hook unit tests (mocked DBAPI connection with `transport.ch_settings`): setting written before the statement, prior state (value or absence) restored after it, also when the statement raises — verify they fail now
- [x] 1.3 Client unit tests (mocked engines, sync + async): permission check runs once per engine, including under concurrent threads; `readonly = 1` skips the setting and yields `statement_timeout_skipped`/`readonly_user`; Postgres rejected `SET` → rollback, statement still runs, `timeout_rejected`; MySQL rejected `SET` → raises, statement not run; `None` and unknown types take the Postgres path; probe applies the 60 s timeout before `SET TRANSACTION READ ONLY` on every dialect; DuckDB sends no timeout and warns nothing; `execute`/`execute_sync` return `ExecutionResult` with the warnings — verify they fail now
- [x] 1.4 Warning payload tests: `StatementTimeoutSkippedWarning` round-trips through `AnySlayerWarning`, carrier `UserWarning` wording equals `human_message` and names `readonly = 2`
- [x] 1.5 Engine test: a skipped timeout on the data query appears in `SlayerResponse.warnings` (and on the EXPLAIN path)
- [x] 1.6 Law test: `slayer/sql/client.py` has no `db_type ==` / `db_type in` timeout dispatch and no sqlglot parse of the statement on the execution path
- [x] 1.7 ClickHouse integration (update existing class): statement text in `system.query_log` byte-identical to the input for `toStartOfMonth(d)`, `x::Int32`, trailing `-- comment`; `sleep(3)` with 1 s → `TIMEOUT_EXCEEDED`; SQL's own `SETTINGS max_execution_time = 5` wins over a 1 s timeout; `readonly = 1` user runs twice with the warning and one permission check; `readonly = 2` user gets the timeout and no warning; setting restored on the pooled connection after a timed call
- [x] 1.8 Postgres integration: type probe with read-only transaction + `SET LOCAL statement_timeout` works (gotcha: verify `SET TRANSACTION READ ONLY` is accepted after `SET LOCAL` in one transaction before implementing D3's order)
- [x] 1.9 Codex review of the tests against specs/sql/execution/spec.md; resolve findings with the user
- [x] 1.10 Untyped (`None`) datasource probe sends `SET TRANSACTION READ ONLY` after the timeout, like Postgres — verify it fails now

## 2. Implementation (spec-implement)

- [x] 2.1 `slayer/core/warnings.py`: `StatementTimeoutSkippedWarning` + carrier + `AnySlayerWarning` member; verify 1.4 passes
- [x] 2.2 `slayer/sql/dialects/`: hooks on `SqlDialect` (D2); MySQL/Postgres/Snowflake/ClickHouse overrides; `MariadbDialect` registered by ds-type alias only (D7); verify 1.1–1.2 pass
- [x] 2.3 `slayer/sql/client.py`: `ExecutionResult`; `_apply_statement_timeout` + async sibling (D3–D5); lock-guarded per-engine permission cache (D4); delete `_with_ch_statement_timeout`, `_exec_clickhouse`, `_CH_*`, the `_settings_holder` import, the `db_type` branches and `_apply_type_probe_timeout` (+async); the read-only guard lookup resolves `None` through `dialect_for_ds_type`; verify 1.3, 1.6 and 1.10 pass
- [x] 2.4 Callers: `slayer/engine/query_engine.py` data-query + EXPLAIN paths attach warnings; every other `execute`/`execute_sync` caller reads `.rows`; verify 1.5 passes and the full unit suite is green
- [x] 2.5 Docs: rewrite the ClickHouse note in `docs/configuration/datasources.md` (request setting, readonly behaviour, warning) + one MariaDB sentence; add `statement_timeout_skipped` to the `warnings` row in `docs/concepts/queries.md` and any warning-kind list in `docs/reference/`; verify `grep -rn statement_timeout_skipped docs/`
- [x] 2.6 Gates: full unit suite, ClickHouse + Postgres integration (CI invocation), `ruff`, `basedpyright` (no new errors vs baseline), `tools/arch_check.py`, conventions gate

## 3. Review (spec-review)

- [ ] 3.1 Independent full review of the whole PR diff by the agent (the user asked for one: the original code was not written in this flow) — correctness, arc42 compliance, tests, docs — in addition to the /process-reviews loop
- [ ] 3.2 /process-reviews until every source is green
- [ ] 3.3 Comment on DEV-1940 that it is done by this PR (after archive)
- [ ] 3.4 `openspec archive clickhouse-statement-timeout` with the user's explicit go-ahead
