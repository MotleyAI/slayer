## Context

See proposal.md — Why. Governing principles: sql §3.1 (no parse/re-emit of
rendered SQL), §3.2 (dialect quirks only in `dialects/`), §3.9 (fail closed),
§3.13 (verbatim execution, one door), §3.14 (engine ownership), core §3.5
(degradations are structured warnings on both channels), system §3.7 (sync and
async share one path). The LikeC4 model needs no change: `client.py` (node
`sql`) already imports `sql.dialects`; `sql → core` and `engine → sql` cover the
new warning payload.

## Goals / Non-Goals

**Goals:** one timeout mechanism for every dialect behind hooks; no `db_type`
dispatch for timeouts in `client.py`; no statement rewriting.

**Non-Goals:** the ClickHouse native driver; timeouts for dialects that have
none today (BigQuery, T-SQL, DuckDB, SQLite, Tier 2); MariaDB live testing /
Tier 1 promotion (DEV-1975).

## Decisions

**D1 — Timeout beside the statement, not inside it.** ClickHouse's timeout is a
per-request HTTP setting: the dialect writes `max_execution_time` into the
checked-out DBAPI connection's `transport.ch_settings` for the call and restores
the key's prior state (value or absence) in a `finally`. Alternatives rejected:
the sqlglot `SETTINGS` rewrite (violates §3.1/§3.13; live probe showed it
rewrites `toStartOfMonth`→`dateTrunc`, `::`→`CAST`, `dateDiff`→`DATE_DIFF`,
comments); engine-level `connect_args` (loses the per-call timeout, and a
`readonly = 1` user then fails on the engine's first connect); the driver's
`execution_options={"settings": …}` (appends ` SETTINGS …` as text, breaks on an
existing clause or `FORMAT`). The request setting yields to a `SETTINGS` clause
in the SQL, so the SQL's own value wins without inspecting the SQL.

**D2 — Hooks on `SqlDialect` (no-op defaults).**
- `statement_timeout_sql(timeout_seconds) -> str | None` (existing): MySQL
  `SET max_execution_time = <ms>`, MariaDB `SET max_statement_time = <s>`,
  Postgres `SET LOCAL statement_timeout = <ms>`, Snowflake unchanged.
- `statement_timeout_best_effort: bool = False`; `True` on Postgres.
- `set_connection_timeout(dbapi_connection, timeout_seconds | None) -> object`
  / restore: ClickHouse only (returns the prior state; restore puts it back).
- `timeout_permission_sql() -> str | None` and `timeout_permitted(value) ->
  bool`: ClickHouse returns `SELECT getSetting('readonly')` and `value != 1`.

The client runs every statement through `_exec_verbatim` /
`_exec_verbatim_async` (§3.13); dialects never execute anything themselves.

**D3 — One orchestration helper per path.** `_apply_statement_timeout` (sync)
and its async sibling are the only timeout code in `client.py`, used by query
execution and the type probe alike; the four `db_type` branches and
`_apply_type_probe_timeout` (+async) are deleted. Order on each connection:
permission check (cached) → timeout → `SET TRANSACTION READ ONLY` (probe only)
→ statement → restore.

**D4 — Readonly detection by asking, once per engine (not by error text).**
The driver surfaces errors as text only; error 164 also covers readonly writes,
so matching it could disable timeouts for a whole engine after one write. The
permission result is cached per engine in a lock-guarded
`WeakKeyDictionary[Engine, bool]` in `client.py` (shared engines are used from
worker threads). The cache stores the hook's answer only, no dialect knowledge.
A failing permission check propagates like any connection error. A later change
of the user's level is seen only after the engine is rebuilt.

**D5 — Postgres best-effort.** `SET LOCAL` makes the timeout transaction-scoped
(the pool's rollback-on-return already reverts a plain `SET`; `LOCAL` makes it
explicit). On failure the client rolls back the aborted transaction, reports
`timeout_rejected`, and runs the statement. The timeout precedes `SET
TRANSACTION READ ONLY` in the probe so that rollback cannot discard the guard.
`None`/unknown types reach this path through `dialect_for_ds_type`'s existing
Postgres fallback (user decision: one lookup rule).
The probe's read-only guard resolves through the same lookup, so `None` gets
`SET TRANSACTION READ ONLY` like an unknown type.

**D6 — Session carry-over accepted for MySQL, MariaDB, Snowflake.** Their
session-scoped timeout persists on the pooled connection after the call. Each
client call sets its own value first, so client calls are always correct; the
leftover only bounds other users of the shared engine (ingestion,
introspection) by a SLayer-chosen timeout. A reset statement per call was
rejected: an extra round trip on every query (≥100 ms on Snowflake).

**D7 — `MariadbDialect(MysqlDialect)` registered by ds-type alias only.** It
shares `sqlglot_name = "mysql"`, so it joins `_BY_DS_TYPE` (taking the `mariadb`
alias from `MysqlDialect`) but not `_BY_SQLGLOT_NAME`; rendering by sqlglot name
still resolves `MysqlDialect`. Its only override is the timeout.

**D8 — Reporting.** New `StatementTimeoutSkippedWarning(SlayerWarning)`:
`kind = "statement_timeout_skipped"`, `datasource`, `timeout_seconds`,
`reason: Literal["readonly_user", "timeout_rejected"]`; `human_message` names
the remedy. A carrier `UserWarning` subclass (one wording on both channels, like
`SlayerNormalizationWarning`); the payload joins `AnySlayerWarning`.

**D9 — `ExecutionResult`.** `SlayerSQLClient.execute` / `execute_sync` return a
Pydantic `ExecutionResult(rows, warnings)`; each call reports its own events (no
shared state, no races). The engine's data-query and EXPLAIN paths append
`warnings` to `SlayerResponse.warnings`; other callers read `.rows`. Rejected: a
second `execute_with_warnings` method (two doors, the old one silently drops the
channel) and a post-call client query (races for per-call Postgres events).
`SlayerSQLClient` is not documented public API; all callers are in-repo.

## Risks / Trade-offs

- [`transport.ch_settings` is an undocumented `clickhouse-sqlalchemy`
  attribute] → unit test pins its use; the live ClickHouse suite breaks on any
  driver change.
- [Pooled-connection leakage of the ClickHouse setting] → restore in `finally`;
  spec scenario "ClickHouse setting does not outlive its call".
- [Postgres ordering: `SET TRANSACTION READ ONLY` after `SET LOCAL` in the same
  transaction] → believed legal (a `SET` takes no snapshot); verified by a
  Postgres integration test before relying on it.
- [MySQL 5.7 probes over heavy derived tables may now time out at 60 s] →
  accepted (user decision); fails loudly instead of hanging.
- [Unknown non-Postgres types get one rejected `SET` per query] → accepted
  (user decision); reported as `timeout_rejected`.

## Migration Plan

No data migration. Internal callers of `SlayerSQLClient.execute*` switch to
`.rows` in the same change.
