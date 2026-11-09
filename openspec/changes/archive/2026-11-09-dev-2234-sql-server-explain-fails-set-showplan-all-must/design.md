## Context

See proposal.md — Why. Current shape: `SqlDialect.build_explain_sql(sql)` returns `f"{explain_prefix} {sql}{explain_postfix}"`; the engine (`query_engine.py`, explain branch) sends it through `client.execute`. T-SQL's prefix/postfix are `SET SHOWPLAN_ALL ON;` / `; SET SHOWPLAN_ALL OFF`.

Live probe (SQL Server 2022, pyodbc through SQLAlchemy `exec_driver_sql`, 2026-11-08):

- the single batch fails with error 1067;
- `SET SHOWPLAN_ALL ON`, the query, `SET SHOWPLAN_ALL OFF` as three statements on one connection work; the query returns plan rows (`StmtText`, `StmtId`, `NodeId`, `Parent`, `PhysicalOp`, `LogicalOp`, …);
- `OFF` after a failing plan statement succeeds;
- a connection left with SHOWPLAN on keeps it after pool return (the pool's rollback does not reset it): the next checkout gets plan rows;
- plan rows must be fetched in full before `OFF` (an unconsumed result can make ODBC report "connection busy").

pyodbc is SQL Server's default driver and is sync-only, so SQL Server explains run on the client's threaded sync path; `mssql+aioodbc` would take the async path.

Principles that bind: system §3.6 (AST-built SQL; EXPLAIN keyword prefix excepted, approved), python.sql §3.1 (AST end to end; same exception), §3.9 (fail closed with a typed error), §3.13 (rendered SQL executes verbatim through `_exec_verbatim` / `_exec_verbatim_async`), §3.14 (one engine owner), system §3.7 (async-first), §3.23 (one database door).

## Goals / Non-Goals

**Goals:**

- SQL Server `explain=True` works and never leaves a connection in plan mode.
- The "session state left behind" class is handled once, in the one client operation that runs explain statements.

**Non-Goals:**

- Changing what other dialects' EXPLAIN returns (Postgres / DuckDB / Trino `EXPLAIN ANALYZE` still execute the query, as today).
- Refusing `SET` in `execute` (DEV-2230).
- An AST form of the EXPLAIN keyword prefix: sqlglot parses EXPLAIN only as `Command`, and the compiled sqlglot (`sqlglotc`) rejects custom `Expression` subclasses ("interpreted classes cannot inherit from compiled").

## Decisions

1. **The client operation takes a typed `ExplainStatements`, not bare SQL** (user). The engine asks the dialect for it, so every statement is SLayer-rendered and reaches the client verbatim (§3.13), a gateway needs no dialect knowledge, and the unsupported-dialect refusal stays in one place. Rejected: `explain(sql)` with the client composing the form — it hides SLayer's rendered EXPLAIN from gateways and splits the refusal.
2. **`ExplainStatements(BaseModel, frozen)`** in `slayer/sql/dialects/base.py` next to `ServerProfile`: `statement: str`, `session_on: str | None`, `session_off: str | None`; a validator requires both session statements or neither (default).
3. **Session statements are AST from a `bool`-operand hook** `explain_session(enabled: bool) -> Expression | None`: base `None`; `TsqlDialect` returns `exp.Set` for `SHOWPLAN_ALL ON` / `OFF`. Rendered once in `explain_statements`. Satisfies §3.6 and the §3.1 hook-operand law (default).
4. **`explain_statements(sql: str) -> ExplainStatements`**: with a session hook, `statement` is the bare `sql`; else `statement = f"{explain_prefix} {sql}"`; neither → `ExplainUnsupportedError`. T-SQL sets `explain_prefix = None` (default).
5. **The keyword prefix stays text composition, as a named arc42 exception** (user, approved wording) — system §3.6: "SQL is built as sqlglot AST, never by string concatenation of fragments, save a dialect's EXPLAIN keywords before one rendered statement, which sqlglot cannot represent." python.sql §3.1: "rendered to text once (EXPLAIN prefix excepted, system §3.6); text round-trips of already-emitted SQL are forbidden". Raised by Codex (High): no AST form exists (see Non-Goals) (codex).
6. **One typed refusal `ExplainUnsupportedError(SlayerError)`** in `slayer/core/errors.py` for BigQuery and for session-mode EXPLAIN on a client without `explain`; it is a `ValueError`, so today's `except ValueError` keeps catching BigQuery's (default).
7. **Run order and restore**: one connection; `session_on`; `statement` with rows fetched in full; `session_off` in a `finally` on any `BaseException`. On the async path the switch-off is shielded (`asyncio.shield`) so a second cancellation cannot skip it, and a switch-off that is not confirmed (it raised, or was cancelled) invalidates the checked-out connection (`invalidate()`) before the connection context exits. On the threaded path the worker thread finishes the sequence itself, so a cancelled caller cannot interrupt the `finally`. Error precedence: the plan statement's error, else the switch-off's (codex).
8. **Timeout, retry and typed failures match `execute`**: same `_statement_timeout` / `_statement_timeout_async` context (timeout statements before `session_on`) and skip reporting in `ExecutionResult.warnings`; same `_RETRYABLE_ERRORS` retry, repeating the whole sequence; the invalidation in (7) happens inside the attempt, so a retry always checks out a fresh connection; same auth-failure engine discard and `_typed_failure`. Never commits (default + codex).
9. **`DelegatingSqlClient.explain` forwards to `inner`**; `BorrowedSqlClient` inherits it (default).
10. **Compatibility via `adapt_client`, following the 1.6.0 `get_column_types` precedent** (default). Detection uses the same identity checks `adapt_client` uses today: a non-delegating client is legacy when its `explain` is absent or is the `SqlClient` Protocol stub; a `DelegatingSqlClient` subclass is legacy when its class's `execute` is not `DelegatingSqlClient.execute` but its `explain` is `DelegatingSqlClient.explain` (resolved through the MRO, so an intermediate subclass counts). A legacy client's single-statement explain runs `statement` through its `execute` with `warn_deprecated(..., removal="2.0")`; a session-mode explain raises `ExplainUnsupportedError` before any statement. The wrapper composes with the existing 1.6.0 probe adapter (both may apply to one client) (codex: detection shapes specified and tested).
11. **`SqlDialect.build_explain_sql` stays deprecated until 2.0** (user): single-statement dialects return today's string; T-SQL and BigQuery raise `ExplainUnsupportedError`; every call warns via `warn_deprecated(..., removal="2.0")` naming `explain_statements`. `explain_postfix` and the engine's `_build_explain_sql` are removed (user).
12. **Both new 2.0 sites are pinned in the removal tripwire** (`PINNED` in `tests/test_law_removal_tripwire.py`, and the covered list in `compatibility/removal-tripwire`) (user). Recorded for removal on DEV-2231.
13. **Conformance `explain` check**: explain a read-only fixture query with the dialect's `ExplainStatements`; pass when rows are non-empty and the client's next `execute` of a fixture query returns its data rows. No per-backend plan column pinning (plan shapes differ by backend). Broken-client cases: no `explain`, and an `explain` that leaves plan mode on (codex).
14. **Verbatim-door coverage**: all three statements go through `_exec_verbatim` / `_exec_verbatim_async`; tests beside `tests/test_dev1933_verbatim_execution.py` cover the explain path (codex).
15. **Live SQL Server test forces connection reuse**: the client is built over a caller engine with a one-connection pool (`pool_size=1, max_overflow=0`), so "the next query returns data" proves that connection was restored. Live coverage: a successful explain and a failing plan statement. The switch-off-failure, cancellation and retry paths are unit-tested with fakes, because they cannot be triggered reliably on a live server (codex).
16. **Docs**: `docs/configuration/sql-clients.md` gains the `explain(statements)` row, one sentence on the deprecated route, and an `explain` override in the statements-only gateway example; `docs/database-support.md`'s stale BigQuery sentence is corrected (`explain=True` raises `ExplainUnsupportedError`) (default).
17. **No dependency on DEV-2230**; `execute` is unchanged (default).
18. Rejected (codex): adding arc42 text naming the restore invariant — arc42 principles stay normative and minimal; the invariant is specified in `sql/client-contract`.

## Risks / Trade-offs

- [Third-party clients fail the new conformance `explain` check and type-check against the Protocol] → intended contract growth; runtime keeps working through the deprecated route until 2.0, documented in `sql-clients.md`.
- [Direct callers of `build_explain_sql` on SQL Server now get an error instead of a string] → that string never ran on SQL Server; the error names `explain_statements`.
- [`TsqlDialect().explain_prefix` changes to `None`] → internal attribute; T-SQL's form lives in `explain_session`.
- [A future sqlglot drops `exp.Set` rendering for `SHOWPLAN_ALL ON`] → the dialect unit test pins the rendered text.
