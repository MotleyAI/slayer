## Context

See proposal.md (Why). Every engine SLayer builds already passes one of two doors:

1. `connection_policy.engine_kwargs`. It serves:
   - the factory's default `create_engine`;
   - the dialect `build_engine` hooks (Snowflake, BigQuery, Trino);
   - the client's `create_async_engine`.
2. `build_in_memory_sqlite_engine`, which is in-memory and therefore exempt.

Per-connection setup already exists as `connect` listeners attached in `engine_factory._build_engine`: the SQLite UDFs and Snowflake's session overrides. A per-connection hook forces sync connections (`SqlDialect.needs_sync_connections`), which is why SQLite and Snowflake refuse async drivers.

Today's partial read-only measure is `client._read_only_transaction_sql`: `SET TRANSACTION READ ONLY` around type probes on postgres, redshift and oracle.

The default client's `execute` admits DDL and DML (`admit_statement` returns `writes` only to drive commit and retry). The bare client's `execute_sync` releases local-file connections only on its async-driver path.

Principles that bind this change:
- `python.database` §3.2: rendered SQL is verbatim, so nothing may be prepended to a query.
- §3.3: one engine owner (`create_engine` only in the factory and the dialect hooks).
- §3.4: one driver; the URL is never rewritten, and its query parameters reach the driver unchanged.
- §3.5: one request per statement.
- §3.6: one thread per connection.
- New §3.7 (below).
- `system` §3.4: SQL is built as AST, apart from fixed driver-level transaction statements, which the existing type-probe guard already uses.

## Goals / Non-Goals

**Goals:**
- Read-only connections on every live-tested backend that has a mode, as specified in `specs/database/execution`, with no extra request per statement.
- A client admission rule that holds on every backend, including those without a read-only mode.
- A structural fix: dialect hooks applied at the doors every builder already uses, rather than per-builder patches.

**Non-Goals:**
- Redshift connection-level read-only, deferred to DEV-2228. Redshift is not Tier 1, so it can't be verified live; its type-probe guard and the client refusal stay.
- An opt-out field on `DatasourceConfig`.
- Detecting side effects of functions or procedures called inside an admitted read.
- BigQuery's read-only OAuth scope: its documented query path (`jobs.insert`) does not accept it.
- Read-only for Oracle, SQL Server, Snowflake, BigQuery, Trino, Presto/Athena, Databricks or Spark connections: none has a usable connection-level mode.
- ClickHouse drivers other than `clickhouse+http`. They are refused, as the specs state.

## Decisions

### Approach

**One read-only hook family on `SqlDialect`, no-op by default.** It has four parts:

1. **Engine arguments.** Given the parsed URL, the existing kwargs and whether the build is async, the dialect returns read-only additions. `connection_policy.engine_kwargs` merges them, so the factory default, the dialect `build_engine` hooks and the async builder all get them.
   - Postgres: connect args `options` (psycopg/psycopg2), `server_settings` (asyncpg) or `startup_params` (pg8000), carrying `default_transaction_read_only=on`. Behind a pooler it is the `postgresql_readonly` execution option instead, on psycopg, psycopg2 and asyncpg.
   - MySQL/MariaDB: `init_command` = `SET SESSION TRANSACTION READ ONLY`.
   - DuckDB: `read_only=True` for a file database.
   - **Merge rule:** strings with list semantics (`options`) are appended, dict settings are merged, and ours wins only for the read-only key itself. Every other URL or kwarg value reaches the driver unchanged (§3.4).
   - A URL that already sets MySQL's `init_command` is refused, because two init statements would need the multi-statement client flag.
2. **Per-connection setup.** A `connect` listener attached in `_build_engine`, beside the UDF and session-override listeners:
   - SQLite file: `PRAGMA query_only = ON`.
   - ClickHouse: see below.
3. **Transaction start.** A `begin` listener for pg8000 behind a pooler. pg8000's DB-API sends its own `begin transaction` before a transaction's first statement. The listener sends `START TRANSACTION READ ONLY` in its place, by flipping the DBAPI connection to autocommit for that one statement. pg8000 then sees the open transaction and skips its own begin. It uses only public DB-API, and the same request count was probed against a pytest-postgresql server's statement log. pg8000 is sync-only, so this needs no async handling.
4. **Type-probe guard.** The `SET TRANSACTION READ ONLY` statement for postgres, redshift and oracle. This replaces `_READ_ONLY_TXN_DIALECTS`; behaviour is unchanged.

**Local-database classification is a dialect question.** `SqlDialect` answers, for a URL, "in-memory / local file at path P / not local":
- SQLite covers `sqlite://`, `:memory:`, `file:` URIs with `mode=memory` or `:memory:`, and relative and absolute paths.
- DuckDB covers `:memory:` and empty forms.

`engine_factory._is_in_memory_sqlite`, `holds_local_file`, the missing-file refusal and the client's in-memory exemption all read this one answer. The missing-file refusal runs before any connect, in both the sync and async builders. DuckDB's native "database does not exist" error is also mapped to the same typed database-unavailable failure, which narrows the window between the check and the connect.

**ClickHouse lifecycle.**
- A `connect` listener on ClickHouse engines reads the engine's server profile. On the first connection it fills the per-engine cache by running the existing profile probe on that DBAPI connection, under the existing per-engine profile lock; later connections reuse the cached value.
- When the profile reports `readonly = 0`, the listener sets `readonly = 2` in that connection's per-request settings. Because the probe runs before `readonly = 2` is set, the profile records the user's true level, so `timeout_permitted` and the correlated-subquery refusal behave as today.
- Reflection-first use, reconnects and concurrent first checkouts are all covered, because every connection passes the listener.
- The per-engine profile cache moves out of `client.py` into a module that both `engine_factory` and `client` import, since `engine_factory` cannot import `client`.

**Client admission.** `admit_statement` refuses a text with a new typed `WriteStatementRefused` (a `SqlClientError`) when any statement's lead is outside the read leads, or when it holds a write keyword. Two changes:
- `EXPLAIN` leaves the read leads.
- `INTO` joins the write tokens (`SELECT … INTO`).

A datasource classified as in-memory skips the write rule but keeps the session rule. `explain()` checks its plan statement: the dialect's EXPLAIN prefix followed by an admitted statement, or, for a session-mode dialect, an admitted statement. `writes` keeps driving commit and retry, which now matters only for in-memory databases.

**`execute_sync` release.** The sync-driver branch of `SlayerSQLClient.execute_sync` releases idle local-file connections in a `finally`, as the bridged branch already does.

### Decisions log

1. **No opt-out field.** Options were none, or `read_only: bool = True` on `DatasourceConfig`. Taken: none, because the guarantee should not be switchable by whoever can edit a datasource. (user)
2. **Fix the bare client's `execute_sync` local-file release in this change.** It was a bug against "A local database file is released after a synchronous workflow", and read-only connections turn it into a hard error. (user)
3. **The client refuses non-read statements except on in-memory databases.** Options were leaving admission unchanged (writes reach the database, which rejects them only where read-only), or refusing them. Taken: refuse, as the only rule that holds on backends without a read-only mode. (user)
4. **ClickHouse sends `readonly = 2` only when the profile reports 0.** Options were that, or relying on the client refusal alone. (user)
5. **arc42 `python.database` §3.7 wording.** The first approved wording was replaced after Codex finding 9, because it overclaimed. Final (user, approved wording): "7. **Read-only datasources**: except into an in-memory database, a connection is read-only wherever its backend is live-tested and allows it, and the client sends no statement it cannot classify as a read. [enforced: test:tests/test_law_read_only_datasources.py]"
6. **arc42 `python.database` §3.5 amendment.** `run_sync` already contradicted it, and the `execute_sync` fix would contradict it again. Edit (user, approved wording): "…and connections persist across calls, synchronous ones included, save idle ones holding a local database file, which a synchronous call releases on return; …"
7. **`EXPLAIN` only through `explain()`.** Options were dropping `EXPLAIN` from `execute`'s read leads with `explain()` checking its plan statement, or parsing each dialect's EXPLAIN option grammar in the client. Taken: the first; nothing in SLayer sends `EXPLAIN` through `execute`. (user; codex finding 2)
8. **pg8000 behind a pooler.** Options were refusing it at build, an extra `SET TRANSACTION READ ONLY` per transaction, no enforcement, or swapping pg8000's own begin for `START TRANSACTION READ ONLY`. Taken: the swap, which keeps the same request count (probed). (user; codex finding 3)
9. **Redshift is out of this change and deferred to DEV-2228,** which has a comment recording the mechanism to add. It is not Tier 1. Options were applying the transaction-start swap without live verification, or deferring. (user; codex finding 3)
10. **Keep the type-probe `SET TRANSACTION READ ONLY` for Postgres, Redshift and Oracle,** rather than dropping it for Postgres. Without it, a caller-supplied engine's probe would be writable. (codex finding 1)
11. **ClickHouse lifecycle is a per-connection listener with the engine-cached profile,** rather than kwargs fixed before the profile is known. Other ClickHouse drivers are refused. (codex finding 4)
12. **Typed per-driver merges, and a MySQL URL with its own `init_command` is refused.** The alternative was a shallow kwargs update, which would drop user values. (codex finding 6)
13. **One dialect-owned local-database classifier,** with tests for every URL form and DuckDB's native missing-file error mapped too. The alternative was ad-hoc `exists()` checks. (codex finding 7)
14. **A construction-path test matrix:** factory default, dialect `build_engine`, async builder, reconnect, and direct engine use. (codex finding 8)
15. **Rejected: SQLite `mode=ro` for the main database.** It needs the URL rewritten to `file:` URI form, against §3.4. The client refuses `PRAGMA`, and `query_only` also covers attached and temp databases, which `mode=ro` does not. (rejected (codex), finding 10)
16. **A backend that rejects the read-only setting fails the connection with its own error.** There is no warn-and-continue; this fails closed. (default)
17. **SQLite uses `PRAGMA query_only` rather than `mode=ro`,** for the same reasons as 15. (default)
18. **Postgres mechanism:** startup `default_transaction_read_only` when direct (it holds under autocommit; verified on every driver), and the `postgresql_readonly` execution option behind a pooler (`BEGIN READ ONLY`, verified on psycopg, psycopg2, asyncpg and async psycopg). (default)
19. **Oracle keeps only the type-probe guard,** since per-statement `SET TRANSACTION READ ONLY` would cost a request. BigQuery's read-only scope is not used. (default)
20. **A missing local DuckDB or SQLite file is refused before connecting,** rather than an empty file being created. DuckDB already fails natively, and SQLite is aligned with it. (default)
21. **The read-lead set is otherwise unchanged,** so a DuckDB `FROM t` or a `PRAGMA` is refused (fail closed). (default)
22. **A caller-supplied engine is not modified.** The client refusal still applies to it. (default)

## Risks / Trade-offs

- [DuckDB same-process conflict] A process holding a read-write DuckDB connection to a file SLayer then opens read-only, or the reverse, gets "different configuration". → The sync APIs now release after every call. The demo builder already invalidates the factory engine before writing. Docs say to close your own write connection before SLayer's next call.
- [Demo reuse path] `ensure_demo_datasource` writes (date shift) after `invalidate_engine`. If a SLayer connection is checked out concurrently, the open fails. → It already raises a clear `SlayerError` there, and the builder runs at startup.
- [Conservative admission] Some legitimate reads are refused: DuckDB `FROM t`, `SUMMARIZE`, `PRAGMA`. → The typed error names the lead. Callers can rewrite the query as `SELECT`, or use their own driver.
- [ClickHouse per-request `readonly = 2`] A settings-profile constraint could reject it. → It fails closed with ClickHouse's error, and is covered by the integration suite.
- [pg8000 begin swap] It depends on pg8000 honouring a transaction already open on the server. → A unit test against pytest-postgresql pins the statement log.
- [Existing tests] About ten tests and the v0.10.2 fixture generator write through SLayer-built engines or the default client. → Their seeding moves to raw drivers. pr-tests asks approval per file.
