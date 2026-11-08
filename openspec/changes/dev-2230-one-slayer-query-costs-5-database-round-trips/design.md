## Context

Motivation: proposal.md. Measured baseline (Postgres 16, `log_statement=all`, 2026-11-08):

| Path | Per statement |
|---|---|
| psycopg (default driver), one event loop | `;` ping, BEGIN, `SET LOCAL statement_timeout`, query, ROLLBACK |
| psycopg2 (factory sync engine) | `SELECT 1` ping, BEGIN, SET LOCAL, query, ROLLBACK |
| psycopg via `execute_sync` / `run_sync` | CONNECT + 8 dialect-init statements + the four above, every call |

Snowflake (issue body, live): ~5.4 requests per query; 38 new connections at ~2.4 s each.

Sources:
- `pool_pre_ping=True` at six sites: `client.py` async builder, `engine_factory.py` fallback, the
  Snowflake / Trino / BigQuery (×2) build hooks.
- SQLAlchemy's autobegin plus close/reset-on-return rollback. In SQLAlchemy 2.0, `AUTOCOMMIT` alone
  still calls DBAPI `rollback()`; only `skip_autocommit_rollback` (2.0.43+) skips it, and only on
  dialects that implement `detect_autocommit_setting`.
- `_statement_timeout(_async)` sends the dialect's timeout statement before every statement.
- Snowflake `apply_session_overrides` runs on the pool `checkout` event.
- `async_utils.run_sync` runs `asyncio.run` per call, so the client's per-loop async engine dies with
  the loop.
- `SlayerQueryEngine.run_sync` and `_execute_bridged` call `aclose()`, which reaches
  `engine_factory.release_idle()` and drains a networked QueuePool. It was added to free DuckDB file
  handles.

No production path sends client text verbatim (facade absorbs or refuses session statements; model
SQL must be a read-only query), but `SlayerSQLClient.execute` is public API (D9).

Principles: sql §2 (dialect quirks only in `dialects/`), §13 (verbatim; a timeout travels beside
the statement), §14 (one engine owner; async engines released on their own loop), §15 (one
driver), new §17 (One request per statement); system §7 (async-first), §11 (versioned
persistence), §15 (fail closed), §23 (one database door), §24 (fail at save time).

## Goals / Non-Goals

**Goals:** the specs' "A warm statement costs one request" on every Tier-1 networked backend, on
async, sync and bridged paths; connection reuse across synchronous calls; the Snowflake job under 5
minutes.

**Non-Goals:**
- Batching the Snowflake sweeps (`test_every_snowflake_alias_failure_is_quoted`,
  `test_dev2120_library`); revisited once measured (task 10.3).
- Configurable idle threshold or pool sizing.
- CI runs against the SQLAlchemy minimum version.
- Storyline's own per-query pool disposal (DEV-1984, another repo).

## Decisions

### D1 Long-lived bridge loop
`run_sync` and `run_sync_on_caller_thread` submit to one process-wide event loop on a daemon thread,
created lazily. Tasks are created with `context=contextvars.copy_context()` of the caller. A call
from the bridge thread itself falls back to a private thread + `asyncio.run` (no self-deadlock).
`run_sync_on_caller_thread` keeps its queue-pumping protocol: the caller thread serves
`on_caller_thread` calls while the bridge task runs. If the waiting caller is interrupted
(`KeyboardInterrupt`, cancellation), the bridge task is cancelled thread-safely, the caller waits a
bounded time for its `finally` blocks, then re-raises the original interruption.
- Owner: the bridge owns the loop and a registry of shutdown callbacks. Shutdown is idempotent:
  refuse new submissions, run the callbacks on the loop (clients dispose their bridge-loop async
  engines), stop and join the thread. `atexit` runs it; the test-session leak gate runs it too.
- Fork: `os.register_at_fork(after_in_child=…)` replaces the bridge's locks and forgets the inherited
  loop, thread and registries without any network disposal; the child builds its own loop lazily.
- Alternative rejected: let a bridged call use the sync engine for drivers that also run sync
  (psycopg). Two paths for one driver, and asyncpg/aiomysql would still churn.

### D2 Release semantics
- `aclose()` keeps meaning full asynchronous disposal of the running loop's resources.
- A new, separately named end-of-workflow release frees only idle connections that hold a local
  database file. "Holds a local file" is judged from the resolved URL and pool type: a file-backed
  SQLite or DuckDB URL on a QueuePool. In-memory pools (StaticPool, SingletonThreadPool) are never
  drained.
- `SlayerQueryEngine.run_sync` and the client's `_execute_bridged` call the end-of-workflow release
  instead of `aclose()`. `short_lived_engine` keeps `aclose()` + `close()`.
- `SlayerQueryEngine.close()` and `SlayerSQLClient.close()` also release the client's bridge-loop
  async engine by scheduling its disposal on the bridge loop and waiting. A `weakref.finalize`
  backstop schedules the same for an unclosed client; bridge shutdown covers the rest.

### D3 Connection policy in one place
One policy helper in `slayer/sql/` builds the engine keyword arguments and attaches listeners for
the factory's sync builder, the dialect build hooks (which pass their own arguments through it) and
the client's async builder. Each dialect declares its transaction mode:
- **DBAPI autocommit:** `isolation_level="AUTOCOMMIT"` + `skip_autocommit_rollback=True`. Postgres
  (psycopg, psycopg2, asyncpg, pg8000 where it detects autocommit), MySQL/MariaDB
  (pymysql, mysqldb, aiomysql). Each driver is verified live; one that can't detect autocommit falls
  to the dialect-supplied form.
- **Dialect-supplied autocommit:** SQL Server turns on pyodbc `autocommit=True` at connect (its SA
  dialect rejects `AUTOCOMMIT` as an isolation level), and Snowflake skips its unconditional
  `ROLLBACK`. In both, the SLayer dialect makes rollback and commit no network request once the
  connection is in autocommit.
- **Rollback is local:** BigQuery and ClickHouse (DBAPI rollback is a no-op) and Trino (already
  AUTOCOMMIT): nothing to change.
- **Transactional:** a `transaction_pooler` Postgres datasource (D7).

SQLite/DuckDB keep their current behaviour (in-process, no round trip). The commit after a
statement returning no rows stays and is free under autocommit. SQLAlchemy is pinned to `>=2.0.43`.

### D4 Timeout once per connection
- The dialect hook declares its timeout's scope: session statement (Postgres `SET`, MySQL, MariaDB,
  Snowflake), transaction-local statement (Postgres under `transaction_pooler`), or DBAPI-side
  (Trino, ClickHouse; unchanged, no request).
- For session statements the client stores the value in force in the pool connection record's
  `info`, and sends the statement only when the needed value differs. Any failure of the timeout
  statement clears the stored value. A record is fresh on connect, so an invalidated and replaced
  connection always re-sends.
- A best-effort rejection (Postgres family) is stored as "rejected at value v": no rollback under
  autocommit; later calls at v skip the statement and still report `timeout_rejected`. Only the
  transactional path rolls back after a rejection.
- Type probe on a Postgres-family autocommit connection: session timeout first (outside any
  transaction), then a real read-only transaction (`BEGIN` via the driver's transaction API on that
  checkout, `SET TRANSACTION READ ONLY`), the probe, and `ROLLBACK` in a `finally`. If the rollback
  fails the connection is invalidated. The probe leaves the stored value at 60; the next 120-second
  query re-sets it once.

### D5 Snowflake overrides on connect
`apply_session_overrides` moves to the pool `connect` event (dispatch order: UDFs, then overrides,
then the policy listeners). Its "stray `USE` can't leak" rationale is now answered by D9. The
build-hook error message that says overrides "fire … on every pool checkout" is updated.

### D6 Ping after idle
A `checkout` listener (sync pool; the async engine's pool through `sync_engine`) reads a monotonic
`last_checkin` stamp from the connection record. If it is older than 60 s, it pings with the
dialect's `do_ping`; a failed ping raises `DisconnectionError`, so the pool replaces the connection
and calls the listener again. The stamp is written by a `checkin` listener only after a successful
use and cleared on `connect` and `invalidate`, so a fresh or replacement connection is never
pinged. StaticPool engines get no listener.

### D7 `transaction_pooler`
- `DatasourceConfig.transaction_pooler: bool = False` is additive: no version bump, no migration.
- `StorageBackend.save_datasource` calls an sql-side check: only `postgres`/`postgresql` types are
  accepted; untyped, unrecognised and other types are refused with an error naming the type.
- Engine build repeats the check against the resolved SQLAlchemy backend name (`postgresql`).
- The flag joins `_runtime_fingerprint`, so the engine cache key differs.

### D8 Snowflake suite
- `sa_statement_fails` sweeps run with a worker count no larger than the engine pool size.
- Seeding fixtures open one raw connection each.
- The job time is measured; batching is revisited before the issue closes.

### D9 Session and transaction statements refused at the client door
The default client's `execute` / `execute_sync` tokenise the text with sqlglot's tokenizer for the
datasource's dialect (no parse). They split it into statements at top-level `;` and refuse, with a
new `SessionStatementRefused(SqlClientError)`, any statement whose leading keywords are `USE`,
`SET`, `RESET`, `ALTER SESSION`, `DISCARD`, `BEGIN`, `START TRANSACTION`, `COMMIT` or `ROLLBACK`.
Literals, quoted identifiers and comments are tokens, so `SELECT 'SET x'` passes. The client's own
session statements (timeouts, probe transaction) do not pass through this check; they are built by
dialect hooks. Where the tokenizer raises, the text is refused (fail closed).

### D10 Measurement
Before any production change (task 1), count requests and new connections per warm statement on
Postgres, MySQL, SQL Server, ClickHouse, Trino, BigQuery and Snowflake, on async, sync and bridged
paths. Count again after (task 11), and post both tables on DEV-2230. Server-side counting where
the server logs statements: Postgres `log_statement`, MySQL `general_log`, ClickHouse
`system.query_log`, Trino `system.runtime.queries`, Snowflake `QUERY_HISTORY_BY_SESSION`. Otherwise
count client-side at the DBAPI or HTTP layer: SQL Server, BigQuery, and pings that the server does
not log.

## Risks / Trade-offs

- [Session timeout behind a transaction-mode pooler can miss the query or leak into other sessions]
  → `transaction_pooler` opt-in; documented in `docs/configuration/datasources.md`.
- [Bridge loop is process-global state (threads, forks, Jupyter, shutdown)] → D1 owner + at-fork
  hook + atexit; tests for nesting, fork during submit, cancellation points, caller-thread queue,
  context propagation and leak gate.
- [A driver's autocommit detection differs from its docs] → per-driver live verification in task 1;
  the per-backend statement-count tests catch regressions.
- [Narrowed public `execute` contract] → typed refusal naming the keyword; one-sentence contract
  update in `docs/configuration/sql-clients.md`.
- [A connection killed within 60 s of use fails its next statement] → existing transient retry.

## Decisions log

1. Fix sync-path connection churn too: as many causes as possible unless epic. Options: both
   causes here / idle-release rule only / neither. (user)
2. Postgres timeout session-level, set once per connection, plus a `transaction_pooler` opt-in that
   keeps it transaction-local. Options: session-level only / transaction-local only /
   session-level + opt-in. (user)
3. `transaction_pooler` is a boolean on `DatasourceConfig`, false by default, part of the engine
   cache key, refused at save time on non-Postgres types. (default)
4. Liveness: ping only after 60 s idle, a constant, from one factory listener; all
   `pool_pre_ping=True` removed. Options: after-idle ping / no ping + retry. (user)
5. Snowflake sweeps keep one statement and one query per case; workers sized to the pool; job time
   measured and batching revisited before the issue closes. Options: per-case / batch. (user)
6. New sql principle (user, approved wording):
   "17. **One request per statement**: on a warm connection a statement costs the database one
   request — session state is set once per connection and re-set only when it changes, liveness is
   checked only after idle, and connections persist across calls, synchronous ones included; a
   datasource marked as reached through a transaction-mode pooler keeps its timeout
   transaction-local instead. [enforced: test:tests/test_law_one_request_per_statement.py]"
7. `architecture/system.arc42.md` §3: the second "18. **Fail at save time**" renumbered to 24. (user,
   approved wording: `24. **Fail at save time**`)
8. Snowflake overrides move from `checkout` to `connect`. (default)
9. Postgres probe: timeout set before the read-only transaction opens. (default)
10. File-backed SQLite/DuckDB keep idle release at the end of a synchronous workflow; networked
    pools don't. (default)
11. SQLAlchemy minimum raised to 2.0.43. (default)
12. Before/after measurements on every Tier-1 networked backend, recorded on the issue. (default,
    from the issue)
13. Per-dialect transaction mode (DBAPI autocommit / dialect-supplied autocommit / rollback is local
    / transactional); pyodbc autocommit at connect for SQL Server. (codex)
14. `skip_autocommit_rollback` only on dialects verified to detect autocommit; Snowflake's skip is
    SLayer-supplied. (codex)
15. Bridge loop owner with idempotent shutdown (refuse, dispose on loop, stop, join; atexit; leak
    gate covers it). (codex)
16. Fork safety via `os.register_at_fork(after_in_child=…)`, no network disposal in the child;
    tested while another thread submits. (codex)
17. Cancellation: task created with the caller's copied context; interruption cancels the loop
    task, waits bounded for its cleanup, re-raises the original. (codex)
18. `run_sync_on_caller_thread` keeps its queue-pumping protocol on the bridge loop; nested,
    raising-callback, cancellation and Jupyter tests. (codex)
19. A rejected timeout under autocommit sends no rollback; replaced connections never inherit stale
    success or rejection records. (codex)
20. Probe read-only transaction always rolls back in `finally`; a failed rollback invalidates the
    connection. (codex)
21. Idle-ping bookkeeping: monotonic, stamped on successful checkin, cleared on connect/invalidate;
    fresh or replacement connections never pinged. (codex)
22. "Warm" defined in the spec; ping-after-idle, timeout change and reconnect setup tested as
    separate transitions. (codex)
23. "Holds a local file" judged from resolved URL plus pool type; in-memory pools never drained.
    (codex)
24. `aclose()` stays full disposal; a separately named end-of-workflow release is used by
    `run_sync`. (codex)
25. `transaction_pooler` accepted only on explicit `postgres`/`postgresql`; engine build checks the
    resolved backend. (codex)
26. SQLAlchemy lower bound and lock updated. (codex) Rejected (codex): an extra CI run at the minimum
    SQLAlchemy version — the repo tests no dependency at its floor; out of scope.
27. The default client refuses session-changing and transaction-control statements with a typed
    error (sqlglot tokenizer, every `;`-separated statement). Options: refuse at the door /
    document only. (user, from codex findings 3–4)
