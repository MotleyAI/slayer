## Context

See proposal.md — Why. Findings that shape the approach:

- SQLAlchemy's `_get_table_sql` (sqlite/base.py) runs `… "<schema>".sqlite_temp_master` first whenever a
  schema is passed and relies on catching the failure. `get_pk_constraint`, `get_foreign_keys`,
  `get_unique_constraints` always reach it; `get_columns` reaches it for tables with a generated column.
  CPython's `_sqlite` builds the error from `sqlite3_errcode(db)` after re-taking the GIL
  (`statement.c`); when that reads `SQLITE_OK` the failure is a `SystemError`, which SQLAlchemy's
  `except DBAPIError` does not catch. What resets the code in that gap is not established (seen ~1 in
  25 runs on 3.14.10, never on 3.12); removing the always-failing statement makes the gap unreachable.
- A thread-overlap probe on 3.14 found 69–91 overlapping uses per shard-3 run of the in-memory
  `StaticPool` connection (`test_concurrent_async_calls_share_in_memory_db`), none on file engines.
  CPython #118172 reports wrong rows under such sharing.
- A full-suite probe found no same-thread nested checkout of an in-memory engine.
- Principles that apply: database 1 (one database door), 3 (one engine owner), 6 (new: one thread per
  connection); sql 1 (AST end to end), 2 (dialect quirks only in `dialects/`); system §3.4 (SQL as AST);
  live_schema 1 (idempotent, additive-only ingestion — a re-ingest fills a lost key).

## Goals / Non-Goals

**Goals:**
- No SQLite catalog read SLayer makes depends on a statement failing.
- No connection SLayer opens is used by two threads at once.
- A failed key read is never silent.

**Non-Goals:**
- Establishing what resets the SQLite error code on 3.14 (made unreachable instead).
- Passing no schema to SQLAlchemy's Inspector on SQLite: unqualified pragmas search every attached
  database, breaking schema scoping.
- Failing a table closed on a key-read failure (user decision: keep the table, report the loss).
- Changing CLI exit status or HTTP status on a failed key read.

## Decisions

1. **Scope: remove the trigger** — SQLite reads its own catalog, plus in-memory exclusivity, a law test
   and reporting. Options: A (detector-first, trigger left in place), B (investigate until reproduced),
   A′ (this). A′ taken after the always-failing statement was identified. (user)
2. **A failed key read keeps the table and is reported** on the ingest result in every surface.
   Alternative: fail the table closed — rejected: reverses `models/fk-ingestion` and would keep a table
   with permanently unreadable keys out forever. (user)
3. **SQLite columns are read from SQLite's own catalog too**, typed through SQLAlchemy's SQLite
   type-affinity resolver (`_resolve_type_affinity`) and the existing `column_from_sa_type` path, so
   types are identical. Alternative: keys only — leaves generated-column tables on the failing
   statement. The private resolver is pinned by a parity test across the supported SQLAlchemy range.
   (user)
4. **arc42 principle 6**, with the law test widened to a structural pool check for every Tier 1
   database. Alternative: SQLite-only wording. (user, approved wording)
   "6. **One thread per connection**: a database connection is used by one thread at a time.
   [enforced: test:tests/test_law_one_thread_per_connection.py]"
5. **arc42 principle 3 wording** follows the pool change (user, approved wording):
   "`reset_cache` always dispose; `:memory:` is built by its single builder,
   keyed per datasource), `SlayerSQLClient` for its private in-memory"
   replacing "`reset_cache` always dispose; `:memory:` is built by its single StaticPool
   builder, keyed per datasource), `SlayerSQLClient` for its private in-memory".
6. **In-memory exclusivity: a one-slot `QueuePool` subclass** (`pool_size=1`, `max_overflow=0`, SQLAlchemy's
   standard pool timeout) over the one in-memory connection, `check_same_thread=False` kept. The
   subclass records the thread holding its slot (QueuePool's get/return pair one-to-one) and fails a
   same-thread re-checkout at once with an error naming the nesting. Alternatives: a lock-holding
   `StaticPool` subclass — deadlocks, since SQLAlchemy checks the StaticPool record out twice on a
   nested connect but returns it once; a dedicated owner thread per engine — more machinery for the
   same guarantee. (default; nested fail-fast from codex)
7. **The in-memory database persists exactly as long as today**: until its connection is invalidated or
   the engine disposed (StaticPool also rebuilt an invalidated record into a fresh database). (codex)
8. **Lost key reads leave exit and HTTP status unchanged** — skips fail the CLI because `--exclude` is
   their remedy; here the remedy is a re-ingest. (default)
9. **Failed reads ride on the key-read result** (`TableKeys.failed_reads: list[KeyReadFailure]`, default
   empty) so gateway clients stay conforming; `best_effort` produces the record for every dialect,
   DuckDB included. (default)
10. **Reported failures carry the table's location** (name, schema, catalog where the database has one)
    and are de-duplicated by location and read; the scan returns its failures explicitly (not via the
    local key cache), including for a table later skipped, and the scan report collects them across
    schemas. (codex)
11. **SQLite reads**: table-valued pragmas built as sqlglot AST, run through `reflection.fetch`, schema as
    the pragma's schema argument. PK: `pragma_table_xinfo` `pk > 0` ordered by `pk`. FKs:
    `pragma_foreign_key_list` grouped by `id`, ordered by `seq`; a NULL `to` resolves to the referred
    table's primary key; referred schema is the table's own. Unique constraints: `pragma_index_list`
    `origin = 'u'`. Unique indexes: `origin = 'c'`, `unique = 1`, `partial = 0`, and every
    `pragma_index_xinfo` `key = 1` member a plain column — any expression member (`cid = -2`) discards
    the whole index (codex). Columns: `pragma_table_xinfo`, `hidden = 1` skipped, generated (2/3) kept.
    SQLite stops using `primary_key_inspector_only`. (default)
12. **"No catalog read depends on a failing statement" lives in the SQLite spec deltas**, not arc42 — it
    is a SQLite quirk. (default)
13. **Corpus covers keyless tables, views, a missing table and a virtual table** in addition to the key
    shapes, asserting today's results. (codex)

## Risks / Trade-offs

- [`_resolve_type_affinity` is private SQLAlchemy API] → parity test against the Inspector's column types
  on every SQLAlchemy release the range allows.
- [Own pragma reads diverge from SQLAlchemy's on an untested shape] → parity test over the corpus; any
  divergence is a test failure, not drift.
- [Concurrent in-memory statements now queue] → in-memory SQLite is a test/demo datasource; correctness
  over overlap. A wait longer than the pool timeout raises SQLAlchemy's timeout error.
- [The structural pool check needs every Tier 1 database's engine to build without connecting] →
  engines are built from URLs only; a type whose build connects is a test failure to resolve, never a
  skip.
