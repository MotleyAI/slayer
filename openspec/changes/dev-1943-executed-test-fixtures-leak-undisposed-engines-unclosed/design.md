## Context

Measured on CPython 3.14 (full unit suite, one run; census plugin wrapping `sqlite3.connect`
and `sqlite3.dbapi2.connect` — SQLAlchemy's pysqlite dialect binds the latter):

- 11,311 `unclosed database` warnings; 0 on 3.12 (the sqlite3 finalizer warning is new in 3.13).
- 11,007 from raw `with sqlite3.connect(p) as conn:` blocks — the sqlite3 context manager
  commits/rolls back but never closes: `sidecar_embedding_store.py` (7 sites; `_init_db` alone
  9,293 — one per `YAMLStorage()`), `sqlite_storage.py` (24), `v4_migration.py` (1), 27 identical
  test sites, ~200 `con = sqlite3.connect(); …; con.close()` test seeders (close only on success).
- ~180 from SQLAlchemy engines never disposed: `engine_factory.reset_cache()` defaults
  `dispose=False` and is the only exit from the LRU cache that does not dispose (eviction and
  `invalidate_engine` do) — `tests/test_engine_factory.py` calls it ~47×, wiping each xdist
  worker's cache undisposed (pool attribution: 17/17 executed-test file-engine leaks); `:memory:`
  through the factory gets SQLAlchemy's `SingletonThreadPool` (a connection per thread,
  `check_same_thread=True`) so closing from another thread raises `ProgrammingError` and each
  `asyncio.to_thread` worker sees a different empty database; the client's private in-memory
  `StaticPool` engine has no disposing owner; `client._sync_engines` is a second, unbounded,
  never-disposed engine cache reached only by direct test callers (`_INLINE_SYNC_DB_TYPES` is
  empty); test-side `sa.create_engine` fixtures never dispose.
- Warnings surface 1–N tests after the leaking test (finalization drift).
- The 12 `make_exec_engine` roots + `_law_harness` build byte-equivalent engines; their factory
  engines are disposed on eviction — they were never the leak.

Constraints: `SlayerQueryEngine.aclose()` is a cross-repo per-call contract (MCP
`_slayer_engine.aclose()`, `execute_sync`) that must keep sparing synchronous engines
(`tests/test_mcp_engine_teardown.py`, `tests/test_async_engine_disposal.py`);
`tests/test_sql_client_in_memory_async.py::test_two_clients_get_isolated_in_memory_dbs` pins
per-client isolation of in-memory engines; principle 7 (async-first, sync-in-thread) holds.

## Goals / Non-Goals

**Goals:** zero `unclosed database` warnings on 3.13+ across the unit and integration suites;
the leak classes made structurally impossible (one door, one owner) and gated in CI; the 13
duplicated executing-engine builders collapsed into one context.

**Non-Goals:** DuckDB connection hygiene (no finalizer warning; out of the door's scope);
concurrency safety of one in-memory SQLite connection shared across threads (SQLAlchemy's
documented `StaticPool` + `check_same_thread=False` pattern, already the client's policy; SLayer
issues statements sequentially per operation — `asyncio.gather` only spans datasources in
`validate_models`); unifying the client's private in-memory engine with the factory's (D3);
`SingletonThreadPool` for anything.

## Decisions

**D1 — The door has two shapes, both close.** `transaction()` wraps `with conn:` (commit /
rollback) inside a `closing`; `open_connection()` only closes, for the memory allocator that
manages `BEGIN IMMEDIATE` itself under `isolation_level=None`. Alternative rejected: one shape with
an `autocommit` flag — two call sites need different transaction control, and a flag invites the
wrong default.

**D2 — In-memory SQLite in the factory: StaticPool + `check_same_thread=False`, keyed per
datasource name.** The builder moves from `client.py` to `engine_factory.build_in_memory_sqlite_engine`
(with `_is_in_memory_sqlite` / `_MEMORY_DB_NAME`) so client and factory share one policy;
`_cache_key` adds the datasource name to its runtime leg for in-memory URLs so two `:memory:`
datasources never share one factory database (Codex finding, folded). Alternatives rejected:
`SingletonThreadPool` (per-thread empty DBs, cross-thread close failures); shared-cache memory
URIs (`file:x?mode=memory&cache=shared`) — deprecated by SQLite and would silently rewrite the
user's URL.

**D3 — The client keeps its private in-memory engine (option A).** Option B — one factory-owned
in-memory engine per datasource, no private engines — would fix the pre-existing
"ingest via factory ≠ query via client" split but breaks the pinned per-client isolation, and
LRU eviction would silently destroy an in-memory database. So: the client owns and disposes its
private engine; the factory-path split for `:memory:` datasources stays a documented limitation
(`:memory:` is only meaningful when seeded through the same owner).

**D4 — Ownership is explicit `close()` with a finalizer backstop.** `SlayerSQLClient` stores the
`weakref.finalize` handle for its private engine, invokes/detaches it on every discard (auth
failure, `close()`) and registers a fresh one per rebuild (Codex finding, folded — a finalizer
bound to the first engine would let a rebuilt one leak). `SlayerQueryEngine.close()` closes each
client, continues past a failing one, clears `_sql_clients` in `finally`, is idempotent and
leaves the engine reusable. `aclose()` is unchanged.

**D5 — `reset_cache()` always disposes; the kwarg goes.** No production caller exists; a reset
that leaves live pools behind is the leak. SQLAlchemy dispose closes checked-in connections; a
checked-out one keeps working, and the caller `invalidate()`s it on return so its DBAPI connection
closes rather than re-pooling into the orphaned old pool (closed only on GC — a 3.13+ leak) —
pinned by a test.

**D6 — Retire the client module's engine cache.** `engine` becomes a required keyword on
`_execute_sql_sync` / `_get_column_types_sync`; their unused `connection_string` parameters (and
those of the retry wrappers) go; `_INLINE_SYNC_DB_TYPES` and its two dead branches go. The
factory is the one sync-engine cache.

**D7 — The gate is config plus an active-hook teardown.** `filterwarnings` errors for
`unclosed database` (ResourceWarning) and — scoped to sqlite's finalizer message
`Exception ignored while finalizing database connection` — `PytestUnraisableExceptionWarning`; a
leaked sqlite connection surfaces as either, depending on when it is finalized. The unraisable
filter is message-scoped so a non-sqlite driver's connection finalizer (e.g. aiomysql's
`Exception ignored in: <function Connection.__del__>` after the loop closes, seen in the MySQL
integration suite on 3.11) stays a warning — DEV-1943's scope is sqlite. A session-scoped autouse
fixture in `tests/conftest.py` runs
`engine_factory.reset_cache()` then `gc.collect()` in its finalizer — inside the last test's
teardown, while pytest's warning filters and unraisable hook are still installed in every xdist
worker (Codex finding, folded: a `pytest_sessionfinish` hook may run after the hooks are gone).
A self-test runs a deliberately leaking test in a subprocess with the repo's pyproject and
asserts the run fails on 3.13+ (skipped below).

**D8 — The ratchet resolves import bindings.** Per file, build the binding table
(`import sqlite3 as s`, `from sqlite3 import connect as c`, `from sqlite3 import dbapi2`,
`import sqlalchemy as sa`, `from sqlalchemy import create_engine`, `sa.engine.create_engine`…) and
canonicalise every call target before matching (Codex finding, folded — `tests/integration/test_mcp_inspect.py`
already uses `import sqlite3 as _sqlite3`). Allowed: `sqlite3.connect` only in
`slayer/storage/sqlite_conn.py`; `create_engine` only in `slayer/sql/engine_factory.py`, lexically
inside a dialect engine-build hook under `slayer/sql/dialects/` — a function named `build_engine`
**or** a `_build_*_engine` helper it delegates to — and in `tests/_engine_helpers.py`;
`create_async_engine` only inside `slayer/sql/client.py::_get_async_engine`. Self-checks cover
every alias form. Baseline: none — zero tolerance, like `ALLOWED_EXPRESSIVENESS`.

_spec-tests calibration (2026-09-19):_ the pure `build_engine`-only allowlist could never go green
while `bigquery.py`'s `_build_oauth_engine` helper exists, so the dialect allowance is the build
hook — `build_engine` and the `_build_*_engine` helpers it factors its auth paths into. This still
honours the arc42 principle (engines are built only in the factory and the dialects' build hooks)
and needs no churn to the bigquery dialect. Pinned by
`tests/test_law_resource_ownership.py::TestMatcherSelfChecks::test_dialect_create_engine_allowed_only_in_build_hooks`.

**D9 — The seeded context tears down completely.** `seeded_exec_engine` calls
`SlayerQueryEngine.close()` and then `engine_factory.invalidate_engine(datasource)` in `finally`,
before the `TemporaryDirectory` exits, so no engine outlives its file (Codex finding, folded;
also removes DuckDB lock and Windows-unlink hazards). The law harness copies its oracle right
after entering the context — engine connections are lazy.

**D10 — arc42 placement.** L2 is a `sql`-node principle (item 14, "One engine owner"); L1 is a
`storage`-node principle, which needs the node's first arc42 file; L3 is the enforcement tag of
both, not a principle. No `.c4` change: `sqlite_conn` attributes to `storage`, its importers are
storage modules, the builder move is sql-internal, tests are not modelled. **Exact edits below
need James's per-change OK before any of the three files is touched.**

### Harness edits (pending approval)

`architecture/sql.arc42.md`, after item 13:

```
14. **One engine owner**: every SQLAlchemy engine is disposed by exactly one
    owner — `engine_factory` for the engines it caches (eviction and
    `reset_cache` always dispose; `:memory:` is built by its single StaticPool
    builder, keyed per datasource), `SlayerSQLClient` for its private in-memory
    engine (via `close()`, with a finalizer backstop). `create_engine` runs only
    in the factory and the dialects' build hooks; `create_async_engine` only in
    the client's async builder. [enforced: test:tests/test_law_resource_ownership.py]
```

New `architecture/storage.arc42.md`:

```
# storage — persistence backends

## 1. Purpose & context

`slayer/storage` persists models, datasources, memories and embeddings behind
the `StorageBackend` ABC (`base.py`): a YAML tree (`yaml_storage.py`) or one
SQLite file (`sqlite_storage.py`), with the embedding sidecar
(`sidecar_embedding_store.py`) shared by both, the dict→dict migration registry
(`migrations.py`, `v2_…v9_migration.py`, `legacy_alias_rewrite.py`) applied on
load, crash-safe writes (`atomic_write.py`), and live-schema type refinement
(`type_refinement.py`) through the `sql` engine factory.

## 2. Building blocks

Child `migrations` (the registry); the other modules are leaves of the node.

## 3. Principles

1. **One sqlite door**: raw `sqlite3` connections are opened only through
   `sqlite_conn` (`transaction` / `open_connection`), which always closes them;
   a `with sqlite3.connect(...)` block anywhere else is a violation.
   [enforced: test:tests/test_law_resource_ownership.py]

## 4. Rationale

The sqlite3 context manager commits or rolls back but never closes, so a
per-call `with sqlite3.connect(...)` leaks one connection per call until garbage
collection — silent before CPython 3.13, a ResourceWarning after. One door makes
the class impossible instead of re-auditing every site.
```

`architecture/index.yaml`, storage stanza gains `arc42: architecture/storage.arc42.md`.

## Codex review (plan) — resolutions

| Finding | Resolution |
|---|---|
| Finalizer bound to the first private engine leaks a rebuilt one | Folded (D4) + test build → auth failure → rebuild → close |
| StaticPool shares one connection across threads (transaction interleaving) | Rejected: SQLAlchemy's documented in-memory multi-thread pattern; existing client policy; statements are sequential per operation (non-goal) |
| `:memory:` cache key ignores datasource name | Folded (D2) + isolation test |
| Two engines per in-memory datasource (client vs factory) | Rejected as scope (D3): pinned per-client isolation; documented limitation |
| `seeded_exec_engine` leaves the factory file engine cached past tempdir cleanup | Folded (D9) |
| Gate: shutdown-time warnings escape; force GC under active hooks; self-test | Folded (D7) |
| Ratchet misses aliases | Folded (D8) |
| Dialect allowlist too broad | Folded (D8: lexically inside `build_engine`) |
| `reset_cache` API change; checked-out connections | Folded (D5 docstring + test) |
| `close()` robustness | Folded (D4) |

## Codex review (working tree) — resolutions

| Finding | Resolution |
|---|---|
| `SlayerQueryEngine.close()` → `client.close()` disposed the private sync engine but not the client's async engine, then cleared `_sql_clients`, orphaning an async pool if a client had used an async datasource | Partly folded; residue accepted by-design (James, 2026-09-19, option A). An async engine is event-loop-bound and cannot be disposed synchronously — `sync_engine.dispose()` raises `MissingGreenlet` for a real asyncpg/aiomysql pool (a first attempt swallowed that and orphaned the pool anyway). `SlayerSQLClient.close()` therefore does NOT fake-dispose it: it disposes the private sync engine and, if a loop-bound async engine is still live, **warns** (test `test_close_warns_and_retains_a_loop_bound_async_engine`). `close()` is the synchronous teardown (sync / in-memory datasources, where no async engine is ever created); **`aclose()` — unchanged — is the async teardown**: it disposes the async pools on their loop and keeps the clients. The remaining orphan only occurs under the misuse "async `execute()` then synchronous `close()` instead of `aclose()`"; making `SlayerQueryEngine.close()` retain async-bearing clients would contradict the approved spec (`clears _sql_clients`, pinned by `test_close_closes_every_client_and_clears`), so it is accepted as a documented boundary rather than fixed here (out of DEV-1943's sqlite-leak scope). |

## Risks / Trade-offs

- [Gate failures land in a later test] → tasks.md and the ratchet docstring carry the recipe:
  rerun with `PYTHONTRACEMALLOC=25 -W always::ResourceWarning -p no:warnings -s` to print the
  allocation site.
- [`reset_cache` now disposes during tests that reset mid-way] → engines are rebuilt on the next
  `get_engine`; cost is milliseconds per test.
- [Factory `:memory:` engines are now one DB across threads] → probe: full suite green; it is
  the behaviour the client already had.
- [~230 mechanical test edits] → sed-driven, ratchet-verified; each file's tests re-run.
