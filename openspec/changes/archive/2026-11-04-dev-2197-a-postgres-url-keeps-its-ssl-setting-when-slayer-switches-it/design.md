## Context

See proposal.md — Why. Today `SlayerSQLClient` builds a factory-cached sync engine for metadata and,
for postgres/mysql, a per-instance async engine for statements; `client._async_connection_string`
moves the default sync driver to the dialect's async driver, and DEV-2163's
`engine_factory._sync_connection_string` moves an async-only driver back for the sync engine. The
two drivers read the URL query string differently, and the async engine is bound to the first loop
that used it (DEV-2199, folded in). Since 1.6.0 every live database access goes through the client
(system §3.17), so the client alone decides the driver. This change stacks on DEV-2163 (PR #543):
its spec requirement "An async-only driver URL still gets a synchronous engine" and its
`type_probe_uses_sync_driver` flag are removed here.

Library facts checked during planning (SQLAlchemy 2.0.49, asyncpg 0.31, psycopg 3.3.3, psycopg2
2.9.11, aiomysql 0.3.2, pymysql 1.1.2):
- SQLAlchemy passes URL query values to asyncpg as strings; asyncpg parses a string `ssl` only as a
  libpq mode name (`ssl=true` fails), and has no `sslrootcert` / `sslcert` / `sslkey` /
  `connect_timeout` keyword; numeric asyncpg keys (`timeout`, `statement_cache_size`) fail as
  strings. libpq rejects asyncpg-only keys (`psycopg2.extensions.parse_dsn`).
- MySQL: SQLAlchemy folds `ssl_*` URL keys into an `ssl` dict for both pymysql and aiomysql; pymysql
  accepts it, aiomysql treats `ssl` as an `SSLContext`, so URL SSL configuration breaks aiomysql.
- asyncpg's cursor description carries no NUMERIC typmod; psycopg v3 and psycopg2 report precision
  and scale.
- psycopg's async mode refuses asyncio's `ProactorEventLoop` (the Windows default).
- `AsyncEngine.dispose(close=False)` only dereferences the pool; it never closes its connections.

## Goals / Non-Goals

**Goals:**
- One driver per client, no URL rewriting beyond pinning the default driver; the bug class "a URL
  SLayer moved to another driver stops connecting" becomes impossible.
- The async engine's lifetime is per event loop, so sync bridges work after async use.

**Non-Goals:**
- Translating query parameters between drivers (superseded by one driver).
- A driver-independent Postgres type probe for asyncpg (decision 11).
- Building an `SSLContext` for aiomysql from `ssl_*` URL keys (aiomysql is no longer a default).
- Trimming the extras (2.0, via the tripwire).

## Decisions

1. Async→sync direction: stack on #543 rather than re-implement it (user). Superseded by 6: #543's
   sync switch is deleted here.
2. SSL value mapping: key rename only, values verbatim — asyncpg rejects `ssl=true`/`false` in a URL
   (user). Superseded by 6.
3. DEV-2199 kept separate (user). Superseded by 9.
4. Guard libpq-only keys on sync→async (user). Superseded by 6.
5. Dropping asyncpg-only keys on async→sync: withdrawn when 6 was raised.
6. One driver per client; async-capable drivers serve metadata via `AsyncConnection.run_sync` over
   the same engine (user). Options: translate query keys per direction (with guards) vs one driver.
   One driver deletes both switch functions and all translation; since 1.6.0 the client is the only
   database door, so a second driver buys nothing.
7. Postgres default driver psycopg v3, over psycopg2 sync-in-thread (user): dual-mode, reads the URL
   in libpq's vocabulary, reports NUMERIC typmod. psycopg2 kept the default path off native async.
8. MySQL default driver pymysql, over aiomysql (user): only pymysql accepts the `ssl_*` URL keys; the
   default path stops building loop-bound aiomysql engines (DEV-1988's warnings).
9. Fold DEV-2199 in, over landing it first (user): metadata now runs on the async engine, so the
   loop-binding bug would hit the default path; engine lifetime is part of this design.
10. Per-loop engine map, over rebuilding one slot on loop change (default): one slot thrashes
    between the outer and helper loops and cannot dispose across loops.
11. asyncpg probe typing: accept the no-scale result, typed DOUBLE per the probe's unknown-scale rule
    (DEV-2163's client-contract requirement), and document it, over a temp-view catalog probe
    (user). Planning first said DECIMAL; corrected to the corpus rule. A temp view runs DDL per probe
    that restricted roles or standbys refuse.
12. Extras keep psycopg2/asyncpg (postgres) and aiomysql (mysql) until 2.0, with a
    `warn_deprecated(..., removal="2.0")` on clients using them, over trimming now (user): no 1.x
    packaging break, and the cleanup is on the tripwire.
13. Windows Proactor loop: the same driver's sync mode in a worker thread, over documenting a loop
    policy (user). The dialect declares the refused loop kinds per driver (sql §3.2).
14. Rename `sync_driver` → `default_driver` (default): "sync" no longer means anything.
15. Missing-driver hint driven by an `extra_drivers` fact, a driver → distribution map (postgres:
    psycopg→psycopg, psycopg2→psycopg2-binary, asyncpg→asyncpg; mysql/mariadb: pymysql→pymysql,
    aiomysql→aiomysql) (default; distribution mapping from codex finding 5). A unit test pins the
    pyproject extras against the distributions.
16. psycopg floor `>=3.1`, installed as `psycopg = {extras = ["binary"]}` (default).
17. arc42 §3.14 extension (user, approved wording): "`SlayerSQLClient` for its private in-memory
    engine (via `close()`, with a finalizer backstop) and its async engines, one per event loop,
    each released only on its own loop." plus the tag
    `[enforced: test:tests/test_sql_client_event_loops.py]`.
18. arc42 §3.15 (user, approved wording, amended after codex finding 1): "15. **One driver per
    client**: a client reaches its database through one driver only — its caller's engine's, else
    its URL's (the one named, else the type's default); no URL is rewritten to another driver.
    [enforced: test:tests/test_one_driver_per_client.py]"
19. Caller-supplied engine (codex finding 1): kept as the client's one driver (the deprecated 1.5.0
    `sa_engine` forms build the datasource from the engine's URL, so validating a driver match would
    break them); §3.15 names it (decision 18) and a test pins that such a client builds no other
    engine.
20. Metadata reads take one open sync `Connection` (codex finding 2): `sa.inspect(connection)` and
    the catalog helpers execute on it without opening or closing connections; the sync path opens it
    from its engine, the async path receives it from `run_sync`. An engine-shaped adapter is not
    inspectable and nested `.connect()` is unavailable on a `Connection`.
21. Closed-loop engines are abandoned best-effort (codex finding 3): `engine.sync_engine.dispose(
    close=False)`, no cross-loop I/O, no unawaited coroutine. Clean release is `aclose()` on the
    engine's own loop, which every sync bridge runs before its helper loop ends; "no
    `ResourceWarning`" is asserted only on clean paths, the fallback is tested separately.
22. A lock guards the per-loop registry (codex finding 4); it is never held across engine
    construction or disposal; a two-thread / two-loop test covers it.
23. Requirement named "A bundled non-default driver warns until 2.0" with one scenario per driver
    (codex finding 6), not "former default".
24. One-driver tests instrument `create_async_engine` and `engine_factory.get_engine` across every
    public execution and metadata method for `+asyncpg`, `+psycopg`, `+aiomysql`, asserting the
    factory is never called (codex finding 7).
25. Query-string assertions (codex finding 8): named URLs reach engine construction unchanged and the
    parsed query keys/values reaching the dialect are unchanged; bare URLs are checked for the pinned
    driver plus an unchanged query — no byte-identity claim at the driver boundary.
26. Requirements whose scenario titles named psycopg2 or "sync driver" are reworked (REMOVED +
    ADDED under adjusted names) rather than MODIFIED, because a MODIFIED delta cannot rename a
    scenario (default).

## Risks / Trade-offs

- [Existing installs upgraded without the extra lack psycopg] → bare Postgres URLs raise the typed
  missing-driver error with `pip install 'motley-slayer[postgres]'`; release notes call it out.
- [Explicit `+psycopg2` / `+pymysql` URLs lose native-async statements] → statements run in worker
  threads like every other sync-in-thread dialect; name `+psycopg` for native async.
- [asyncpg probes lose exact-numeric scale] → documented; values stay exact, only INT refinement is
  lost.
- [A client used under `asyncio.run` repeatedly abandons closed-loop engines] → best-effort
  dereference; `aclose()` before the loop ends is the clean path.
- [psycopg v3 returns some values typed differently from psycopg2 (e.g. `inet` as `ipaddress`)] →
  statements already ran on asyncpg by default, which returns the same Python types; metadata is
  unaffected.

## Migration Plan

Merge after #543 lands on `main`. No stored-data migration: stored `connection_string`s are never
rewritten. Rollback is a revert.
