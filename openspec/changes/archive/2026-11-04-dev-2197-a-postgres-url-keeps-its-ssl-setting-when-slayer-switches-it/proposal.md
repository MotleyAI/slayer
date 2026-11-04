## Why

A Postgres URL stops connecting when SLayer moves it between psycopg2 and asyncpg: the two drivers read the query string differently (`sslmode` vs `ssl`, and libpq-only or asyncpg-only keys such as `sslrootcert` or `prepared_statement_cache_size`), and the move copies it verbatim. The root cause is that one SQL client runs two drivers per datasource and rewrites URLs between them by hand; since 1.6.0 every database access goes through that one client, so it needs only one driver. Its async engine is also bound to the first event loop that used it, so a sync bridge after async use on the caller's loop fails with "attached to a different loop".

## What Changes

- A SQL client connects through exactly one driver: the one its URL names, else the type's default; SLayer never rewrites a URL to another driver.
- An async-capable driver serves statements natively and every metadata read (ping, reflection, listings, keys, comments, server profile, type probes) through the same async engine; a sync-only driver serves everything on the factory's sync engine in a worker thread.
- **BREAKING** The default Postgres driver becomes psycopg (v3): a URL naming no driver, and a structured config, connect as `postgresql+psycopg`. MySQL/MariaDB keep pymysql.
- **BREAKING** A URL naming psycopg2 or pymysql no longer runs statements on asyncpg / aiomysql; it runs on its named driver in a worker thread.
- On an event loop the driver's async mode refuses (psycopg: the Windows `ProactorEventLoop`), the client runs the same driver's sync mode in a worker thread.
- The client holds one async engine per event loop; `aclose()` releases only the running loop's engine.
- The `postgres` extra installs psycopg, psycopg2 and asyncpg, the `mysql` extra pymysql and aiomysql; a client on a bundled non-default driver warns that the extra stops installing it in 2.0.
- On asyncpg a probed exact numeric has no reported scale and is typed DOUBLE.

## Capabilities

### New Capabilities

### Modified Capabilities
- `sql/execution`: one driver per client replaces the sync/async driver switching; the Postgres default driver changes; async engines are held per event loop; bundled non-default drivers warn until 2.0; the missing-driver hint covers every driver an extra installs.
- `sql/client-contract`: the column-type probe gains the asyncpg no-scale scenario.
- `compatibility/removal-tripwire`: the covered set gains the bundled non-default driver warning.

## Impact

- Code: `slayer/sql/client.py` (engine selection, per-loop async engines, metadata over one connection), `slayer/sql/engine_factory.py` (the sync-URL switch removed), `slayer/sql/dialects/` (driver facts: default driver, extra drivers, refused loops; the async-driver fields and `type_probe_uses_sync_driver` removed), `slayer/sql/dialects/reflection.py` (catalog reads on an open connection).
- Dependencies: `psycopg[binary]` joins the `postgres` and `all` extras.
- Architecture: `python.sql.arc42.md` §3.14 extended, §3.15 added.
- Docs: `docs/configuration/datasources.md`, `docs/database-support.md`.
