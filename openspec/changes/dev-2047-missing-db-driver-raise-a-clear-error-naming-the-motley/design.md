## Context

See proposal.md — Why. Constraints from `architecture/sql.arc42.md` §3: dialect quirks live only
in `dialects/` (§2) — today `client._ASYNC_DRIVERS` and `DatasourceConfig.get_connection_string`'s
`driver_map` are per-dialect tables outside it; fail closed with a typed error (§9); `create_engine`
runs only in `engine_factory` and the dialects' build hooks, `create_async_engine` only in the
client's async builder (§14) — this change adds no new construction site.

Probe facts (SQLAlchemy 2.0.49): a missing third-party dialect plugin (clickhouse-sqlalchemy,
snowflake-sqlalchemy, sqlalchemy-bigquery, every Tier-2 plugin) raises
`sqlalchemy.exc.NoSuchModuleError`, not `ImportError`; a missing DBAPI for a built-in dialect
raises `ModuleNotFoundError` from `create_engine` / `create_async_engine`.
`make_url(u).get_dialect(_is_async=True).is_async` is True for psycopg / asyncpg / aiomysql /
asyncmy and False for psycopg2 / pg8000 / pymysql / mysqldb. `create_async_engine` on a sync
driver raises `InvalidRequestError`.

## Goals / Non-Goals

**Goals:** one place per driver fact; every missing-driver failure on any engine path becomes the
typed error; no import failure that is not the driver's is relabelled.

**Non-Goals:** changing any structured-config default scheme (Tier-2 `spark://` / `athena://` /
`oracle://` defaults stay as they are — Tier-2 by design); per-type exact pip lines for Tier-2;
adding or changing extras; an async path for types without a native async driver.

## Decisions

**D1 — Driver facts are `SqlDialect` data; the registry is the one table.** New frozen fields:
`url_scheme: str | None` (structured-config scheme: postgres `postgresql`, mysql/mariadb
`mysql+pymysql`, clickhouse `clickhouse+http`, tsql `mssql+pyodbc`, snowflake `snowflake`, bigquery
`bigquery`, sqlite/duckdb their names; `None` = the datasource type itself — every Tier-2 dialect),
`sync_driver: str | None` (postgres `psycopg2`, mysql/mariadb `pymysql`), `async_driver: str | None`
(postgres `asyncpg`, mysql/mariadb `aiomysql`, else `None` = sync-in-thread), `install_extra:
str | None` (postgres, mysql, clickhouse, sqlserver, snowflake, bigquery; `None` elsewhere).
`client._ASYNC_DRIVERS` and `driver_map` are deleted. `get_connection_string` uses
`dialect.url_scheme` only when the type is one of that dialect's `ds_type_aliases`; otherwise the
type itself — so an unknown type (which `dialect_for_ds_type` maps to Postgres) keeps `<type>://`,
and `None` still raises. The sqlite/duckdb/tsql branches of `get_connection_string` are untouched.
Alternative rejected: a new `_DRIVER_EXTRAS` dict beside the registry — a third hand-synced table.

**D2 — `MissingDriverError(SlayerError, ImportError)`** in `slayer/core/errors.py`, carrying
`datasource_name`, `ds_type`, `missing` (module name, or the scheme whose plugin failed) and the
hint. Being a `SlayerError` (a `ValueError`) means every existing REST / MCP / CLI handler already
reports its message (REST answers 400); being an `ImportError` keeps `except ImportError` callers
working. Alternative rejected: an `ImportError`-only type with new per-surface mappings — any
surface that misses the mapping shows a generic 500 without the hint.

**D3 — Hint selection by what the URL selects, not by module name.** For a `NoSuchModuleError`
whose backend is the dialect's own backend, or an `ImportError` while loading a driver whose name is
the dialect's `sync_driver` / `async_driver` or the driver of its `url_scheme`: the default driver
is missing → `pip install 'motley-slayer[<install_extra>]'`, or for a type without an extra the
generic "install the module/plugin named above" hint plus a link to the datasource docs'
Additional-support table. Any other driver → "install the driver named in your connection_string".
A missing transitive module inside a default driver therefore still gets the extra.

**D4 — Narrow, attributable translation: one driver pre-flight.** New
`slayer/sql/dialects/drivers.py` with `load_driver(datasource, url, *, is_async)`: calls
`make_url(url).get_dialect(_is_async=is_async)` (missing plugin → `NoSuchModuleError`) then
`.import_dbapi()` (missing driver → any `ImportError`, `.name` possibly unset) and translates only
failures of those two calls. It runs in `engine_factory._build_engine` before `build_engine` /
`create_engine` (skipped for in-memory SQLite) and in the client before `create_async_engine` and
around the async-URL derivation (which itself loads the dialect). For the Snowflake
`connection_name` sentinel the pre-flight also imports `snowflake.connector`, so the connect-time
`creator` cannot be the first to fail. Hooks that import vendor libraries themselves (Snowflake's
`snowflake.sqlalchemy.URL` in `build_connection_url`; BigQuery OAuth's `google.cloud.bigquery` /
`google.oauth2.credentials`) use `import_driver(module, *, datasource)` from the same module;
Snowflake's two hand-written `ImportError` hint helpers are deleted. Alternative rejected: a
context manager catching every non-`slayer.*` `ModuleNotFoundError` around construction — it
misses plain `ImportError`, misses the async-URL dialect load, and relabels genuine hook bugs.

**D5 — Async URL derivation.** `_async_connection_string`: `async_driver is None` → `None`;
URL backend ≠ the dialect's backend → `None`; the URL's dialect is async-capable (`is_async`) →
the URL unchanged; the URL is plain or its driver is `sync_driver` → the driver swapped to
`async_driver` via `URL.set(drivername=…)` (every other part preserved); any other named sync driver
→ `None` (sync-in-thread with the user's driver). A missing async driver raises through D4 — no
fallback; the `SlayerSQLClient` docstring is corrected.

**D6 — Docs.** `docs/configuration/datasources.md`: add the SQL Server row and BigQuery's
`motley-slayer[bigquery]` extra; one sentence that a custom `connection_string` driver (e.g.
`postgresql+psycopg`) must be installed separately and is kept on the async path when it can run
async. Tier-2 rows unchanged. `docs/database-support.md`: one-line pointer to that table.

## Risks / Trade-offs

- [`get_dialect(_is_async=…)` is an underscore keyword] → a unit test pins its behaviour per
  driver, so a SQLAlchemy rename fails loudly.
- [`import_dbapi()` imports the driver one step earlier than `create_engine` would] → it is the
  same import `create_engine` performs immediately after; no new side effect.
- [Tier-2 hints are generic] → accepted: Tier-2 has no extras and version-sensitive defaults; the
  message still names the exact missing module or plugin.
- [REST answers 400 for a server-install problem] → accepted for uniform hint delivery (D2).
