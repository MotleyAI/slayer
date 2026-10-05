## Why

A missing database driver surfaces as a bare `ModuleNotFoundError` (or SQLAlchemy's
`NoSuchModuleError` for a missing dialect plugin) from engine construction, so a user
who installed SLayer without the right extra is never told which extra to install.
The async path also replaces any driver the user named with asyncpg / aiomysql, so a
user who picked psycopg v3 still needs asyncpg; and the client docstring promises a
sync fallback that does not exist.

## What Changes

- Building a sync or async engine with a missing driver or dialect plugin raises a
  typed `MissingDriverError(SlayerError, ImportError)` naming the missing module (or
  plugin), the datasource type, and an install hint: `pip install 'motley-slayer[<extra>]'`
  for the six extras (postgres, mysql, clickhouse, sqlserver, snowflake, bigquery);
  a generic "install the module named here" hint plus a docs pointer for Tier-2 types;
  a "driver named in your connection_string" hint when the user chose a custom driver.
- Per-dialect driver facts (default URL scheme, default sync driver, async driver,
  install hint) become `SqlDialect` data; `client._ASYNC_DRIVERS` and the
  `get_connection_string` driver map are deleted. Structured-config URLs are unchanged.
- The async connection string keeps an async-capable driver the user named
  (`postgresql+psycopg`, `mysql+asyncmy`), rewrites only a plain URL or the dialect's
  default sync driver (`+psycopg2`, `+pymysql`) to the async driver, and leaves any
  other named sync driver (`+pg8000`, `+mysqldb`) on sync-in-thread.
- A missing async driver raises the typed error — no silent sync fallback; the
  `SlayerSQLClient` docstring is corrected.
- Snowflake's and BigQuery's lazy driver imports go through the same typed error.
- Docs: the datasource table gains the SQL Server row and BigQuery's extra, and says
  a custom `connection_string` driver must be installed separately.

## Capabilities

### New Capabilities

_None._

### Modified Capabilities

- `sql/execution`: adds requirements for the typed missing-driver error on sync and
  async engine construction, and for async-driver selection from the connection string.

## Impact

- `slayer/core/errors.py` (new `MissingDriverError`), `slayer/core/models.py`
  (`get_connection_string` reads the dialect scheme).
- `slayer/sql/dialects/base.py` + per-dialect files (driver fields),
  new `slayer/sql/dialects/drivers.py` (driver pre-flight + lazy import helper),
  `slayer/sql/engine_factory.py`, `slayer/sql/client.py`, `snowflake.py`, `bigquery.py`.
- Tests asserting `_ASYNC_DRIVERS` and the Snowflake `ImportError` hint are re-pointed.
- `docs/configuration/datasources.md`, `docs/database-support.md`.
- No dependency or extra changes.
