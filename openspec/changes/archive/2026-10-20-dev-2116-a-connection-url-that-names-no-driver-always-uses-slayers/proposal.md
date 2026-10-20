## Why

A connection URL that names no driver (`postgresql://`, `mysql://`, `mariadb://`) is resolved by
SQLAlchemy's own per-version default driver, not the one SLayer's extra installs. SQLAlchemy 2.1
switched bare `postgresql://` to psycopg 3, so every Postgres query on a fresh
`pip install 'motley-slayer[postgres]'` fails with a `MissingDriverError` whose hint points back at
the installed extra (GitHub #500); bare `mysql://` / `mariadb://` already resolve to `mysqldb`,
which `[mysql]` does not install, on 2.0.

## What Changes

- A URL whose drivername names no driver, on a backend the datasource type serves, is pinned to that
  type's declared default driver — the `+driver` part of its URL scheme — before any engine, cache
  key or error hint sees it. Explicitly named drivers and every other URL pass through unchanged.
- Each dialect declares its default driver once, in its URL scheme; Postgres's scheme becomes
  `postgresql+psycopg2`, so structured Postgres datasources also name their driver.
- MySQL-family types serve both the `mysql` and `mariadb` backends: `mariadb://` pins to
  `mariadb+pymysql` and runs asynchronously on `mariadb+aiomysql`.
- `sqlalchemy` is capped at `<2.1`: on 2.1, snowflake-sqlalchemy fails to import and duckdb-engine's
  foreign-key reflection breaks. A follow-up lifts the cap and adds a latest-SQLAlchemy CI leg.
- Docs: a `connection_string` naming no driver uses the driver from SLayer's extra.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `sql/execution`: new requirement pinning driverless URLs to SLayer's default driver; the
  structured-config URL requirement lists Postgres's new scheme; the asynchronous-path requirement
  covers the `mariadb` backend and drops the now-unreachable no-driver case.

## Impact

- Code: `slayer/sql/dialects/base.py` (`DriverFacts`, `SqlDialect` driver facts),
  `slayer/sql/dialects/{postgres,mysql}.py`, `slayer/core/models.py`
  (`DatasourceConfig.get_connection_string`), `slayer/sql/client.py` (`_async_connection_string`),
  `slayer/sql/dialects/drivers.py` (`_is_default_driver`).
- Dependencies: `pyproject.toml` / `poetry.lock` — `sqlalchemy >=2.0,<2.1`.
- Docs: `docs/configuration/datasources.md`.
- Behaviour: structured Postgres URLs and engine-cache keys now carry `+psycopg2`; stored
  `connection_string` values are never rewritten.
