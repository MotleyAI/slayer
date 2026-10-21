## Context

See proposal.md — Why. Today two authorities pick the driver: SLayer's `DriverFacts` (`url_scheme`,
`sync_driver`) for URLs it builds, SQLAlchemy's per-version default for any URL naming no driver.
Every consumer — sync engine (`engine_factory._build_engine`), async engine
(`client._get_async_engine`), engine-cache keys, `_runtime_fingerprint`, the `MissingDriverError`
hint — reads `DatasourceConfig.get_connection_string()`.

Principles applied: sql §3.2 (dialect quirks only in `dialects/`) — the default-driver fact and the
pin rule live on `DriverFacts`; system §3.1/§3.2 — `core.models` reaches it only through the
existing `core.models -> sql.dialects` `#legacy` arrow (no new arrow, baseline unchanged); sql §3.9
and §3.14 unchanged. No arc42 / `.c4` / `index.yaml` edit: the delta attaches to the existing
`sql/execution` spec of node `sql`.

## Goals / Non-Goals

**Goals:** `DriverFacts` is the only authority on a driverless URL's driver; one pin step at the one
URL source; the bug class (SQLAlchemy changing a default) cannot recur for any dialect whose scheme
names a driver.

**Non-Goals:** SQLAlchemy 2.1 compatibility (snowflake-sqlalchemy import, duckdb-engine foreign-key
reflection) and the latest-SQLAlchemy CI leg — DEV-2118, which also lifts the `<2.1` cap; `postgres://`
(an unregistered SQLAlchemy backend, not a default-driver question).

## Decisions

1. **One source for the default driver.** `sync_driver` is a read-only property — the `+driver` part
   of `url_scheme`, `None` when the scheme names none — on both `SqlDialect` and `DriverFacts`.
   `PostgresDialect.url_scheme = "postgresql+psycopg2"`; the `sync_driver` declarations on Postgres
   and MySQL are deleted. ClickHouse (`clickhouse+http`) and SQL Server (`mssql+pyodbc`) gain a
   default driver without any new declaration.
2. **Backends a type serves.** `SqlDialect.url_backend_aliases: frozenset[str] = frozenset()`,
   carried on `DriverFacts`; `MysqlDialect` (inherited by `MariadbDialect`) declares `{"mariadb"}`.
   `DriverFacts.url_backends` is a property: `{url_backend} | url_backend_aliases`, empty when
   `url_scheme` is `None`.
3. **One pin step: `DriverFacts.pin_default_driver(url: str) -> str`.** Pins iff the URL parses
   (`make_url`; `ArgumentError` → pass through), its backend is in `url_backends`, its drivername
   has no `+`, and `sync_driver` is set: `url.set(drivername=f"{backend}+{sync_driver}")` with the
   URL's own backend, rendered `hide_password=False`. Every other input is returned byte-identical
   (no re-render), so explicit drivers, foreign backends, `:memory:` and unparseable strings are
   untouched.
4. **Applied once.** `get_connection_string()` returns
   `dialect.driver_facts(self.type).pin_default_driver(<current result>)`; the current branching
   body moves to a private helper. No consumer changes; stored `connection_string` is never rewritten.
5. **Async derivation.** `_async_connection_string` matches the backend against `url_backends` and
   rebuilds with the URL's own backend (`mariadb+pymysql` → `mariadb+aiomysql`). Its "plain URL" leg
   is removed — the pin means no driverless URL reaches it: driver == `sync_driver` → `async_driver`;
   another sync driver → sync; async-capable driver → kept. Rewriting an explicitly named default
   sync driver (`+psycopg2`, `+pymysql`) to its async counterpart is kept on purpose: after the pin a
   bare URL and an explicit default-sync URL are the same URL, and the brief's "named drivers are
   respected" rule governs which sync driver is chosen, not the sync→async pairing (Codex review,
   rejected finding).
6. **`_is_default_driver`** matches the backend against `url_backends`; its `scheme_driver` branch
   folds into `sync_driver`.
7. **Version cap** `sqlalchemy = ">=2.0,<2.1"`; the lock stays on 2.0.49.

## Risks / Trade-offs

- [The CI env cannot reproduce the 2.1 Postgres failure: pytest-postgresql pulls in psycopg 3, and
  the cap keeps SQLAlchemy at 2.0] → the version-independent resolver test asserts that every pinned
  URL names `sync_driver` explicitly and that `get_dialect().driver == sync_driver`; its `mysql://` /
  `mariadb://` cases fail without the fix on 2.0.49.
- [Structured Postgres URLs change from `postgresql://` to `postgresql+psycopg2://`] → engine-cache
  keys are in-process only; nothing persists the built URL.
