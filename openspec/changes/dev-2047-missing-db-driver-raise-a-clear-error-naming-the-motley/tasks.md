## 1. Tests (pr-tests stage — all fail before implementation unless marked regression)

- [x] 1.1 New `tests/test_missing_driver.py`: sync path per extra-backed type (postgres, mysql, mariadb, clickhouse, sqlserver, snowflake, bigquery) with the default driver / plugin blocked (`sys.modules[name] = None` for a DBAPI; hide the SQLAlchemy plugin entry point for a plugin) → `MissingDriverError` naming datasource, type, missing module or scheme, and the exact `pip install 'motley-slayer[<extra>]'`; covers both `ImportError` and `NoSuchModuleError` kinds; verify each fails on main
- [x] 1.2 Async path: asyncpg blocked (postgres) and aiomysql blocked (mysql) → `execute` raises `MissingDriverError` with the right hint, and the sync driver is never called (no fallback)
- [x] 1.3 Custom driver: `postgresql+pg8000` with `pg8000` blocked → message names `pg8000` and `connection_string`, does not contain `motley-slayer[postgres]`
- [x] 1.4 Tier-2: a redshift datasource with its plugin missing → `MissingDriverError` naming the `redshift` scheme with the generic install hint + datasource-docs pointer
- [x] 1.5 Lazy vendor imports: Snowflake `connection_name` sentinel with `snowflake.connector` blocked (fails at engine build, before the connect-time `creator`), Snowflake inline URL with `snowflake.sqlalchemy` blocked, BigQuery `oauth_credentials_json` with `google.*` blocked → `MissingDriverError` with the type's extra
- [x] 1.6 Error type: `MissingDriverError` is a `SlayerError` and an `ImportError`; an `ImportError` raised inside a dialect `build_engine` hook for a non-driver module propagates unchanged; a REST endpoint (or MCP tool) over a datasource with a missing driver reports the install hint in its error message
- [x] 1.7 `_async_connection_string` matrix: keeps `postgresql+psycopg`, `postgresql+asyncpg`, `mysql+aiomysql`, `mysql+asyncmy`; rewrites `postgresql://`, `postgresql+psycopg2://` → `+asyncpg` and `mysql://`, `mysql+pymysql://` → `+aiomysql` preserving user/password/host/port/database/query; `postgresql+pg8000`, `mysql+mysqldb`, a backend mismatch (`type: postgres` with a `redshift+…` URL) and Snowflake → `None`
- [x] 1.8 Pin SQLAlchemy behaviour the design relies on: `get_dialect(_is_async=True).is_async` per driver in 1.7 (regression guard)
- [x] 1.9 Registry completeness: every registered dialect declares the D1 fields; the six extra-backed types carry exactly their extra; every `async_driver` yields an async-capable URL; no `_ASYNC_DRIVERS` / `driver_map` table remains
- [x] 1.10 `get_connection_string` regression guards (pass on main; must keep passing after D1): unknown structured type `foo` keeps `foo://`, `None` type still raises, postgres/mysql/mariadb/clickhouse/Tier-2 structured URLs byte-identical to main
- [x] 1.11 Docs consistency: every extra-backed type's `motley-slayer[<extra>]` appears in its row of `docs/configuration/datasources.md` (fails today for sqlserver and bigquery)
- [x] 1.12 Re-point existing tests (approved in planning): `tests/test_sql_client_snowflake.py` `_ASYNC_DRIVERS` assertions → `async_driver is None` on the Snowflake dialect; `tests/dialects/test_snowflake.py` install-hint test → `MissingDriverError` with the snowflake extra

## 2. Driver facts as dialect data

- [ ] 2.1 Add `url_scheme`, `sync_driver`, `async_driver`, `install_extra` to `SqlDialect` and set them per D1 in each dialect file; verify 1.9 passes
- [ ] 2.2 `get_connection_string` reads `dialect.url_scheme` only for the dialect's own aliases; delete `driver_map`; verify 1.10 passes

## 3. Typed error and driver pre-flight

- [ ] 3.1 Add `MissingDriverError(SlayerError, ImportError)` to `slayer/core/errors.py`; verify the type half of 1.6
- [ ] 3.2 New `slayer/sql/dialects/drivers.py`: `load_driver` (get_dialect + import_dbapi, D3 hint selection, Snowflake sentinel connector check) and `import_driver`; verify 1.1, 1.3, 1.4
- [ ] 3.3 Call `load_driver` in `engine_factory._build_engine` before the dialect hook / `create_engine` (not for in-memory SQLite); verify 1.1 and the propagation half of 1.6
- [ ] 3.4 Route Snowflake (`build_connection_url`, connect-time `creator`) and BigQuery OAuth lazy imports through `import_driver`; delete Snowflake's hint helpers; verify 1.5 and 1.12

## 4. Async path

- [ ] 4.1 Rewrite `_async_connection_string` per D5 from dialect data; delete `_ASYNC_DRIVERS`; verify 1.7, 1.8
- [ ] 4.2 Run `load_driver(is_async=True)` around the async-URL derivation and before `create_async_engine`; correct the `SlayerSQLClient` docstring; verify 1.2

## 5. Docs

- [ ] 5.1 `docs/configuration/datasources.md`: SQL Server row, BigQuery `motley-slayer[bigquery]` extra, one sentence on custom `connection_string` drivers and the async rule; `docs/database-support.md` one-line pointer; verify 1.11

## 6. Gate

- [ ] 6.1 `poetry run pytest -m "not integration"`, the CI integration invocation from CLAUDE.md, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), and `uvx --no-build --from living-architecture==0.2.1 la-arch-check` all green
