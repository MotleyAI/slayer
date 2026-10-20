## 1. Tests (pr-tests stage; all fail before implementation)

- [x] 1.1 Pin-step tests for every scenario of "A connection URL that names no driver uses SLayer's default driver": bare postgres / mysql / mariadb / clickhouse / mssql URLs pinned (reserved-char credentials + query params preserved); named drivers (`postgresql+psycopg`, `postgresql+asyncpg`, `mysql+mysqldb`), foreign backend, sqlite / duckdb / `:memory:` / snowflake / trino / redshift / unregistered `foo` returned byte-identical; stored `connection_string` unchanged after use; verify they fail on the current code
- [x] 1.2 Version-independent resolver test: for every registered dialect whose scheme names a driver, `make_url(get_connection_string()).get_dialect().driver == sync_driver` for structured fields AND a bare user URL. Gotcha: CI cannot reproduce the 2.1 Postgres failure (pytest-postgresql installs psycopg 3; SQLAlchemy is capped at 2.0), so this test plus 1.1's explicit `+driver` assertions are the guard; verify the `mysql://` / `mariadb://` cases fail now
- [x] 1.3 Driver-facts tests: `sync_driver` derived from `url_scheme` (None for driverless schemes); `url_backends` = `{url_backend} | aliases`, `{"mysql","mariadb"}` for mysql/mariadb, empty for an unregistered type's facts with no scheme; Postgres scheme is `postgresql+psycopg2`
- [x] 1.4 Async tests: bare postgres / mysql URLs → `+asyncpg` / `+aiomysql`; `mariadb://` and `mariadb+pymysql://` → `mariadb+aiomysql://`; missing `psycopg2` on a bare `postgresql://` raises a missing-driver error naming the `[postgres]` extra
- [x] 1.5 Update existing tests that assert bare/old URLs to the pinned form (behaviour change per DEV-2116): `tests/test_models.py:1068`, `tests/test_datasource_snowflake.py:79`, `tests/test_dialect_driver_facts.py:86,99-100` (`test_registry_is_the_one_driver_table` stops setting `sync_driver` in `model_copy`), `tests/test_engine_factory.py:90`, `tests/test_async_driver_selection.py:33`; structured sqlserver scheme `mssql+pyodbc` in the "Known types keep their scheme" test

## 2. Implementation

- [ ] 2.1 `slayer/sql/dialects/base.py`: `sync_driver` becomes a property on `SqlDialect` and `DriverFacts`; add `url_backend_aliases` field + `DriverFacts.url_backends` property; add `DriverFacts.pin_default_driver`; verify 1.3 passes
- [ ] 2.2 `postgres.py`: `url_scheme = "postgresql+psycopg2"`, drop `sync_driver`; `mysql.py`: drop `sync_driver`, declare `url_backend_aliases = {"mariadb"}`; verify 1.3 passes
- [ ] 2.3 `slayer/core/models.py`: `get_connection_string()` pins its result via the private-helper split; verify 1.1, 1.2 pass
- [ ] 2.4 `slayer/sql/client.py` `_async_connection_string`: `url_backends` match, URL's own backend, drop the plain-URL leg; verify 1.4 passes
- [ ] 2.5 `slayer/sql/dialects/drivers.py` `_is_default_driver`: `url_backends` match, fold `scheme_driver` into `sync_driver`; verify the missing-driver suite passes
- [ ] 2.6 `pyproject.toml`: `sqlalchemy = ">=2.0,<2.1"`; `poetry lock` (no upgrades — lock stays on 2.0.49); verify `poetry check --lock`

## 3. Docs and gates

- [ ] 3.1 One sentence in `docs/configuration/datasources.md`: a `connection_string` naming no driver (`postgresql://`, `mysql://`) uses the driver from SLayer's extra; name one explicitly to use another
- [ ] 3.2 Full unit suite (`poetry run pytest -m "not integration" -n logical`), `ruff check slayer/ tests/`, `basedpyright` (no new errors vs baseline), `la-arch-check` all green
