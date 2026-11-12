## Why

`test_postgres_engine_closes_its_connections` flakes on Python 3.14: tests that build a query engine over Postgres and
never release it on its event loop leave an async psycopg connection open, and a later `gc.collect()` in this test
finalizes it under warnings-as-errors. Nothing attributes such a leak to the test that caused it, so it surfaces as an
order-dependent failure in an unrelated test.

## What Changes

- One owning test helper builds a query engine and, on exit, releases it on its own loop (`aclose()`), closes it, and
  invalidates the factory engines of the datasources it registered; `seeded_exec_engine` is rebuilt on it.
- Every test engine lifecycle that leaks or hand-rolls this cleanup moves onto the helper: 4 unit modules that leak,
  the other pytest-postgresql unit modules, 9 Postgres integration modules that leak, and any MySQL / ClickHouse /
  SQL Server / Trino integration module the new law flags in CI.
- A test-harness law fails the test that built an async engine which never received a completed `dispose()` on its
  own event loop before that loop closed — in the unit and the integration suites.
- A narrow marker lets a test that exercises closed-loop abandonment declare it.
- No production code or specified behaviour changes.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — test harness only (`skip_specs: true`). Release semantics are already specified in `database/execution`
("The query engine owns the clients it builds").

## Impact

- `tests/_engine_helpers.py`, `tests/_pg_helpers.py`, `tests/conftest.py`, new `tests/_async_engine_release_law.py`,
  `tests/test_law_resource_ownership.py` (law self-tests), `tests/test_sql_client_event_loops.py` (marker only).
- Unit modules: `test_cast_postgres`, `test_dev2177_postgres`, `test_dev2186_postgres`,
  `test_sql_mode_retype_postgres`, plus the pytest-postgresql modules that hand-roll engine cleanup and
  `tests/_dev2210_fixtures.py`.
- Integration modules: `test_dev1737_date_functions_postgres`, `test_integration_postgres`, `test_dev2046_postgres`,
  `test_dev1756_identifier_length_pg`, `test_dev2101_postgres`, `test_postgres_statement_timeout`,
  `test_dev2172_postgres`, `test_dev1933_postgres_verbatim`, `test_dev2015_postgres`, and whatever the law flags in
  the MySQL / ClickHouse / SQL Server / Trino workflows (all four are path-triggered by `tests/_*.py`).
- `pyproject.toml`: registers the new marker.
