Gotchas: this worktree's `.venv` is Python 3.14.10 (the flake's interpreter). Locally, pytest-postgresql needs
`"--postgresql-postgres-options=-c unix_socket_directories="` (sandbox: TCP only). Leak-probe recipe used in
planning: `PYTHONTRACEMALLOC=25 poetry run pytest <module> tests/test_dev2210_join_target_model_filters.py::test_postgres_engine_closes_its_connections -p no:randomly -p no:xdist -q`.

## 1. Law self-tests (pr-tests; each must FAIL before the law exists)

- [x] 1.1 In `tests/test_law_resource_ownership.py`, add pytester self-tests of the async-engine release law
      (design Decision 10): a function-scoped async fixture that builds a client's async engine and drops it
      without `aclose()` fails, naming that test; the same through the owner helper passes. Verify: they fail
      now (no law), for the right reason.
- [x] 1.2 Add the remaining Decision 10 cases: module- and session-scoped loops (leak judged when the loop
      closes, naming the building test), a leaking async fixture finalized after the test body, a client
      created inline and dropped, an engine built through `SlayerQueryEngine(client_factory=…)`, a sync test
      using `asyncio.run()`, a bridge-loop engine (passes), abandonment without `abandons_async_engines`
      (fails) and with it (passes). Verify: each fails now for the right reason, or passes where it pins a
      non-violation.
- [x] 1.3 Add `@pytest.mark.abandons_async_engines` to
      `tests/test_sql_client_event_loops.py::test_a_closed_loops_engine_is_dropped` (marker only; user approved).
      Verify: the module still passes.

## 2. Owner helper (pr-implement)

- [ ] 2.1 In `tests/_engine_helpers.py`, add the owner helper (Decision 8): async context manager building a
      `SlayerQueryEngine` over a storage with pass-through engine kwargs; exit = `await asyncio.shield(engine.aclose())`,
      `engine.close()` in a nested `finally`, invalidate the exact saved `DatasourceConfig`s. Rebuild
      `seeded_exec_engine` on it. Verify: modules using `seeded_exec_engine` pass.
- [ ] 2.2 In `tests/_pg_helpers.py`, add the Postgres convenience composing the helper with a datasource from a
      pytest-postgresql client fixture's `info` or from `pg_database`. Verify: used by 3.1 and passes.

## 3. The law (pr-implement)

- [ ] 3.1 Add `tests/_async_engine_release_law.py` (Decisions 3–5, 7): record handed-out async engines (strong ref,
      creating loop, datasource, building node id), completed `dispose()` on the creating loop, and abandonment;
      judge in a `pytest_runtest_teardown` hookwrapper after `yield`; honour `abandons_async_engines`. Install it
      from `tests/conftest.py` for unit and integration suites; register the marker in `pyproject.toml`.
      Verify: all 1.1/1.2 self-tests pass.

## 4. Route engine lifecycles through the helper (pr-implement)

- [ ] 4.1 Unit leakers: `test_cast_postgres`, `test_dev2177_postgres`, `test_dev2186_postgres`,
      `test_sql_mode_retype_postgres`. Verify: each module passes under the law, and the pairwise repro
      (module then the flaky test) passes.
- [ ] 4.2 Other pytest-postgresql unit modules that hand-roll `aclose()`/`close()`/`invalidate_engine`, incl.
      `tests/_dev2210_fixtures.filtered_engine`'s postgres branch. Verify: the 29 pytest-postgresql unit modules
      pass with `-n 6 --dist loadscope -p no:randomly` (the planning repro had exactly one failure: the flaky test).
- [ ] 4.3 Postgres integration modules (Decision 9 scopes): `test_dev1737_date_functions_postgres`,
      `test_integration_postgres`, `test_dev2046_postgres`, `test_dev1756_identifier_length_pg`,
      `test_dev2101_postgres`, `test_postgres_statement_timeout`, `test_dev2172_postgres`,
      `test_dev1933_postgres_verbatim`, `test_dev2015_postgres`, and any other the law flags locally. Verify: the
      CI integration invocation (CLAUDE.md) passes locally.
- [ ] 4.4 MySQL / ClickHouse / SQL Server / Trino integration modules the law flags in their path-gated CI
      workflows. Verify: all four workflows green on the PR.

## 5. Verification

- [ ] 5.1 Full unit suite under xdist on Python 3.14 (`poetry run pytest -m "not integration" -n 8`) passes —
      checks the law has no false positives (the census prototype errored in `test_list_set_equality.py::TestStages`
      under xdist). Then `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the
      baseline), `poetry run la-arch-check`.
