## 1. Tests (pr-tests stage)

- [x] 1.1 Align `tests/test_engine_factory.py::TestResetCacheDisposal::test_checked_out_connection_finishes_after_reset` as design decision 3 states; verify it fails before the fix (the returned connection is still open)
- [x] 1.2 Add one SQLite test per new scenario of the `database/execution` delta (late return after reset, invalidation, eviction, private in-memory engine close, blocking call abandoned by cancellation, engine used after disposal); verify each fails before the fix
- [x] 1.3 Add the Postgres late-return scenario on the local `pytest-postgresql` server (unit test; `postgresql_proc(port=None, postgres_options="-c unix_socket_directories=")`), counting sessions in `pg_stat_activity` by `application_name`; verify it fails before the fix
- [x] 1.4 Add tests for the quiet error boundary: a listener-registration failure and a `dispose()` failure are each logged and not raised
- [x] 1.5 Add harness self-tests to `tests/test_law_resource_ownership.py`, run in scratch pytest projects in subprocesses like the async-engine law: a leak through each door is reported at the following teardown and names the opening test; a leak finalized during a later test names the earlier opener; a connection opened on another thread is attributed; a closed connection and a disposed engine pass; a leak dying after the last teardown fails the session; an integration-opened leak dying in a unit test is a warning; a unit-opened leak dying in an integration test fails; install is idempotent and restore is exact; on 3.13+ the built-in warning text carries the opener. Verify they fail before the plugin exists

## 2. Production

- [ ] 2.1 Add the one sync-engine disposal routine to `slayer/database/engine_factory.py` (engine-level `checkin` listener that invalidates the returned record, then `dispose()`, both inside the log-and-continue boundary, registration idempotent per engine) and route `_dispose_quietly` through it; verify tasks 1.1–1.4 pass for the factory scenarios
- [ ] 2.2 Route the client's private in-memory engine disposal (`close()` and the finalizer backstop) through the factory routine; verify the private-engine scenario passes and `tests/test_law_resource_ownership.py::test_resource_ownership_law_holds` stays green

## 3. Harness

- [ ] 3.1 Add `tests/_sqlite_connection_law.py` as designed and list it in `tests/conftest.py` `pytest_plugins`; verify the self-tests of task 1.5 pass
- [ ] 3.2 Run the local test scope; if the harness flags an existing test that leaks in-process on purpose, stop and ask before changing that test

## 4. Gates

- [ ] 4.1 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors against the baseline), `poetry run la-arch-check` and LikeC4 model validation all pass
- [ ] 4.2 `openspec validate dev-2497-a-unit-test-leaks-an-unclosed-sqlite3 --strict` passes
