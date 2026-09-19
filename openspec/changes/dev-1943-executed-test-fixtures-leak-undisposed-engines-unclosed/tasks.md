## 1. Failing suites (spec-tests stage)

Gotcha for every stage: on 3.13+ a leak fails whichever test the collector runs in, not the
leaking test. Localise with
`PYTHONTRACEMALLOC=25 PYTHONWARNINGS=always::ResourceWarning poetry run pytest <module> -p no:warnings -s`
(prints the allocation traceback). A 3.14 env exists at
`poetry env use /home/james/.local/bin/python3.14` (scratch env; do not commit lock changes).

- [x] 1.1 `tests/test_law_resource_ownership.py`: the AST ratchet (D8) over `slayer/**` and
  `tests/**` with the binding-resolving matcher, the zero-tolerance allowlists, the pyproject
  `filterwarnings` pin, and self-checks for every alias form (`import sqlite3 as s`, `from sqlite3
  import connect as c`, `from sqlite3 import dbapi2`, `sqlalchemy.create_engine`,
  `sa.engine.create_engine`, imported `create_engine`, a `create_engine` outside `build_engine` in
  a dialect file); verify it is red on the current tree. Done: red = 312 violations (29 slayer:
  client.py:53/95, storage sqlite sites; 283 test-side); self-checks + gate-config pin green.
  **D8 allowlist widened** to `build_engine` **or** `_build_*_engine` build-hook helpers (bigquery's
  `_build_oauth_engine`) — see design.md D8 spec-tests calibration.
- [x] 1.2 `tests/test_sqlite_conn.py`: `transaction` commits on success, rolls back on error, closes
  in both cases (connection unusable afterwards) and on 3.13+ emits no ResourceWarning;
  `open_connection` never commits, always closes; kwargs pass through
  (`isolation_level=None`/`IMMEDIATE`); verify red (module missing). Done: `ModuleNotFoundError:
  slayer.storage.sqlite_conn` (collection red). 3.14 unclosed-database warning behaviour confirmed
  directly.
- [x] 1.3 `tests/test_engine_factory.py`: re-point `TestResetCacheDisposal` to "reset always
  disposes" (drop the `dispose=` kwarg everywhere), add checked-out-connection-finishes-after-reset,
  in-memory builder (StaticPool, UDFs registered, one db across threads), cross-thread coherence
  through the factory, per-datasource-name isolation, reset closes a multi-thread in-memory engine
  without a logged close error; verify red. Done: 8 red (reset-always-disposes, reset-reason,
  4 builder, coherence, isolation — SingletonThreadPool shows `no such table`); checked-out +
  multithread-clean guards green. **Stays UNSTAGED** (modification of an existing test file).
- [x] 1.4 `tests/test_engine_ownership.py` (new): `SlayerSQLClient.close()` disposes the private
  in-memory engine and never a factory engine, idempotent; finalizer disposes on collection (via a
  class-level dispose spy — version-independent — plus a 3.13+ no-warning guard); reuse yields a
  fresh empty in-memory db; `SlayerQueryEngine.close()` idempotent, continues past a failing client,
  clears clients in `finally`, reusable afterwards; `aclose()` still spares sync engines (green
  invariant guard); verify red. Done: 8 red (`no attribute 'close'` / finalizer / reuse), aclose
  guard green.
- [x] 1.5 Gate self-test (3.13+ only, `skipif`): run a one-file leaking test in a subprocess with the
  repo's `pyproject.toml` and assert the run fails with `unclosed database`; verify red (gate
  absent). Lives in `test_law_resource_ownership.py::test_gate_fails_a_leaking_run`. Skipped on the
  3.12 dev env; the 3.14 `unclosed database` warning behaviour it relies on is confirmed directly.
- [x] 1.6 smoke for `seeded_exec_engine` (`tests/test_seeded_exec_engine.py`): yields a working
  engine + db path on sqlite (+ duckdb via `importorskip`), closes the query engine and invalidates
  the factory engine on exit (`engine_factory` cache no longer holds the datasource), no
  ResourceWarning across the lifecycle on 3.13+; verify red. Done: `ImportError: seeded_exec_engine`
  (collection red). **Signature pinned by this test** — see §5.1.
- [x] 1.7 Docs check (`tests/test_docs_engine_close.py`): asserts `docs/getting-started/python.md`
  documents `engine.close()` tied to in-memory. Done: red (sentence absent).

## 2. One sqlite door

- [x] 2.1 Add `slayer/storage/sqlite_conn.py` (D1); verify 1.2 green and ruff clean
- [x] 2.2 Route `sidecar_embedding_store.py` (7), `sqlite_storage.py` (memory allocator via
  `open_connection`, the rest via `transaction`), `v4_migration.py` (1); sidecar docstring's
  lifecycle sentence updated; storage suites green
- [x] 2.3 Route every test site (with-blocks, seeders, `tests/integration`, `tests/perf`)
  through `transaction` / `open_connection`; ratchet sqlite half green (0 violations)

## 3. One engine owner

- [x] 3.1 `engine_factory`: `_is_in_memory_sqlite`, `_MEMORY_DB_NAME`, the StaticPool builder
  (`build_in_memory_sqlite_engine`, UDFs via `_attach_register_udfs_for_dialect`); `_build_engine`
  routes in-memory URLs to it; `_runtime_fingerprint` carries the datasource name for in-memory
  URLs; `reset_cache()` always disposes, kwarg removed, docstring says teardown-only; 1.3 green
- [x] 3.2 `client.py`: private in-memory engine via the factory builder + stored `weakref.finalize`
  handle; `close()`; discard-on-auth-failure disposes the private engine / evicts the factory one;
  retired `_sync_engines` / `_get_sync_engine` / `_resolve_sync_engine`, `_INLINE_SYNC_DB_TYPES` and
  its branches; `engine` required on `_execute_sql_sync` / `_get_column_types_sync`, dead
  `connection_string` dropped on them + the retry wrappers; 1.4 green and `tests/test_sql_client*.py`,
  `tests/test_dev1933_verbatim_execution.py` green
- [x] 3.3 `query_engine.py`: `SlayerQueryEngine.close()` (D4); 1.4 green and
  `tests/test_mcp_engine_teardown.py`, `tests/test_async_engine_disposal.py` unchanged-green
- [x] 3.4 Test callers: `test_sql_client_in_memory_async.py`, `integration/test_in_memory_sqlite.py`,
  `test_sql_client_snowflake.py` dropped the retired-symbol fallbacks; `integration/test_integration.py`
  replaced `_sync_engines` with `engine_factory.reset_cache()`; `test_sql_client_in_memory.py` re-points
  `_is_in_memory_sqlite` to `engine_factory`; `test_sql_generator.py::TestGetColumnTypesSql` injects
  under `_sql_client_cache_key(mock_ds)` (D2 name-keying); modules green
- [x] 3.5 `tests/_engine_helpers.py::disposable_engine(url, **kw)`; every test-side `create_engine`
  routed through it; ratchet engine half green (0 violations)

## 4. Gate

- [x] 4.1 `pyproject.toml` `filterwarnings` (two entries) and the session-scoped autouse
  teardown fixture in `tests/conftest.py` (`reset_cache()` then `gc.collect()`); 1.5 is skipif-3.13+
  (dev env is 3.12, so the gate is inert there and the self-test skips) — the config is pinned green
- [ ] 4.2 Run the integration suite (CI invocation from CLAUDE.md) on the 3.14 env; verify green
  under the gate (postgres/duckdb-only locally; note any skips) — DEFERRED to spec-review/CI: no
  local 3.14 poetry env on this session; 3.12 non-integration full suite is green

## 5. One seeded executing-engine context

- [x] 5.1 `tests/_engine_helpers.py::seeded_exec_engine` (D9); 1.6 green. **Signature pinned by
  `tests/test_seeded_exec_engine.py`:** an async context manager
  `seeded_exec_engine(*, dialect, seed, models, datasource="test", validate=False)` yielding
  `(engine, db_path)`; on exit `engine.close()` then `engine_factory.invalidate_engine(<ds>)` in
  `finally`, before the `TemporaryDirectory` is removed. `seed` is `Callable[[str], None]` seeding
  the db file at the given path; `dialect` picks the file extension + `DatasourceConfig.type`.
- [x] 5.2 Delegated all 12 roots + `_law_harness.make_law_engine` onto `seeded_exec_engine`; deleted
  every `_engine_for`; extracted the one shared `build_exec_engine(db_path, *, dialect, models,
  datasource, validate)` into `_engine_helpers.py` (`seeded_exec_engine` delegates to it) and
  re-pointed the four external `_engine_for` consumers (`test_dev1891`, `test_dev1841`, `test_dev1892`,
  `test_dev1858`) to it. Metric `grep "YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path)"
  tests/_dev*` = 0; suite green.
- [x] 5.3 Folded `_dev1835.make_shipped_exec_engine`, `_dev1868.make_daily_exec_engine`,
  `_dev1866.make_chain_exec_engine` (`datasource=DS_CHAIN, validate=True`). `_dev1471.make_engine`
  left with a note — caller-driven (`base_dir`/`db_path`/dynamic tables), doesn't fit the
  tempdir-owning context; its sqlite seeding already routes through the door.

## 6. Harnesses, docs, accounting, gates

- [x] 6.1 Applied on James's per-change OK (2026-09-19): `architecture/sql.arc42.md` item 14,
  new `architecture/storage.arc42.md`, `architecture/index.yaml` `arc42:` for storage. No `.c4`
  change needed (storage node already in `views.c4`; new arc42 follows the `ir.arc42.md` no-view
  precedent). `arch_check: OK`; `likec4 validate architecture` exit 0.
- [x] 6.2 `docs/getting-started/python.md`: one sentence after the `execute_sync` example — call
  `engine.close()` when done with an engine over an in-memory SQLite datasource; 1.7 green
- [x] 6.3 Full gates (3.12 env): `pytest -m "not integration" -n auto` green (19039 passed);
  `ruff check slayer/ tests/` clean; `basedpyright` 0 errors (baseline shrank 5684→5681);
  conventions gate CLEAR (0 violations; compare.py sys.path imports waived with James's OK);
  Codex working-tree pass done — 1 finding folded (client `close()` async-engine handling), residue
  accepted by-design (option A). 3.14 full-env run deferred to CI (no local 3.14 env this session;
  the gate is inert on 3.12).
