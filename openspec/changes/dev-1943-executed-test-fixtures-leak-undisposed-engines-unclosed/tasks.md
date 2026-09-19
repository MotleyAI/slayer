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

- [ ] 2.1 Add `slayer/storage/sqlite_conn.py` (D1); verify 1.2 green and ruff clean
- [ ] 2.2 Route `sidecar_embedding_store.py` (7), `sqlite_storage.py` (24, incl. the memory
  allocator via `open_connection`), `v4_migration.py` (1); update the sidecar docstring's
  lifecycle sentence; verify the storage suites and the 3.14 probe of
  `tests/test_dev1832_transform_source.py` show zero warnings
- [ ] 2.3 Route every test site (27 with-blocks, ~200 seeders, `tests/integration`, `tests/perf`)
  through `transaction` / `open_connection`; verify the door half of 1.1 is green

## 3. One engine owner

- [ ] 3.1 `engine_factory`: move `_is_in_memory_sqlite`, `_MEMORY_DB_NAME`, the StaticPool builder
  (`build_in_memory_sqlite_engine`, UDF listener via `_attach_register_udfs_listener`); `_build_engine`
  uses it for in-memory URLs; `_cache_key` runtime leg carries the datasource name for in-memory
  URLs; `reset_cache()` always disposes, kwarg removed, docstring says teardown-only; verify 1.3 green
- [ ] 3.2 `client.py`: private in-memory engine via the factory builder + stored `weakref.finalize`
  handle; `close()`; discard-on-auth-failure invokes the finalizer; retire `_sync_engines` /
  `_get_sync_engine` / `_resolve_sync_engine`, `_INLINE_SYNC_DB_TYPES` and its branches; `engine`
  required on `_execute_sql_sync` / `_get_column_types_sync`, dead `connection_string` params dropped
  on them and the retry wrappers; verify 1.4 green and `tests/test_sql_client*.py`,
  `tests/test_dev1933_verbatim_execution.py` green
- [ ] 3.3 `query_engine.py`: `SlayerQueryEngine.close()` (D4); verify 1.4 green and
  `tests/test_mcp_engine_teardown.py`, `tests/test_async_engine_disposal.py` unchanged-green
- [ ] 3.4 Test callers: `tests/test_sql_client_in_memory_async.py` and
  `tests/integration/test_in_memory_sqlite.py` drop the `_get_sync_engine` fallback;
  `tests/integration/test_integration.py` replaces `_sync_engines` with `engine_factory.reset_cache()`;
  `tests/test_storage_type_refinement.py` reset call unchanged; verify those modules green
- [ ] 3.5 `tests/_engine_helpers.py::disposable_engine(url, **kw)`; route every test-side
  `create_engine` (`test_osi_converter`, `test_dev1743_importers`, `test_sql_client`, ingestion,
  dialect tests, `tests/perf`, integration) through it or through the factory; verify the engine
  half of 1.1 green

## 4. Gate

- [ ] 4.1 `pyproject.toml` `filterwarnings` (two entries) and the session-scoped autouse
  teardown fixture in `tests/conftest.py` (`reset_cache()` then `gc.collect()`); verify 1.5 green
  and the 3.14 full unit run green under the gate
- [ ] 4.2 Run the integration suite (CI invocation from CLAUDE.md) on the 3.14 env; verify green
  under the gate (postgres/duckdb-only locally; note any skips)

## 5. One seeded executing-engine context

- [ ] 5.1 `tests/_engine_helpers.py::seeded_exec_engine` (D9); verify 1.6 green. **Signature pinned by
  `tests/test_seeded_exec_engine.py`:** an async context manager
  `seeded_exec_engine(*, dialect, seed, models, datasource="test", validate=False)` yielding
  `(engine, db_path)`; on exit `engine.close()` then `engine_factory.invalidate_engine(<ds>)` in
  `finally`, before the `TemporaryDirectory` is removed. `seed` is `Callable[[str], None]` seeding
  the db file at the given path; `dialect` picks the file extension + `DatasourceConfig.type`.
- [ ] 5.2 Delegate the 12 roots (`_dev1739`, `_dev1740`, `_dev1750`, `_dev1800`, `_dev1832`,
  `_dev1836`, `_dev1838`, `_dev1840`, `_dev1842`, `_dev1846`, `_dev1847`, `_dev1900`) and
  `_law_harness.make_law_engine`; delete their `_engine_for`; verify each root's executed suites
  green on sqlite + duckdb and no byte-level duplicate remains (`grep -c "YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path)" tests/_dev*` = 0)
- [ ] 5.3 Fold the near-variants (`_dev1835` shipped, `_dev1868` daily, `_dev1866.make_chain_exec_engine`
  with `DS_CHAIN`, `_dev1471` spec-driven seeder) via `datasource=` / `validate=` / a seed closure,
  or record why not in this file; verify their suites green

## 6. Harnesses, docs, accounting, gates

- [ ] 6.1 Present the exact edits in design.md "Harness edits" to James and apply only on his OK:
  `architecture/sql.arc42.md` item 14, new `architecture/storage.arc42.md`, `architecture/index.yaml`
  `arc42:` for storage; verify `poetry run python tools/arch_check.py` and
  `npx -y likec4@1.47.0 validate architecture` green
- [ ] 6.2 `docs/getting-started/python.md`: one sentence after the `execute_sync` example — call
  `engine.close()` when done with an engine over an in-memory SQLite datasource; verify 1.7
- [ ] 6.3 Full gates: `poetry run pytest -m "not integration" -n auto` (3.11/3.12 env) and the
  same on the 3.14 env, `ruff check slayer/ tests/`, `basedpyright` (baseline holds or shrinks),
  conventions gate; verify all green, then the Codex working-tree pass before push
