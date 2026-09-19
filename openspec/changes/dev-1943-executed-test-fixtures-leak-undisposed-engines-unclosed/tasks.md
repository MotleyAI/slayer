## 1. Failing suites (spec-tests stage)

Gotcha for every stage: on 3.13+ a leak fails whichever test the collector runs in, not the
leaking test. Localise with
`PYTHONTRACEMALLOC=25 PYTHONWARNINGS=always::ResourceWarning poetry run pytest <module> -p no:warnings -s`
(prints the allocation traceback). A 3.14 env exists at
`poetry env use /home/james/.local/bin/python3.14` (scratch env; do not commit lock changes).

- [ ] 1.1 `tests/test_law_resource_ownership.py`: the AST ratchet (D8) over `slayer/**` and
  `tests/**` with the binding-resolving matcher, the zero-tolerance allowlists, the pyproject
  `filterwarnings` pin, and self-checks for every alias form (`import sqlite3 as s`, `from sqlite3
  import connect as c`, `from sqlite3 import dbapi2`, `sqlalchemy.create_engine`,
  `sa.engine.create_engine`, imported `create_engine`, a `create_engine` outside `build_engine` in
  a dialect file); verify it is red on the current tree (261 door violations, the test-side engines)
- [ ] 1.2 `tests/test_sqlite_conn.py`: `transaction` commits on success, rolls back on error, closes
  in both cases (connection unusable afterwards) and on 3.13+ emits no ResourceWarning under
  `simplefilter("error")`; `open_connection` never commits, always closes; kwargs pass through
  (`isolation_level=None`, `timeout`); verify red (module missing)
- [ ] 1.3 `tests/test_engine_factory.py`: re-point `TestResetCacheDisposal` to "reset always
  disposes" (drop the `dispose=` kwarg everywhere), add checked-out-connection-finishes-after-reset,
  in-memory builder (StaticPool, `check_same_thread=False`, UDFs registered), cross-thread coherence
  through the factory, per-datasource-name isolation, reset closes a multi-thread in-memory engine
  without a logged close error; verify red on the current tree
- [ ] 1.4 `tests/test_engine_ownership.py` (new): `SlayerSQLClient.close()` disposes the private
  in-memory engine and never a factory engine; build → auth failure → rebuild → close disposes both
  exactly once; finalizer disposes on collection (3.13+: no warning under `error` filter);
  `SlayerQueryEngine.close()` idempotent, continues past a failing client, clears clients in
  `finally`, reusable afterwards (in-memory DB starts empty); `aclose()` still spares sync engines;
  verify red
- [ ] 1.5 Gate self-test (3.13+ only, `skipif` below): run a one-file leaking test in a subprocess
  with the repo's `pyproject.toml` and assert the run fails with `unclosed database`; verify red
  (gate absent)
- [ ] 1.6 `tests/test_dev1739_fixture_smoke.py`-style smoke for `seeded_exec_engine`: yields a
  working engine and db path on sqlite + duckdb, closes the query engine and invalidates the
  factory engine on exit (the temp db file can be unlinked immediately, `engine_factory` cache no
  longer holds the datasource), no ResourceWarning across the lifecycle on 3.13+; verify red
- [ ] 1.7 Docs check: `docs/getting-started/python.md` sentence on `engine.close()` present;
  verify via grep in the test suite's docs tests if one exists, else manual

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

- [ ] 5.1 `tests/_engine_helpers.py::seeded_exec_engine` (D9); verify 1.6 green
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
