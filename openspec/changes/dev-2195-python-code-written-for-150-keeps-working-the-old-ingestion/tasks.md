## 1. Tests first (pr-tests stage)

- [x] 1.1 `tests/test_law_removal_tripwire.py`: import-alias-aware AST scan for the deprecation helper, literal-`removal` check, pinned (file, feature) site set, patched-2.0.0 failure naming every site, synthetic aliased-call self-test — verify it fails now (helper absent) and is unmarked (unit)
- [x] 1.2 `tests/test_compat_legacy_call_forms.py`: each 1.5.0 form from the issue table written as the 1.5.0 docs/examples wrote it (`git show v1.5.0:docs/getting-started/python.md`, `docs/concepts/ingestion.md`, `examples/`, the dbt/OSI notebooks) vs the new form on file-backed SQLite and DuckDB — equal results, exactly one `FutureWarning` with remedy text and the caller's filename, incl. `run_sync(ingest_datasource_idempotent(...))` attribution; idempotent fallback over a store needing refinement on load; `validate_datasource` with no client / `sql_clients={}` (one warning); verify all fail now
- [x] 1.3 Converter cases in the same file: dbt and OSI with `sa_engine=sa.create_engine("sqlite://")` (plain sync and inside a running loop) and a file-backed DuckDB engine; caller engine not disposed / invalidated / drained after success and after a simulated auth failure (spy on `dispose`, `pool.dispose`, `engine_factory.release_idle`, `engine_factory.invalidate_engine`)
- [x] 1.4 New-form silence and lifecycle: client-supplied forms and `*_async` names record no `FutureWarning`; a client used by sync `ingest_datasource(ds, client=...)` then serves an engine query; engine methods `ingest`/`ingest_sync`/`ingest_idempotent`/`ingest_idempotent_sync` equal the free functions and route through an engine `client_factory`; fallback engines close every client on success, database error and cancellation; another engine's factory is not invoked by a no-client call; plain-def wrappers: an unawaited returned coroutine closes cleanly, an exception before the first await leaks nothing
- [x] 1.5 Deprecated surfaces: `--models-dir` and `SLAYER_MODELS_DIR` precedence pairs (warn only when deciding, one stderr line, replacement silent), MCP `inspect_model` warns per call and `inspect` does not, `MemoryNotFoundError.identifier` warns and `memory_id` does not — extend `tests/test_cube_cli.py`'s `--models-dir` coverage rather than duplicate it
- [x] 1.6 Move existing test call sites and monkeypatch seams of the async ingestion functions to `ingest_datasource_async` / `ingest_datasource_report_async` (mechanical: `tests/test_ingestion*.py`, `test_ingest_internal_tables.py`, `test_mcp_server.py`, `test_ingest_cli_ux.py`, `test_startup_ingest.py`, integration files, …); verify `poetry run pytest -m "not integration" --collect-only` succeeds

## 2. Marker helper

- [ ] 2.1 `slayer/core/deprecation.py` `warn_deprecated(message, *, removal, stacklevel, location=None)` emitting `FutureWarning`; verify 1.1's scanner self-tests pass
- [ ] 2.2 Move both `slayer/storage/legacy_backends.py` warnings onto the helper (texts and locations unchanged); verify `tests/test_storage_backend_compat.py` passes unchanged

## 3. Client and sync bridge

- [ ] 3.1 `SlayerSQLClient(datasource, *, sa_engine=None)` with explicit engine ownership gating `close`/`aclose`/auth invalidation/finalizer; caller engine forces the sync path, run inline (no worker thread); verify 1.3's ownership tests and `tests/test_law_resource_ownership.py` / `tests/test_law_database_door.py` pass unchanged
- [ ] 3.2 Caller-thread call-queue helper in `slayer/async_utils.py` for running a coroutine whose sync database calls must stay on the calling thread, under a running loop; verify the in-loop in-memory converter case

## 4. Ingestion, drift, engine

- [ ] 4.1 Rename async implementations to `ingest_datasource_async` / `ingest_datasource_report_async`; restore sync `ingest_datasource` / `ingest_datasource_report` (1.5.0 order + keyword-only `client=None`, `aclose` after a client-supplied run, warning + fallback engine otherwise); verify 1.2 / 1.4 ingestion cases
- [ ] 4.2 `ingest_datasource_idempotent` / `validate_datasource` as plain-def coroutine returners with call-time warning; fallback engine (temp `YAMLStorage` or caller storage via `engine.storage`), `sql_clients` accepted and ignored, one combined warning; `inspect.markcoroutinefunction` on 3.12+; verify 1.2 / 1.4 cases
- [ ] 4.3 `SlayerQueryEngine.ingest` / `ingest_sync` / `ingest_idempotent` / `ingest_idempotent_sync`; verify engine-method tests
- [ ] 4.4 Move internal callers (MCP `create_datasource` / `ingest_datasource_models`, REST `/ingest`, CLI `_create_ingest_and_report`, `slayer/demo/jaffle_shop.py`, `ingest_all_datasources_idempotent`) to the `*_async` names or engine methods through the one module seam; verify the full unit suite

## 5. Converters

- [ ] 5.1 dbt and OSI converters: `sa_engine` restored in its 1.5.0 position with a construction-time warning; `convert()` without prior metadata collects through a fallback engine whose factory wraps the caller's engine (derived `DatasourceConfig`), using 3.2 under a running loop; verify 1.3

## 6. Other workarounds

- [ ] 6.1 One CLI storage resolver (precedence `--storage` > `--models-dir` > `SLAYER_STORAGE` > `SLAYER_MODELS_DIR` > default, lazy default, warn only when deciding) replacing every repeated resolution site in `slayer/cli.py`, and `slayer/storage/base.py` `default_storage_path`; CLI renders `FutureWarning` as one stderr line; verify 1.5
- [ ] 6.2 MCP `inspect_model` warns per call; `MemoryNotFoundError.identifier` warning property; verify 1.5
- [ ] 6.3 Retarget `inspect_model` recommendations to `inspect`: `slayer/inspect/model_render.py` footer hint, `slayer/cli.py` help text, `docs/reference/mcp.md` rows + both workflow examples, `docs/concepts/memories.md`, descriptive mentions in `docs/concepts/models.md`, `docs/concepts/search.md`, `docs/reference/cli.md` (not `docs/api_gaps.md`, not the DEPRECATED row); verify `grep -rn inspect_model docs slayer` shows only the tool, its deprecation row and exempt files

## 7. Docs, examples, architecture

- [x] 7.1 `architecture/index.yaml`: add `  compatibility: {touches: [python.core, python.engine, python.importers, python.storage, python.surfaces]}` verbatim (approved wording); verify `poetry run la-arch-check`
- [ ] 7.2 Docs show only new forms: `docs/getting-started/python.md` (+ one sentence: 1.5.0 forms still work with a `FutureWarning` until 2.0), `docs/concepts/ingestion.md`, `docs/examples/05_joins/joins_utils.py`, `docs/examples/13_osi_import/setup_osi.py`, `docs/examples/15_duckdb/duckdb_python_nb.ipynb` (re-executed with `jupyter nbconvert --execute --inplace`); verify the docs-examples tests
- [ ] 7.3 `examples/embedded/{run,verify}.py`, `examples/*/start.sh` → engine methods; `Dockerfile` and `examples/*/start.sh` → `--storage` / `SLAYER_STORAGE`; verify `examples/` scripts that run locally (embedded)

## 8. Gate

- [ ] 8.1 `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check` all green
