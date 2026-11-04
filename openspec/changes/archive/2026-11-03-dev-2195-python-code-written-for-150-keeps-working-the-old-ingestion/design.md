## Context

See proposal.md (Why). PR #532 (DEV-2181) made every database-touching entry point take an engine-owned `SqlClient`; `ingest_datasource[_report]` became async with `client=` required, `ingest_datasource_idempotent` / `validate_datasource` gained a required `client` (`sql_clients` dropped), and the dbt / OSI converters lost `sa_engine` in favour of `await collect_metadata(client=...)` + a pure `convert()`. 1.6.0 is unreleased (PyPI latest: 1.5.0), so the async forms never shipped.

Structural cause of the missing removal reminder: deprecations are ad hoc (`legacy_backends.py` warns free-form; `--models-dir`, `SLAYER_MODELS_DIR`, `inspect_model`, `.identifier` are help text or comments only), with no shared marker, so neither their warning nor their removal is checkable. Fix: one helper that both warns and tags, plus one tripwire test scanning for it.

Applicable principles: system §3.17 one database door (every fallback reads through a short-lived engine's `client_for`; `tests/test_law_database_door.py` unchanged — `SlayerSQLClient` construction is not a door, and all SA access stays in `slayer/sql/client.py`), §3.7 async-first (sync forms bridge via `run_sync`), §3.12 Pydantic only; sql §3.14 one engine owner (a caller-supplied SQLAlchemy engine's owner is the caller — SLayer never disposes, invalidates or drains it); storage §3.2 access is a view (fallbacks load through `engine.storage`). core P5 (structured warnings on responses) does not apply: deprecations are not query degradations. Spec attaches to the new cross-cutting capability `compatibility` (`architecture/index.yaml`, entry below). No arc42 or LikeC4 edit; the helper lives in `slayer/core`, which every node already imports.

## Goals / Non-Goals

**Goals:** the 1.5.0 forms of the issue's table run unchanged (results equal to the new forms) with one caller-attributed `FutureWarning`; engine one-liners for the common case; one marker and a tripwire covering every 2.0-scheduled workaround.

**Non-Goals:**
- Restoring the 1.5.0 forms of undocumented public helpers: `ingest_all_datasources_idempotent`, `introspect_table_to_model`, `list_ingestable_objects`.
- A no-client form on an *in-memory datasource config* (e.g. `sqlite://`) sees a fresh database: 1.5.0 read through the process-cached factory engine, today each client owns a private in-memory engine. In-memory callers use `client=` or converter `sa_engine=`.
- The known gap: a host whose engine has a `client_factory` but calls a deprecated form without `client=` gets a direct connection (a fallback engine cannot see another engine's factory). A process-wide default factory conflicts with per-engine factories carrying caller identity.
- Honouring `validate_datasource(sql_clients=)`: accepted and ignored.
- Marking stored-data upgrade paths, the colon aggregation spelling, `slayer.flight.{info_schema,probe_queries,translator}`, the pg facade `token=`.

## Decisions

### Marker and tripwire
1. **One helper, `slayer/core/deprecation.py`**: `warn_deprecated(message, *, removal, stacklevel=..., location=None)` emits `FutureWarning` (not `DeprecationWarning`, hidden outside `__main__`); `location` (filename/lineno/module) covers the `legacy_backends` class-hook case via `warnings.warn_explicit`. A call to it IS the tag; `removal` must be a string literal. Message style: what was used, what to use instead, "until SLayer 2.0".
2. **Tripwire `tests/test_law_removal_tripwire.py`**: import-binding-aware AST scan of `slayer/**/*.py` (reusing the ownership/door law's `_import_bindings`), so aliases are found; non-literal `removal` fails; version from `slayer.__version__` (read through one patchable function in the test); fails when `major(removal) <= major(version)`, naming file:line + message. Self-tests: the scan equals a pinned set of sites keyed by (file, feature), so a lost tag fails today; patched `2.0.0` names every pinned site; a synthetic aliased call is found.
3. **Warning granularity**: each use emits exactly one warning (tests reset filters); repeat display follows Python's normal filters; `legacy_backends`' per-class once for `_list_memories_rows` is kept.

### Call forms
4. **Sync forms**: `ingest_datasource` / `ingest_datasource_report` return to their 1.5.0 sync signatures and positional order, plus keyword-only `client=None`. With a client: run through `run_sync`, then `client.aclose()` in the same loop (the bridged loop dies; `aclose` releases its loop-bound resources and leaves the client usable — the `engine.run_sync` contract). Without: warn, fallback engine, `engine.run_sync`, `engine.close()`.
5. **Async names**: `ingest_datasource_async` / `ingest_datasource_report_async` (`client=` required) hold the implementations; the sync functions, `ingest_datasource_idempotent`, and the engine methods call them through the module attribute, so one monkeypatch seam per function covers MCP, REST, CLI and engine paths. Existing test seams move to the new names.
6. **Call-time warning for async entry points**: `ingest_datasource_idempotent` and `validate_datasource` become plain `def`s that warn (when applicable) and return the inner coroutine — the only way to name the caller's line under `run_sync(f(...))` on Python 3.11. Call-time work is limited to warning and argument capture; every resource is created inside the coroutine. Where `inspect.markcoroutinefunction` exists (3.12+) it is applied.
7. **`validate_datasource(sql_clients=)`**: accepted and ignored; omitted client and/or `sql_clients` → one combined warning per call.
8. **Fallback engine**: `SlayerQueryEngine` with no `client_factory` (exactly 1.5.0's direct connection) over `YAMLStorage` in a `TemporaryDirectory` when the call has no storage (as `tests/_osi_helpers.py` does), or over the caller's storage for the idempotent form, which then runs on `engine.storage` (raw storage refuses pre-v8 models that need refinement). Closed on success, error and cancellation.
9. **Engine methods** `ingest` / `ingest_sync` → `list[SlayerModel]`, `ingest_idempotent` / `ingest_idempotent_sync` → `IdempotentIngestResult` on `self.storage`; `self.client_for(ds)`; sync via `self.run_sync`. Docs and warnings recommend them.

### Converters and caller-owned engines
10. **Signatures**: `DbtToSlayerConverter(project, data_source, sa_engine=None, include_hidden_models=False, target_dialect=None)` and `OsiToSlayerConverter(documents, data_source, sa_engine=None, *, dialect=None, target_dialect=None)` — the 1.5.0 order (safe: 1.6.0 unreleased). Passing `sa_engine` warns at construction.
11. **`convert()` fallback**: without prior metadata on an `sa_engine` converter, collect through a fallback engine whose `client_factory` returns `SlayerSQLClient(datasource=<derived>, sa_engine=<caller's engine>)`; derived `DatasourceConfig`: name `data_source`, type via `dialect_for_sa_backend(url backend)`, connection string = rendered URL (identity/cache key only).
12. **`SlayerSQLClient(datasource, *, sa_engine=None)`**: engine ownership recorded explicitly (caller / client-private / factory); for a caller engine every statement and metadata call uses it (async path off) and `close`, `aclose`, auth-failure invalidation and the finalizer never dispose, invalidate or `release_idle` it.
13. **Caller-thread execution**: in-memory SQLAlchemy engines are thread-bound (`SingletonThreadPool`: another thread sees an empty database — reproduced for `sqlite://` and `duckdb:///:memory:`). A caller-engine client runs its sync work inline instead of in a worker thread. With no running loop the fallback runs on the caller's thread; under a running loop (Jupyter, async caller) a new helper in `slayer/async_utils.py` runs the coroutine on a helper thread while the blocked caller thread serves the client's sync calls from a queue until it finishes.

### Other covered workarounds
14. `legacy_backends.py` warnings move onto the helper, same texts and locations.
15. **CLI storage**: one resolver replaces the repeated `args.storage or args.models_dir or _STORAGE_DEFAULT` sites; precedence `--storage` > `--models-dir` > `SLAYER_STORAGE` > `SLAYER_MODELS_DIR` > default; a deprecated input warns only when it decides; the default is resolved lazily (no import-time read); the CLI renders `FutureWarning`s as one stderr line.
16. MCP `inspect_model` warns server-side per call; `MemoryNotFoundError.identifier` becomes a warning property returning `memory_id`.
17. Every surface recommending `inspect_model` is retargeted to `inspect` — the render footer hint (`slayer/inspect/model_render.py`), `docs/reference/mcp.md` rows and workflow examples, `docs/concepts/memories.md`, CLI help text, descriptive doc mentions (`docs/api_gaps.md` exempt).
18. Dockerfile and `examples/*/start.sh` move to `--storage` / `SLAYER_STORAGE`.

### Architecture edit (approved wording, apply verbatim)
- `architecture/index.yaml` `cross_cutting_specs` gains `  compatibility: {touches: [python.core, python.engine, python.importers, python.storage, python.surfaces]}` (user, approved wording)

### Decisions log
1. Undocumented public helpers (`ingest_all_datasources_idempotent`, `introspect_table_to_model`, `list_ingestable_objects`) not covered; non-goal. (user)
2. Four engine methods `ingest` / `ingest_sync` / `ingest_idempotent` / `ingest_idempotent_sync`; no `ingest_report` — the report stays reachable via `ingest_datasource_report_async(client=)`. (user)
3. Spec home: one new cross-cutting capability `compatibility` with `legacy-call-forms` and `removal-tripwire`, chosen over spreading across five capabilities. (user)
4. `architecture/index.yaml`: `  compatibility: {touches: [python.core, python.engine, python.importers, python.storage, python.surfaces]}` (user, approved wording)
5. Retarget every surface recommending `inspect_model` to `inspect`, incl. the agent-facing footer hint and descriptive mentions (extension approved). (user)
6. Caller engines are served on the caller's thread, incl. under a running loop via a caller-thread call queue; chosen over refusing thread-bound engines under a running loop. (user, codex)
7. `validate_datasource(sql_clients=)` accepted and ignored. Codex finding (honour it via a non-owning client factory) rejected (codex): undocumented internal plumbing whose only 1.5.0 caller was the engine; clients are keyed by the same connection identity, so results differ only for an in-memory or custom client. (user)
8. In-memory datasource configs under no-client forms see a fresh database; equivalence tests use file-backed SQLite/DuckDB; scope cut approved. Chosen over reproducing 1.5.0's factory-cached in-memory engine. (user, codex)
9. Storage-less fallback engine on `YAMLStorage` in a `TemporaryDirectory`; a null storage would need every abstract method stubbed. (default)
10. Idempotent fallback runs on `engine.storage`, not the raw storage. (default)
11. Converter `sa_engine` maps to a client over the caller's engine, never a URL-derived datasource (an in-memory URL would open an empty database; `connect_args`/`creator` would be lost). (default)
12. 1.5.0 positional order restored for both converters; warning at construction. (default)
13. One helper in `core`; its call is the tag; tripwire = AST scan vs `slayer.__version__` major. (default)
14. `legacy_backends` warnings move onto the helper. (default)
15. `inspect_model` warns server-side, per the agreed 2.0 removal scope. (default)
16. Env var and flag warn only when they decide; CLI renders `FutureWarning`s as one stderr line. (default)
17. `ingest_datasource_idempotent` / `validate_datasource` become plain `def`s returning coroutines, for caller-line attribution. (default)
18. Sync forms given a client `aclose()` it after the bridged run. Codex's "never close a supplied client" rejected (codex): without `aclose` the client keeps an async engine bound to the dead loop and breaks on next use; a test proves the client stays usable. (default)
19. Dockerfile and example scripts move to `--storage` / `SLAYER_STORAGE`. (default)
20. Explicit engine-ownership state in `SlayerSQLClient`, gating dispose / `release_idle` / auth invalidation / finalizer; auth-failure test. (codex)
21. One helper API incl. explicit location; import-alias-aware scan; pinned site set so a lost tag fails today. (codex)
22. Warning granularity: one emission per use; display per Python filters; `legacy_backends` per-class once kept. (codex)
23. Plain-def wrappers do only warning + argument capture at call time; tests for an unawaited coroutine and an exception before the first await; `inspect.markcoroutinefunction` where available. (codex)
24. Real async implementations under one module-level name each, called through the module attribute by every public form and engine method — one patch seam. (codex)
25. One CLI storage resolver with explicit precedence and shadowing; all precedence pairs tested. (codex)

## Risks / Trade-offs

- [Docker image users setting `-e SLAYER_MODELS_DIR=…` with a custom command now get `/data` — the image's `SLAYER_STORAGE` wins] → the default `CMD` passes `--storage /data` explicitly; noted in release notes.
- [`asyncio.iscoroutinefunction` is False on Python 3.11 for `ingest_datasource_idempotent` / `validate_datasource`] → `await f(...)` is unchanged; marked as coroutine functions on 3.12+.
- [Caller-thread call queue deadlock if the caller thread is also needed by the coroutine for anything else] → the queue is served until the coroutine's future completes; helper-thread exceptions propagate; covered by a running-loop test.
- [Fallback engines cost one connection setup per legacy call] → same cost as 1.5.0's direct connection.

## Migration Plan

No stored-data change. 1.5.0 callers: no change needed (warnings). Callers of the unreleased async `ingest_datasource[_report]`: none outside this repo (1.6.0 unreleased); internal callers move to the `*_async` names. Rollback = revert the PR.
