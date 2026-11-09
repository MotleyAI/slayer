## Context

See proposal.md (Why). `StorageBackend._settle_library_granularities()` (`slayer/storage/base.py`) runs once per instance, called from `_migrate_and_refine_on_load` and `_memory_from_stored`. It calls `list_datasources()` and then `get_datasource()` for each name (each wrapped in `contextlib.suppress(ValueError)`), relying on the backend's `get_datasource` to call `_with_library_granularities_renamed`. Only `SQLiteStorage` and `YAMLStorage` do. The rename itself already reads and writes models and memories through raw-access hooks (`_list_all_model_identities`, `_load_raw_model_dict`, `_raw_memory_dicts`, `_write_raw_*`).

Structural cause: whether the settle accomplishes anything depends on each backend's `get_datasource` calling the rename by hand. The settle and the rename are two code paths that must agree without anything enforcing it, and the settle's cost falls on every backend whether or not it can rename.

Applicable principles (`architecture/system.arc42.md` §3): async-first (the new hook is async), versioned persistence (the rename stays a load-time upgrade of stored documents). No `python.storage` principle is touched; no arc42 or model edit; no new import edge.

## Goals / Non-Goals

**Goals:**
- The settle depends only on documents it can rewrite, so a backend without raw datasource documents gets the right behaviour by default.

**Non-Goals:**
- No change to the rename itself, its naming rule, its ordering (dependants before the datasource) or the per-read rename in `get_datasource`.
- No version bump; forward-merging into `main` happens in the release process, outside this change.

## Decisions

1. **A raw-read hook, not a capability flag (default, structural).** Add `async def _raw_datasource_dicts(self) -> dict[str, dict]` to `StorageBackend`, modelled on `_raw_memory_dicts`: every stored datasource document verbatim, keyed by the name its `get_datasource` reads it under; the default returns `{}` ("backends without raw access store none"). The settle loops over it and calls `_with_library_granularities_renamed(name=..., data=...)` per document. Alternatives: an explicit capability `ClassVar` (rejected: a third thing that must agree by hand with `get_datasource`), or detecting whether `get_datasource` is overridden (rejected: a bundled-backend subclass overrides it legitimately, and override detection is fragile).
2. **Keep the per-read rename in SQLite and YAML `get_datasource` (default).** It covers a `get_datasource` before any model or memory load and a document edited on disk after the settle; the stored-upgrade spec requires the rename "on every load".
3. **Wrappers keep the default (default).** `EngineStorage`, `AccessFilteredStorage` and `ModelOverlayStorage` never call `_migrate_and_refine_on_load` or `_memory_from_stored` themselves; the concrete backend they delegate to settles its own instance. A pass-through would be dead code (they don't pass `_raw_memory_dicts` through either).
4. **The YAML hook reads only `datasources/<name>.yaml` (codex).** `get_datasource` reads only `<name>.yaml` and `_write_raw_datasource_dict` writes `<name>.yaml`; a `.yml` entry included under its stem would make the settle write a new `<name>.yaml` beside an untouched `.yml`, and `shop.yaml` and `shop.yml` would collide on one key. The hook therefore exposes exactly what `get_datasource` reads, skipping names `_datasource_path` refuses, YAML parse errors and non-mapping documents.
5. **The SQLite hook is `SELECT name, data FROM datasources` through `_raw_rows_sync` (default).** It already skips rows that are not JSON or not a mapping.
6. **Per-document isolation stays `contextlib.suppress(ValueError)` (codex).** Each document's rename runs in its own `suppress(ValueError)`, as each `get_datasource` did; a parseable but invalid mapping is renamed as before (the rename never validated) and never blocks another datasource. Other exceptions (I/O failures on a write) propagate as today, so an interrupted rename still converges on the next load.
7. **The custom-backend test proves the settle ran (codex).** The counting backend decodes through `_migrate_and_refine_on_load` and `_memory_from_stored` and records `_raw_datasource_dicts` calls, so a backend that bypasses the base pipeline cannot pass the test vacuously; model-first and memory-first each on a fresh instance.
8. **Spec home: ADDED to `storage/custom-backends` (default).** The requirement is a backend-contract property; `python.storage` already owns the `storage` spec group, so no mapping change.
9. **Base branch `1.x` at v1.8.1; the PR targets `1.x` (user).**

## Risks / Trade-offs

- [A custom backend that does store raw datasource dicts and relied on its own `get_datasource` calling `_with_library_granularities_renamed` before a model load stops settling] → it must also implement `_write_raw_*` to rename anything, so it is already on the private raw-hook surface; overriding `_raw_datasource_dicts` beside them restores it. No bundled or known external backend is in this position.
- [The settle no longer validates datasources, so a broken datasource is no longer logged at settle time] → intended; it is reported where it is used.
