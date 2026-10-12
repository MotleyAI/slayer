## Why

Storyline's role-based permissions (DEV-2014) need SLayer to show each caller only the models their access tags allow, on every surface that reads models: queries, joins, inspect, search, suggestions, root inference and the SQL facade. Today every surface reads one shared store, so per-caller visibility would have to be re-checked by hand at each of them. The structural fix is to make visibility a property of the store itself: a store wrapper built with the caller's tags reads as the store with every model they cannot access deleted, so no downstream code receives caller context or can forget to check it.

## What Changes

- `SlayerModel.access_tags: list[str]` (default `[]`, untagged = public), carried by model JSON, git sync, import/export, REST, the client and MCP `create_model` / `edit_model`. `SlayerModel` schema version 14 → 15.
- `TagFilteredStorage(backend, *, tags, bypass)`: a `StorageBackend` that, for a non-bypass caller, reads as the store with every invisible model and memory deleted (inaccessible ≡ deleted):
  - a model is visible iff it is untagged or shares a tag with the caller, it loads, and every model it reads (stage sources and every hop of every reference path in its stages and definitions) is visible, transitively;
  - joins into hidden models are pruned from the models it returns;
  - a memory is hidden iff every model it links to is hidden; visible memories lose hidden entities and an example query that reads a hidden model;
  - embeddings of hidden models and memories are hidden;
  - non-bypass writes merge onto the full stored documents, keeping hidden content; a write that conflicts with hidden content is refused with a generic error naming nothing hidden; only bypass callers change access tags.
  `bypass=True` passes everything through.
- Saving a model whose effective access is narrower than its declared tags (it reads a tagged model whose tags it does not cover) warns, in both directions; `validate_models` reports such models.
- Storage caches are per view: `StorageBackend.cache_identity()` replaces the search graph's path/`id()` keying, and the default `graph_fingerprint()` is `None` ("unknown, never reuse").
- **BREAKING**: `SlayerModel.backing_query_sql` is removed (the v15 migration drops it from stored models); inspect no longer shows a backing-SQL section and REST no longer returns the field. The SQL of a query-backed model is obtained with a `dry_run` query.

## Capabilities

### New Capabilities

- `models/model-access`: access tags, the per-caller filtered store (visibility, dependents, joins, memories, embeddings, merged writes), the narrowed-access warning, and per-view caches.

### Modified Capabilities

- `models/stored-upgrade`: stored query-backed models drop their cached backing SQL on load.

## Impact

- New `slayer/storage/tag_filtered.py`; `slayer/storage/base.py` (`cache_identity`, `graph_fingerprint` default), YAML/SQLite backends; new reads extractor in `slayer/engine/`, reused by `slayer/memories/resolver.py`; `slayer/search/graph.py` cache keying; `slayer/pg_facade/connection.py` fingerprint refresh; `slayer/core/models.py` (`access_tags`, `backing_query_sql` removed); `slayer/storage/migrations.py` + v15 migration; `slayer/engine/query_engine.py` (save-time warning, `validate_models` finding, backing-SQL removal); `slayer/inspect/`, `slayer/mcp/server.py`, `slayer/api/server.py`, `slayer/client/`.
- Storyline (DEV-2014) wraps every model store it builds with the caller's tags.
- Tests asserting `backing_query_sql` move to dry-run SQL assertions.
- Docs: `docs/concepts/models.md`, `docs/configuration/storage.md`, MCP and REST references.
- arc42: new principle 2 in `architecture/storage.arc42.md` ("Access is a view"), approved in the plan. No LikeC4 change.
