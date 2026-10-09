## Context

Every surface (engine, inspect, search, memories, facade, MCP, REST) reads models from one `StorageBackend`. The base class composes all of its derived reads (`load_models`, `list_models`, `resolve_model_identity`, `builtin_models`, `get_model_or_builtin`, `list_memories`, …) from a small set of reads through `self`. Six call sites outside storage call `_list_all_model_identities` directly. The YAML and SQLite `get_model` resolve bare names via `self._resolve_target_or_none`, and their load path may write a migrated model back. About eight readers use `model.joins` without looking the target up: `inspect/model_render`, `inspect/collection_render`, `search/render`, `join_walker.resolve_hop`, cardinality detection and `schema_drift`. The search graph cache (`search/graph.py`) is keyed by an `isinstance` path or by `id(storage)`, and is checked for freshness only through `graph_fingerprint()`. The pg facade already builds one store per principal through `storage_provider`. Unloadable documents follow `models/document-isolation`. The retired `JoinSyncStorage` (git `f0565399c^`) is the precedent for a `StorageBackend` subclass that wraps another backend.

## Goals / Non-Goals

**Goals:** visibility enforced in one place, below every reader, with no caller context passed downstream.

**Non-Goals:**
- concurrency control on writes (every writer stays last-writer-wins, as today);
- datasource visibility;
- raw-SQL table references that are not dotted paths;
- a way for SLayer's own `serve` / `mcp` to build a filtered store.

## Decisions

### D1 — Inaccessible ≡ deleted
A non-bypass `TagFilteredStorage` reads as the store with every invisible model and hidden memory deleted. Two refinements apply: joins whose targets are hidden are pruned (D4), and withheld memory parts are stripped (D5). Every rule reduces to the existing "this document does not exist" behaviour, so readers need no changes.
- *Alternative: each reader drops absent join targets (the issue's literal item 2).* Rejected: about 8 sites would have to agree by hand, the next new reader would leak again, and editors would lose drift reporting.

### D2 — Wrapper shape
`TagFilteredStorage(backend, *, tags, bypass)` subclasses `StorageBackend` and lives in `slayer/storage/tag_filtered.py`.
- **Filtered methods:** `_list_all_model_identities`, `get_model`, `_get_memory_row`, `_list_memories_rows`, `_memory_ids`, `list_embeddings`, `get_embedding`, `get_embeddings_for_canonical_ids`, `graph_fingerprint` and `cache_identity`.
- **`get_model`** resolves a bare name against the filtered identities, then calls `inner.get_model(name, data_source=ds)`, so ambiguity candidates never include a hidden model.
- **Everything else** is the base class's composition over `self`.
- `_ids_collide_as_filenames` is copied from the inner backend, `aclose` is forwarded, and `bypass=True` delegates everything unchanged.
- *Alternative: a `__getattr__` proxy, like `bundle_builder._ModelReadCache`.* Rejected: the base's derived methods would then run on the inner backend unfiltered.

### D3 — The view
One pass over the **inner** store, never through the wrapper's own filtered methods, computes:
- the loaded models, and the identities that fail to load;
- each model's reads (D6);
- the visible identities: the fixpoint of accessible ∧ loadable ∧ all reads visible;
- the hidden memory ids, and per-memory strip sets.

The view is memoized per wrapper instance under the inner `graph_fingerprint()`, re-read **after** the pass, because migration write-back may change it during the pass. Building the view is serialized by an `asyncio.Lock`. A `None` fingerprint means the view is never reused.

### D4 — Join pruning
Returned model copies drop joins whose target identity exists in the inner store but is outside the view. Joins whose target doesn't exist anywhere stay, so real drift is still reported.

### D5 — Memories
A memory's linked models are its model entities plus `models_read_by(example query)`. It is hidden iff it has at least one linked model and all of them exist but are invisible. A visible copy is returned without entities naming hidden models, their descendants or hidden memories, and without its example query when that query reads a hidden model. The memory resolver's stale-query warnings don't run for a withheld query.

### D6 — One reads extractor
`models_read_by` in `slayer.engine` takes a model or a query and returns the `(data_source, name)` identities it reads. It covers stage sources, extension and inline join targets, sibling-stage chains, and every hop of every dotted path.
- Query fields come from the same field enumeration the binder uses, `order` included.
- Model definitions covered: `Column.sql`, `Column.filter`, model `filters`, saved-measure formulas (recursively) and aggregation params.
- Paths are resolved with the join walker over the full set of inner models; an unresolvable path adds nothing.
- `memories/resolver.extract_entities_from_query` is rebuilt on the same walk and keeps its leaf-only entity output. `stage_ordering.stage_sibling_reads` is the starting point for the stage-source part.

### D7 — Merged writes
For a non-bypass caller, `save_model` runs in two phases:
1. Phase 1 is the base save validation run on the wrapper, so peers and identities are filtered. Its errors pass through typed. Through the engine, the engine's own pre-save checks also run on the view first.
2. Phase 2 merges the stored model's pruned joins back in, then calls `inner.save_model(merged)`, which runs the full validation.

Any phase-2 failure becomes `HiddenContentConflictError`, whose message names only the model being saved. Name, id and edge-name clashes with hidden documents are detected while merging and raise the same error. A join whose target is outside the view fails phase 1 as an unknown model. A change to `access_tags` raises `AccessTagsEditError` before anything else runs.

The memory merge sits at `_save_memory_row`, so `save_memory`, ingestion cleanup and cascades all keep the withheld entities and query.

Deletes resolve the target in the view and then run on the inner backend, so cascades cover hidden memories and embeddings. Embedding writes for hidden ids raise `HiddenContentConflictError`.

### D8 — Narrowed access
A model is narrowed when some tagged model `R` in its transitive reads has `R.access_tags ⊉ M.access_tags`, or `M` is untagged.
- **On save:** the save computes this over the full store and warns `narrowed_access` (a structured payload, surfaced twice per core P5), naming each `R`.
- **Reverse direction:** saving a model whose tags changed warns once for each reader that newly becomes narrowed.
- **`validate_models`:** gains a narrowed-model finding.
- **Callers:** only editors see these warnings with hidden names. For a non-bypass caller every `R` is visible, since a model can only read models in its view.

### D9 — Cache identity
`StorageBackend.cache_identity()` gives YAML and SQLite their absolute path and everything else a per-instance UUID. The wrapper returns `(inner identity, frozenset(tags), bypass)`, or the inner identity alone when bypass is set.
- `search/graph.py` keys `_cache` / `_locks` by this identity, so the cache grows to at most one entry per tag set per backend.
- The base `graph_fingerprint()` default becomes `None`.
- The graph cache and the pg facade's catalog refresh treat `None` as stale. That fixes the adjacent bug where a backend with the default fingerprint never rebuilds.
- The wrapper's fingerprint is `(inner fingerprint, tags, bypass)`, or `None` when the inner one is `None`.

### D10 — Remove `backing_query_sql`
The field, its inspect sections and its "must not be supplied" checks are removed. The v15 migration pops the field, and the save-time dry run keeps filling the cached `columns`. Existing test assertions on the field become dry-run SQL assertions where the behaviour still matters, and the rest are dropped.

## Risks / Trade-offs

- **Recomputing the view.** It loads every model and memory on each content change. → It's memoized by fingerprint and the YAML stat cache absorbs repeated reads. The cost is the same order as the `load_models` passes the engine already makes.
- **Learning text.** A memory's free-text `learning` can still name a hidden model. → Unavoidable; structured parts are withheld.
- **The generic conflict error.** It reveals that some hidden part exists, but never its name or content. → Accepted.
- **Stale views on non-compliant backends.** A third-party backend whose fingerprint doesn't change on writes gets stale views. → The new `None` default fails safe (never reused). Backends that do report a fingerprint already must honour it for the search graph.
- **DEV-2021 raw-SQL holes stay open.** A raw-SQL table reference in a query payload, or in a model definition that isn't a dotted path, bypasses tags. → Tracked by DEV-2021.

## Migration Plan

- `SlayerModel` v15 is forward-only, and is applied on load with write-back.
- Hosts adopt the wrapper by wrapping the store they pass to engines and services. Unwrapped stores behave as before, apart from the `backing_query_sql` removal and the `graph_fingerprint` default.
