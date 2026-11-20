## Why

PR #612 (`main`) moves search's optional-extra availability checks and embedding token truncation off the event loop. On `1.x`, the first embedding check still imports `litellm` (one to two seconds cold) on the loop, so in an MCP or REST server with `advanced_search` the first ingest, `save_memory`, model edit or search freezes every request; the graph check does the same with `ladybug`. The 1.x line needs the same fix in a 1.x release.

## What Changes

- Port #612, in the state that merged to `main` (`abccc2074`), onto `1.x`:
  - async gates `is_available_async()` in `slayer/embeddings/client.py` and `slayer/search/graph.py`, each running the cached sync check in a worker thread;
  - every async caller awaits them: `EmbeddingRetriever.retrieve`, `upsert_memory`, `refresh_datasource`, `refresh_model_subtree`, `embed_batch`, `SearchService._apply_cypher_filter`, `get_filtered_ids`;
  - `embed_batch` runs token truncation in a worker thread;
  - the law test banning direct calls to these checks, or the truncation, from any `async def` under `slayer/`;
  - #612's comment trims, issue-reference removals and `ALLOW(import-not-top)` comments in the touched files.
- 1.x extension: `EmbeddingRetriever.retrieve` no longer imports `numpy` / `slayer.embeddings.ranker` inside the async method; the import moves to module scope exactly as on `main`.
- The `search` bucket in `architecture/model.c4` owns the new `search` spec group, as on `main`.

## Capabilities

### New Capabilities
- `search/event-loop`: search work reached from async code (optional-extra availability checks, their library imports, and embedding token truncation) never blocks the event loop. The delta is #612's, byte for byte.

### Modified Capabilities

## Impact

- Code: `slayer/embeddings/client.py`, `slayer/search/retrievers/embeddings.py`, `slayer/search/graph.py`, `slayer/search/service.py`.
- Architecture: `architecture/model.c4` (`specs ['search']` on the `search` bucket).
- Tests: new `tests/_loop_gated_stub.py`, `tests/test_loop_gated_stub.py`, `tests/test_law_search_checks_off_loop.py`; changed `tests/test_gateway_features.py`, `tests/test_embedding_retriever.py`, `tests/test_embeddings_client.py`, `tests/test_search_graph.py`.
- No API, dependency or user-facing change other than a responsive event loop. No version bump.
- Forward merge (1.x → `main`): reconciliation notes are recorded on DEV-2381.
