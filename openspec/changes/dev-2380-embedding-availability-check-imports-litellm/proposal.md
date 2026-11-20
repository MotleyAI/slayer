## Why

The first call to the embedding channel's availability check imports `litellm` (about 2s cold) synchronously, and five async call sites run it on the event loop. So in an MCP or REST server with `advanced_search` installed, the first ingest, `save_memory`, model edit or search freezes every request. The graph extra's availability check has the same shape: it imports `ladybug` on the loop on the first cypher-filtered search (about 28ms). Both break the async-first principle (`architecture/system.arc42.md` §3.5).

## What Changes

- An async availability gate for the embedding channel always runs the sync check in a worker thread, and still honours monkeypatched stubs and `cache_clear()`.
- Every async caller of the embedding check uses the gate: `EmbeddingRetriever.retrieve`, `upsert_memory`, `refresh_datasource`, `refresh_dataset_subtree`, and `embed_batch`.
- `embed_batch` runs its token truncation (tiktoken encoder load and encoding) in a worker thread.
- The graph extra's availability check gets the same async gate, used by `SearchService._apply_cypher_filter`.
- A law test fails when any `async def` under `slayer/` directly calls one of these lazily importing checks or the embedding truncation.
- The `search` bucket in `architecture/model.c4` owns the new `search` spec group.

## Capabilities

### New Capabilities
- `search/event-loop`: search work reached from async code (optional-extra availability checks and embedding token truncation) never blocks the event loop.

### Modified Capabilities

## Impact

- Code: `slayer/embeddings/client.py`, `slayer/search/retrievers/embeddings.py`, `slayer/search/graph.py`, `slayer/search/service.py`.
- Architecture: `architecture/model.c4` (`specs ['search']` on the `search` bucket).
- Tests: `tests/test_gateway_features.py`, `tests/test_embeddings_client.py`, `tests/test_embedding_retriever.py`, a new `tests/test_law_search_checks_off_loop.py`.
- No API, dependency or user-facing behaviour change other than a responsive event loop.
