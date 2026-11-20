## Context

See proposal.md - Why. `slayer/embeddings/client.py` `is_available()` and `slayer/search/graph.py` `is_available()` are `lru_cache`d sync functions whose first call imports an optional extra (`litellm` ≈ 2.2s cold, `ladybug` ≈ 28ms). Async code calls them directly: the four `EmbeddingRetriever` gates (`retrieve`, `upsert_memory`, `refresh_datasource`, `refresh_dataset_subtree`), `embed_batch`, and `SearchService._apply_cypher_filter`. `embed_batch` also runs `truncate_text_for_model` on the loop, and its tiktoken encoder load reads the BPE file from a local cache, or downloads it over the network when that cache is empty.

`tests/conftest.py` stubs the embedding check with `lambda: False` for every test, and many tests monkeypatch `embedding_client.is_available` with plain lambdas. Those keep working unchanged.

Principle: `architecture/system.arc42.md` §3.5 async-first. No arc42 contradiction and no arc42 edit.

**Structural cause.** A blocking, lazily importing sync check is callable from any `async def`, and nothing stops a new async caller from doing so. Fix: one async gate per check, every async caller goes through it, and a law test bans direct calls from async code. That closes the whole class in `search`, not just the reported instance.

## Goals / Non-Goals

**Goals:**
- Every optional-extra availability check in `slayer.search` / `slayer.embeddings`, and embedding token truncation, runs off the loop when reached from async code.
- A law test keeps it that way for future call sites.

**Non-Goals:**
- The sync prelude of `litellm.aembedding` (about 20ms, inside litellm) stays on the loop. Moving it would mean swapping the async call for sync `litellm.embedding` in a thread, which changes litellm's own async logging and retry path for little gain.
- Sync `is_available()` / `truncate_text_for_model()` keep their signatures and caching: sync callers and test monkeypatches depend on them.
- An arc42 edit: §3.5 is not re-tagged as enforced by the narrow law test.

## Decisions

1. **Async gates.** `slayer/embeddings/client.py` gets `async def is_available_async() -> bool: return await asyncio.to_thread(is_available)`, and `slayer/search/graph.py` gets the same over its own `is_available`. Each reads its module global at call time, so monkeypatched stubs and `cache_clear()` are honoured. There is no warm-cache inline fast path: checking `cache_info().currsize` and then calling isn't atomic (a `cache_clear()` in between, or a stub exposing `cache_info`, would put the import back on the loop), and the hop it saves is marginal next to the storage hops these paths already make. (codex, reversing an earlier default)
2. **Call sites.** `EmbeddingRetriever.retrieve` / `upsert_memory` / `refresh_datasource` / `refresh_dataset_subtree` and `embed_batch` await the embedding gate. `SearchService._apply_cypher_filter` awaits the graph gate. The issue's `refresh_model_subtree` is today's `refresh_dataset_subtree`. (default)
3. **Truncation off the loop.** `embed_batch` runs the whole per-text `truncate_text_for_model` loop in one `asyncio.to_thread` call, because the encoder load can be a network download. (default)
4. **Graph check in scope.** The graph extra's availability check has the same blocking shape (a cold `ladybug` import inside the async `_apply_cypher_filter`). It gets the same gate, and the law and the spec cover it. No existing Linear issue covered it (searched "ladybug event loop", "graph is_available blocking"; nearest is DEV-2117, done and limited to DB introspection). (user)
5. **Law test.** `tests/test_law_search_checks_off_loop.py`, an AST walk of `slayer/`, fails on any direct call inside an `async def` body to `slayer.embeddings.client.is_available`, `slayer.embeddings.client.truncate_text_for_model` or `slayer.search.graph.is_available`. Calls are resolved through import bindings (module aliases such as `embedding_client.is_available` / `_search_graph.is_available`, `from … import … as …` aliases, and bare names inside the defining module), never by bare name alone, so the unrelated same-named functions aren't flagged and aliases don't slip through. Nested sync `def`s and lambdas are excluded, and passing the function as a reference (to `to_thread`) is allowed. It fails on today's code. (user; resolution rule: codex)
6. **Loop-schedulability assertion (deterministic).** The blocking stub waits on a `threading.Event` that only a coroutine scheduled on the event loop sets. Off the loop, the loop runs that coroutine and the stub returns. On the loop, it never runs, so the stub's wait times out and the test fails. The timeout only matters on the failure path, so there's no timing threshold when the test passes. This is the deterministic form of the acceptance's "the loop keeps ticking", chosen over a timing-based tick counter (flake-prone on CI runners). (user; mechanism: codex)
7. **Per-path coverage.**
   - `TestTheLoopStaysResponsive` in `tests/test_gateway_features.py` drives (a) a dataset-subtree refresh through the real ingest/edit orchestration, not a direct retriever call; (b) a search with a seeded, non-`None` corpus and a non-blank question, so `retrieve()` reaches the gate; (c) a cypher-filtered search with a blocking graph-check stub. Each asserts its own stub call count.
   - A retriever-level test parametrized over `retrieve`, `upsert_memory`, `refresh_datasource` and `refresh_dataset_subtree`.
   - An `embed_batch` test with blocking truncation.
   - Gate unit tests: stub honoured, `cache_clear()` honoured, run off the loop thread.

   Every case fails on today's code. (codex)
8. **Spec attachment.** A new capability `search/event-loop` in a new `search` spec group, owned by the `search` bucket through the `architecture/model.c4` edit `+ specs ['search']` in its metadata. Chosen over `skip_specs`, for consistency with "Live database access never blocks the event loop" in `database/execution`. (user, approved wording)
9. **Requirement wording.** "Search work reached from asynchronous code — each check that an optional extra is available, including importing that extra's library, and embedding token truncation — SHALL run without blocking the event loop." (user, approved wording)
10. **The `aembedding` sync prelude stays on the loop**; see Non-Goals. (default)

## Risks / Trade-offs

- [Every gated call is now one executor hop, even when cached] → A few microseconds when the executor is idle. Under saturation it queues like the storage calls on the same path already do.
- [The default executor serialises the cold import behind busy workers] → The import happens once per process; the loop stays responsive while it waits.
- [The law test covers three named functions, not every blocking call] → Accepted scope. Arc42 §3.5 stays `[review]`.
