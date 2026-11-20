## 1. Tests (pr-tests stage; every test fails on today's code)

- [x] 1.1 Shared helper: a blocking stub that records its call count and waits on a `threading.Event` set only by a coroutine scheduled on the running loop, raising/failing on timeout (design.md Decision 6). Verify: a smoke check that the stub times out when called on the loop thread.
- [x] 1.2 `tests/test_gateway_features.py` `TestTheLoopStaysResponsive`: (a) dataset-subtree refresh through the real ingest/edit orchestration with the blocking embedding stub; (b) search with a seeded non-`None` corpus and a non-blank question reaching `EmbeddingRetriever.retrieve`; (c) cypher-filtered search with a blocking graph-check stub (returning False → naive fallback, no ladybug needed). Each asserts its own stub call count ≥ 1 and loop schedulability. Verify: all three fail on today's code.
- [x] 1.3 `tests/test_embedding_retriever.py`: test parametrized over `retrieve`, `upsert_memory`, `refresh_datasource`, `refresh_dataset_subtree` with the blocking embedding stub. Verify: each case fails on today's code.
- [x] 1.4 `tests/test_embeddings_client.py`: `embed_batch` with availability true and a blocking `truncate_text_for_model` keeps the loop schedulable; `is_available_async` honours a monkeypatched stub, honours `is_available.cache_clear()` (fresh computation), and runs the check off the loop thread. Verify: fail on today's code (missing gate / truncation on loop).
- [x] 1.5 Graph gate unit test (in the test file covering `slayer/search/graph.py` or `tests/test_search_unified.py`): `is_available_async` honours a stub and runs off the loop thread; `get_filtered_ids` with a blocking graph-check stub returning False keeps the loop schedulable, then raises its `ValueError`. Verify: fails on today's code.
- [x] 1.6 `tests/test_law_search_checks_off_loop.py`: AST law over `slayer/` per design.md Decision 5 (import-binding resolution; nested sync defs and lambdas excluded; function references allowed). Include fixture-snippet self-tests: module alias, from-import alias, bare name in the defining module are flagged; `to_thread(is_available)` and the unrelated same-named function aren't. Verify: the law fails on today's `slayer/` (lists the eight call sites in seven functions).

## 2. Implementation (pr-implement stage)

- [ ] 2.1 `slayer/embeddings/client.py`: add `is_available_async()` (always `asyncio.to_thread`, no fast path); `embed_batch` awaits it and runs the truncation loop in one `asyncio.to_thread`. Verify: 1.4 passes.
- [ ] 2.2 `slayer/search/retrievers/embeddings.py`: the four gates await `embedding_client.is_available_async()`. Verify: 1.2(a)(b) and 1.3 pass.
- [ ] 2.3 `slayer/search/graph.py`: add `is_available_async()`; `slayer/search/service.py` `_apply_cypher_filter` and `get_filtered_ids` await it. Verify: 1.2(c) and 1.5 pass.
- [ ] 2.4 Verify the law test 1.6 passes and the existing `is_available` monkeypatching tests still pass unchanged.

## 3. Gates

- [ ] 3.1 `architecture/model.c4` `specs ['search']` on the `search` bucket is in place (applied at plan time); `poetry run la-arch-check` and `npx likec4 validate` pass.
- [ ] 3.2 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs baseline) pass; `openspec validate dev-2380-embedding-availability-check-imports-litellm --strict` passes.
