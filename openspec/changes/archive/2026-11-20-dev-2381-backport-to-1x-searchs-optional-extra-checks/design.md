## Context

See proposal.md - Why. The source is #612 as merged to `main`: `git diff abccc2074^1 abccc2074`. Its archived design (`openspec/changes/archive/2026-11-20-dev-2380-embedding-availability-check-imports-litellm/design.md` on `main`) carries the mechanism decisions (always-`to_thread` gates with no warm-cache fast path, the loop-gated stub, the law's import-binding resolution); this change adopts them unchanged.

- **Branches.** The branch starts at `origin/1.x` `4234b0dd3` (1.9.0); the PR targets `1.x`.
- **1.x vs `main` in the touched files:**
  - `slayer/embeddings/client.py` is byte-identical to `main`'s pre-merge file, so #612's hunks apply verbatim.
  - `slayer/search/retrievers/embeddings.py`, `graph.py` and `service.py` differ only in vocabulary (`refresh_model_subtree(model: SlayerModel)`, `models=`, the Cypher label `Model`) and in one 1.x-only block: `retrieve()` imports `numpy` / `slayer.embeddings.ranker` inside the async method; `main` imports them at module scope (`try: … except ImportError: np = None`, then `if np is None:`).
  - On 1.x, `numpy` is already loaded whenever `slayer.search.retrievers` is imported, through the base dependency `rank-bm25`; the inline import costs a `sys.modules` lookup plus a first `ranker` import (about 0.2 ms).
- **Structural cause** (as #612): a blocking, lazily importing sync check is callable from any `async def`. One async gate per check, every async caller through it, and a law test banning direct calls close the class.
- **Principles:** `architecture/system.arc42.md` §3 async-first; the model-truth import law (no new edges: `slayer.embeddings.ranker` is in the same `search` bucket; `#legacy` baseline unchanged); imports at top (each lazy `litellm` / `tiktoken` import carries `# ALLOW(import-not-top): optional dep, lazy`, as on `main`). No arc42 contradiction and no arc42 edit.

## Goals / Non-Goals

**Goals:**
- 1.x meets `search/event-loop` (specs/search/event-loop/spec.md) with #612's code.
- The next 1.x → `main` merge stays mechanical: the source blocks, `model.c4`, the spec, the helper and the law test are identical to `main`'s, apart from forced vocabulary.

**Non-Goals:**
- #612's non-goals: the `litellm.aembedding` sync prelude stays on the loop; sync `is_available()` / `truncate_text_for_model()` keep their signatures and caching.
- A new scenario for the numpy import: the requirement already covers library imports, and the delta stays byte-identical to `main`'s.
- Extending the law test beyond #612's three functions.
- An arc42 edit (§3.5 stays `[review]`), a docs change, or a version bump.

## Decisions

1. **Base and target:** `origin/1.x` `4234b0dd3`, PR into `1.x`; source is #612's merged state, `abccc2074^1..abccc2074`. (default)
2. **Spec reconciliation:** this change has its own id and carries #612's `search/event-loop` delta byte for byte; its wording is already 1.x vocabulary. Archiving yields an `openspec/specs/search/event-loop/spec.md` identical to `main`'s. Alternatives: `skip_specs`, leaving 1.x's corpus without the requirement; reusing the `dev-2380` id, giving two owners of one id. (default; DEV-2358 precedent)
3. **Spec attachment:** `+        specs ['search']` inside the `search` bucket's `metadata { }` in `architecture/model.c4`, identical to `main`. (user, approved wording)
4. **numpy / ranker import moves to module scope on 1.x**, text-identical to `main`'s block. Chosen for forward-merge alignment and literal compliance with the requirement; the runtime gain is about 0.2 ms, because `rank-bm25` already loads numpy. Alternative: keep the inline import, which conflicts on the forward merge and still imports an optional extra inside an async method. (user, re-confirmed after the premise correction)
5. **Tests for decision 4**, in `tests/test_embedding_retriever.py`:
   - (a) `retrieve()` on a seeded corpus, with the embedding check stubbed available and a loop-gated `builtins.__import__` stub that blocks on `numpy` / `slayer.embeddings.ranker`: the loop stays free and the stub is never hit. Fails on today's 1.x (any import statement goes through `__import__`).
   - (b) The module's `np` monkeypatched to `None`: `retrieve()` returns an empty `RetrievalResult` and logs the "numpy not installed" warning. Regression coverage for the new branch, not a fails-before test, and not counted toward the issue's "each fails without the fix". (codex)
   Both pass on `main` and are kept on the forward merge. (default)
6. **Naming:** test files, classes, test names and the helper keep `main`'s names. 1.x vocabulary appears only where 1.x code fixes it: method names and their parametrize ids, kwargs (`models=`, `model_name=`), Cypher labels (`MATCH (n:Model)`), and 1.x's harness and ingest entry points. (default; issue)
7. **#612's comment trims, issue-reference removals and `ALLOW` comments** are mirrored so the files match `main`. (default; issue)
8. **Forward-merge notes** go in a comment on DEV-2381. (default; DEV-2358 precedent)
9. **No arc42 edit, no docs, no version bump.** (default)
10. **Codex: "the hoist moves a ~410 ms import to startup and taxes non-embedding processes; use an async lazy loader".** Rejected: numpy is already loaded at startup on 1.x through the base dependency `rank-bm25`, so the hoist adds about 0.2 ms; a lazy loader would diverge from `main` and defeat decision 4. rejected (codex)
11. **Codex: "the `__import__` test can't detect module-import-time blocking; add a fresh-interpreter test".** Rejected: module import happens at server startup, before the loop serves requests, and nothing on 1.x imports the retriever lazily from a running request; test (a) pins exactly "`retrieve()` imports nothing on the loop". rejected (codex)
12. **Codex: "test (b) doesn't fail behaviourally before the fix".** Folded into decision 5. (codex)
13. **Codex: "extend the law to ban optional-extra imports inside async functions".** Rejected: the law stays byte-identical to `main`'s for a clean forward merge; a general ban is wider than this backport, and test (a) guards the one 1.x instance. rejected (codex)

## Risks / Trade-offs

- [Ported tests depend on 1.x harness / ingest shapes that differ from `main`'s] → Port by hand following the existing 1.x `TestTheLoopStaysResponsive` tests; each ported test must fail on 1.x without the fix.
- [Forward merge conflicts in vocabulary hunks] → Resolved to `main`'s side; recorded on DEV-2381.
- [Test (a) pins an import statement, not a measurable stall] → Accepted: it guards the requirement's "including importing that extra's library" for `retrieve()` regardless of what `rank-bm25` loads.
