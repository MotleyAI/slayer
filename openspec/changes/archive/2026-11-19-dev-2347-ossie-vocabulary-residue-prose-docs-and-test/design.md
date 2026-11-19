## Context

- See proposal.md for the motivation. The tracker issue DEV-2347 holds the scope and the "Done when" sweep. DEV-2318's word table and carve-outs (archived change `2026-11-18-dev-2318-ossie-vocabulary-names-only-dataset-field`) apply unchanged.
- The branch starts at `main` with DEV-2318, DEV-1607 and the OTel work merged. DEV-2318's commit `02fb8f6b4` already renamed the dbt and Cube converter internals and the identifier leftovers it found. Its archived `tasks.md` hands prose to this change, and the user moved the remaining identifier stragglers and test identifiers to DEV-2364.
- Raw word-boundary hits on `main` @ `1d4d40f69`: about 3,500 in `slayer/`, 22,700 in `tests/`, 1,100 in `docs/` and 830 in `examples/`. Most are carve-outs or test names and locals.
- The published docs (docs.motley.ai/slayer) are built by MotleyAI/motley-docs. Its `scripts/merge_slayer_nav.py` copies only SLayer's `nav` from `zensical.toml`.
- `tests/integration/test_notebooks.py` executes notebooks in memory and saves nothing.
- `tests/test_law_guard_ratchet.py` reads the `x-guards` baseline from `architecture/index.yaml`; the guard here follows that pattern.

**Principles that apply:**
- `system` P1 (each top-level package claimed once) and P2 (model truth): no package is added, moved or renamed. The renamed `docs/examples/05_relationships/relationships_utils.py` sits outside `slayer/`.
- `system` P6 (Pydantic, never dataclasses): the guard's allowlist is parsed into a Pydantic model.
- None is strained.

**Spec attachment:** none. No capability is added or changed (`skip_specs: true`). The prose requirement is in `compatibility/ossie-vocabulary` ("Help topics, tool descriptions and messages SHALL use the new words").

## Goals / Non-Goals

**Goals:**
- Every old word in prose under `slayer/`, `docs/`, `examples/` and `tests/` is respelled or sits inside a carve-out.
- An old-word identifier defined in `slayer/` cannot be added without a visible allowlist entry, and the straggler count only falls.
- No behaviour, identifier or result changes.

**Non-Goals:**
- Identifier stragglers in `slayer/`, and test identifiers (helpers, fixtures, module constants, class fields, fixture parameters, test-data name literals): DEV-2364.
- Prose in `openspec/specs/` and `architecture/`, and capability ids.
- Redirects on docs.motley.ai: DEV-2368.
- Usages of old names, which `reportDeprecated` already covers for the twins.

## Decisions

1. **Identifier stragglers go to their own issue, DEV-2364,** not into this change. They are listed in the guard's `stragglers` and renamed there. Alternative: rename them here. Rejected: the user wants to build on DEV-2318 and DEV-1607 without merge churn and mop up later. (user)
2. **An identifier guard with a two-part allowlist is committed.** It is `tests/test_vocabulary_identifier_guard.py` with `tests/fixtures/vocabulary_identifier_allowlist.yaml`, holding `carve_outs` (each with a reason) and `stragglers`. It covers definitions in `slayer/` only, since test names and locals may keep old words. Alternative: a one-time sweep only. Rejected: old-word identifiers would return unnoticed. Prose is not guarded, because "column" and "join" are legitimately physical or SQL in a large share of hits. (user)
3. **The DEV-2318 handoff comment is dropped.** DEV-2318 merged with the handoff in its archived `tasks.md`. (user)
4. **Test identifiers go to DEV-2364 too.** DEV-2347's body was edited to say so, and its title was kept, so the branch name does not change. (user)
5. **Doc pages and example folders with old words in their paths are renamed, with redirects.** The new paths are listed in proposal.md. The work goes by link resolution and per-hit judgement, never blanket text substitution ("smartly, not grep"): every Markdown link, `nav` entry, notebook or test path and README URL is resolved, and only references to a moved file are rewritten. Each moved page gets a `zensical.toml` `redirect_maps` entry. Alternative: keep the paths and respell titles only. Rejected: old-word slugs would stay for good. (user)
6. **The prose respell is a per-hit judgement in context,** carve-out or respell. There is no regex replace. The bulk work fans out per directory, and each worker gets the carve-out table. (user)
7. **DEV-2368 carries the redirects to docs.motley.ai,** where SLayer's `redirect_maps` is otherwise ignored. MotleyAI/motley-docs is cloned at `/home/james/GitHub/Code/motley-docs`. (user)
8. **Converter internals are out of scope,** since DEV-2318's `02fb8f6b4` renamed them. Any converter name still left goes to `stragglers`. (default)
9. **Ossie's own wire names (`from_columns`, `to_columns`) are a carve-out,** because "column" is Ossie's word there. (default)
10. **`skip_specs: true`**, because no requirement changes. The only spec-quoted message with an old word ("partition_by column … is not a query dimension", `queries/transforms`) no longer exists in code. (default)
11. **The guard fails on stale entries,** so both allowlist lists stay exact. (default)
12. **Test-data name literals** (dataset and field names inside tests) count as test identifiers, so they are DEV-2364's. (default)
13. **Existing tests that pin respelled message text are aligned to the new text,** as a mechanical change. (user)
14. **Only names Pydantic's `BaseModel` defines are exempt as `model_*`,** computed from `dir(pydantic.BaseModel)`. Any other `model_*` name goes through the allowlist. (codex)
15. **The `stragglers` count has a ceiling that may only be lowered,** `x-vocabulary-stragglers: {baseline: N}` in `architecture/index.yaml`, like `x-guards`. The guard fails when `len(stragglers)` exceeds it, so a new straggler cannot be added in the same change as its entry. The exact `index.yaml` line, with N known, is shown to the owner before it lands. Alternative: a constant in the test. Rejected: equally editable and less visible. (codex, user)
16. **One binding-name extractor covers every Python binding form:**
    - function, class and argument names (positional, keyword-only, `*args`, `**kwargs`);
    - assignment, annotated (Pydantic fields included) and augmented targets, and attribute stores;
    - `for` and `async for`, `with … as`, `except … as`, comprehension targets, `:=`, and `match` captures;
    - `import … as` aliases (the alias only).

    (codex)
17. **After the path moves, every tracked file is swept for each old path fragment,** and each remaining hit is classified. Only `redirect_maps` and archived OpenSpec changes may keep one. This catches harness constants, shell lines and globs that link resolution cannot see. (codex)
18. **The guard has its own table-driven tests on synthetic source:** every binding form, tokenizing (snake case, camel case, plurals), the Pydantic `model_*` boundary, an unlisted hit, a stale entry, and the ceiling. (codex)
19. **Each edited notebook is re-executed in place** (`jupyter nbconvert --to notebook --execute --inplace`), and its outputs are checked. The notebook suite runs separately afterwards. (codex)
20. **`tests/test_docs_guards.py` gains link-integrity checks:** every relative link in `docs/**/*.md` resolves to an existing file (anchors ignored), and every `redirect_maps` source is absent while its destination exists. Links already broken today are fixed in this change. Alternative: rely only on the one-time link resolution and the old-path sweep. Rejected: a later page move would break links silently. (user)

## Risks / Trade-offs

- **A carve-out is respelled by mistake** (a physical column, an SQL join, a dbt or Cube object). → Per-hit judgement with the carve-out table. Message-pinning tests and the golden and corpus tests catch changed behaviour. The diff is reviewed per directory.
- **MCP descriptions grow past their budget** ("dataset" is longer than "model"). → `tests/test_mcp_surface_budget.py` runs in the local scope; reword to fit.
- **Respelled help topics are re-embedded on reseed.** → Expected; eight small documents.
- **Old docs links return 404 on docs.motley.ai** until DEV-2368 ships. → DEV-2368 must ship before the release that contains this change.
- **Notebooks fail on a stale jaffle DuckDB.** → Delete it and run the notebooks twice.

## Migration Plan

None. No stored data, wire key or behaviour changes. Rollback is a revert.
