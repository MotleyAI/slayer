## 1. Tests first (pr-tests stage)

- [x] 1.1 `tests/test_saved_query_refinement.py`: pure `refine_query` / `QueryRefinement` unit tests (no DB), one per merge rule and conflict in `specs/queries/saved-query-refinement` — union and collapse, source-prefix identity, full-field equality vs metadata conflict (named and unnamed measures, computed dimensions), time-dimension attribute merge incl. scalar-vs-list `date_range`, filters dedupe, order/limit/offset replace and clear, scalar override only when supplied, empty refinement round-trip equality, forbidden keys, functional `gran(col)` in `dimensions`; verify they fail before implementation
- [x] 1.2 `tests/test_saved_query_refinement_exec.py`: executed values on SQLite and DuckDB for every scenario of `specs/queries/saved-query-refinement` (seed and models in the spec header; issue §5 values), rows compared order-insensitively unless the merged query orders
- [x] 1.3 Population pinning tests (`specs/queries/population` ADDED requirement): pinned on save through `save_model`, `create_model_from_query` and `edit_model`; refinement keeps the pinned population; collision refused at save; legacy unpinned model refined vs unrefined
- [x] 1.4 Surface tests in the existing run-by-name files: `tests/test_api_server.py` (refine with name, 400 without name, flat fields incl. explicit null → 400 with the new message, conflict → 400, malformed refine → 422), MCP query-tool tests (refine arg, error with a dict/list query, response-side cap), `tests/test_cli.py` (`--refine` inline and `@file`, error with a JSON query), `tests/test_client.py` (HTTP body `{"name", "refine"}`, in-process forwarding, error with a non-str query)
- [x] 1.5 `tests/test_query_cache.py`: refined run caches separately; `evict(name, refine=...)` removes only it; a stale refined entry refreshes with its refinement
- [x] 1.6 Inspect tests: `saved_queries` in markdown and JSON, compact and full, and in skeleton-listing views; hidden excluded; `ModelExtension`-sourced stage included; listed once; never self; section include/exclude/unknown; search finds a saved query by description
- [x] 1.7 Update `tests/test_dev1858_mcp_query_tidy.py::UNIFIED_ARGS` to include `refine` (user-approved mechanical update)

## 2. Core

- [x] 2.1 Hoist the `Annotated` types of `measures`, `dimensions`, `order` in `slayer/core/query.py` to module-level aliases used by `SlayerQuery`; full suite still green
- [x] 2.2 Add `QueryRefinement` (`extra="forbid"`, shared aliases, `_rewrite_functional_granularity` before-validator, filter after-validator) and `refine_query` per design decisions 3–4; `RefinementConflictError` in `slayer/core/errors.py`; 1.1 passes

## 3. Engine

- [x] 3.1 `SavedQueryRun` internal input; `refine` keyword on `execute`, `execute_sync`, `evict`, `evict_sync`; non-str + refine → the §3 `ValueError`; merge in `_normalize_by_name` before variable layering; cache stores the packed value; 1.2 and 1.5 pass
- [x] 3.2 Save-time population pinning + collision refusal, and in-memory pinning for a refined run of an unpinned stored model; 1.3 passes

## 4. Surfaces

- [x] 4.1 REST `QueryRequest.refine` + route checks via `model_fields_set`; OpenAPI shows `QueryRefinement`; REST tests pass
- [x] 4.2 MCP `query(refine=...)` + docstring line and "Top-level arguments" entry; MCP tests pass
- [x] 4.3 CLI `--refine JSON|@file`; CLI tests pass
- [x] 4.4 Client `refine=None` on `query`, `query_sync`, `sql`, `explain`, `query_df` (and their sync twins); client tests pass
- [x] 4.5 Inspect reverse-index helper, `saved_queries` section registration, full/compact/skeleton rendering; 1.6 passes

## 5. Docs and architecture

- [ ] 5.1 Apply the approved `architecture/engine.arc42.md` P8 wording (pinning exception) and run `la-arch-check`
- [x] 5.2 `docs/concepts/queries.md` "Refining a saved query" subsection (rules in short, R1/R3/R5 and multi-stage examples, R6 key-rename note, population pinning); `docs/concepts/models.md` "Three ways to use a saved query"; one sentence each in the run-by-name reference/interface pages (REST, MCP, CLI, Python client); one sentence in `slayer/memories/help_content/01_models.md`

## 6. Verification

- [ ] 6.1 Full non-integration suite, `ruff check slayer/ tests/`, `basedpyright` (no baseline growth), `la-arch-check`, and `openspec validate dev-2001-refine-saved-queries-and-add-model-scoped-named-views --strict` all green
