## 1. Failing tests (pr-tests)

- [x] 1.1 Write the guard's own table-driven tests on synthetic source (design decisions 14, 16, 18). Cover every binding form in decision 16; tokenizing (snake case, camel case, plurals; `joined` and `columnar` are not hits); the Pydantic `model_*` boundary (`model_config` exempt, `model_cache` not); an unlisted hit; a stale `carve_outs` entry and a stale `stragglers` entry; and `len(stragglers)` above the `x-vocabulary-stragglers` ceiling. Verify that they fail because the extractor does not exist yet.
- [x] 1.2 Write `tests/test_vocabulary_identifier_guard.py`'s real-tree test: every old-word definition in `slayer/` is listed, no entry is stale, and the straggler count is within the ceiling read from `architecture/index.yaml`. Verify that it fails (there is no allowlist or ceiling yet).
- [x] 1.3 Add the link-integrity tests to `tests/test_docs_guards.py` (design decision 20): relative links in `docs/**/*.md` resolve (anchors ignored), and each `redirect_maps` source is absent while its destination exists. Verify that the redirect test fails (no `redirect_maps` yet). If the link test fails on today's tree, record the broken links in this task for 3.4.

## 2. Identifier guard

- [x] 2.1 Implement the binding-name extractor and the guard. The allowlist is parsed into a Pydantic model (`carve_outs: [{name, file, reason}]`, `stragglers: [{name, file}]`), with the reasons listed in decision 2. Verify that 1.1 passes.
- [x] 2.2 Build `tests/fixtures/vocabulary_identifier_allowlist.yaml` from the extractor's hits on `slayer/`. Judge each hit as a carve-out (with its reason) or a straggler, using DEV-2364's candidate list as a first cut. Show the owner the exact `x-vocabulary-stragglers: {baseline: N}` line for `architecture/index.yaml` and add it after the OK (decision 15). Verify that 1.2 passes and `la-arch-check` is green.
- [x] 2.3 Post a comment on DEV-2364 saying the guard's `stragglers` list (path and count) is now its authoritative work list.

## 3. Doc page and example path renames

- [x] 3.1 Move the seven paths in proposal.md. Rename `docs/examples/05_joins/joins_utils.py` → `05_relationships/relationships_utils.py` with `dr-refactor` so its imports follow. Leave inner `models/` storage folders as they are. Verify that the moved files exist and the old ones are gone.
- [x] 3.2 Rewrite references by resolving each one to its target (design decision 5): Markdown links in `docs/`, `README.md` and `examples/`; `zensical.toml` nav; notebook and test paths (the notebook harness, `tests/test_cube_import_example.py`, `tests/test_docs_dev_1560.py` and any other); `README.md`'s absolute docs URLs. Add a `redirect_maps` entry under `[project.plugins.redirects.redirect_maps]` in `zensical.toml` for each moved page. Verify that 1.3 passes.
- [x] 3.3 Sweep every tracked file for each old path fragment (`concepts/models`, `concepts/formulas`, `05_joins`, `05_joined_measures`, `16_list_columns`, `slayer_models`, `extra_models`) and classify every hit (design decision 17). Verify that only `redirect_maps` and archived OpenSpec changes keep one.
- [x] 3.4 Fix any link 1.3 found broken on today's tree. Verify that 1.3 passes.

## 4. Prose respell (per-hit judgement, never regex replace; design decision 6)

Gotcha: give every worker the carve-out table from the issue and design decisions 8, 9 and 12, and review each directory's diff before moving on.

- [ ] 4.1 `slayer/`: messages, warnings, errors, CLI help, docstrings, comments, MCP tool and field descriptions, server instructions, and help topics (`slayer/memories/help_content/`), including `slayer/otel/`. Gotcha: `tests/test_mcp_surface_budget.py` caps description lengths, so reword to fit. Verify that a sweep of `slayer/` prose finds only carve-outs.
- [ ] 4.2 `docs/` (including `docs/upgrading/2.0.md` Part 1's 2.0-behaviour sections; the before/after old spellings stay) and `examples/` Markdown, YAML comments and scripts. Verify that a sweep of both finds only carve-outs.
- [ ] 4.3 `tests/`: docstrings, comments, assertion messages, and old `--type` and tool-name strings in shared helpers (`tests/_law_harness.py`, `tests/test_law_grain_union.py`, …). Test function and class names, test locals, test identifiers and test-data literals stay (DEV-2364). Verify that a sweep of `tests/` finds only those exemptions and carve-outs.
- [ ] 4.4 Align every existing test that pins respelled message text to the new text (design decision 13; mechanical only, no logic change). Verify that the local test scope passes.

## 5. Notebooks

- [ ] 5.1 Re-execute every notebook edited in 3 or 4 in place (`poetry run jupyter nbconvert --to notebook --execute --inplace <nb>`), and check the refreshed outputs. Gotcha: delete the stale jaffle DuckDB first, and run the notebooks twice. Verify that each notebook ran with no errors and its outputs are saved.
- [ ] 5.2 Run `tests/integration/test_notebooks.py` with the CI invocation from `.github/workflows/ci.yml`. Verify that it passes.

## 6. Gate

- [ ] 6.1 Run the local test scope (`la-affected-tests --base main`, plus `test_mcp_surface_budget.py`, the docs guards, the notebook harness and every message-pinning test touched in 4.4) and fix every failure.
- [ ] 6.2 Verify that ruff, `la-typecheck` (no new errors, baseline not rewritten), `la-arch-check` and LikeC4 validation pass.
- [ ] 6.3 Run the issue's word-boundary sweep for `model`, `column`, `measure`, `join` and `formula` over `slayer/`, `tests/`, `docs/` and `examples/`. Verify that every hit is a carve-out, an upgrade path, a back-compat test, `tests/perf/compare/`, a guard straggler, a DEV-2364 test identifier or test-data literal, or a test function or class name or test local.
