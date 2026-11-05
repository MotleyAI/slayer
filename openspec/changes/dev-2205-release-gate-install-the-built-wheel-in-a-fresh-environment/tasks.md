## 1. Tests first (pr-tests stage)

- [ ] 1.1 `tests/test_doc_examples.py`: alt-text extraction (inline and reference-style images, nested
  brackets, multi-line alt text, escaped brackets, several images, non-query alt text, an ordinary
  `[link](…)` not scanned); an alt-text query is checked and named by page and line; an alt text that
  mentions `source_model` without a query fails unless marked; a marked image is exempt; `README.md`
  yields its headline alt-text query. Verify: they fail before task 2.
- [ ] 1.2 New `tests/test_release_gate.py`, invoking the gate as `python -m scripts.release_gate`: the
  real gate passes end-to-end (CLI and MCP legs) against the dev env's `slayer` binary, given a stand-in
  wheel whose metadata carries the dev version and a `--checkout` override pointing at a temp dir (the
  dev install is editable, so the real checkout root would fail provenance — which a separate test
  asserts); failure paths with a fake `slayer` script and a fake stdio MCP server —
  CLI query error, empty rows, `joins_to` missing `stores`, version mismatch, package or `sys.path`
  inside the checkout, MCP `isError`, MCP non-JSON / empty-list response, MCP start-up timeout, zero
  README queries — each exit non-zero with the `release gate: <check>:` line naming the check. Verify:
  they fail before task 3.
- [ ] 1.3 Workflow-structure test: in both `publish.yml` and `publish-test.yml` the wheel-gate step runs
  after "Build package" and before the upload step. Verify: fails before task 4.

## 2. Shared extractor with alt text

- [ ] 2.1 Move the extraction half of `tests/_doc_examples.py` (models, fence parser, literal scanners,
  `extract_queries`, `parse_page`, `load_page`, `in_scope_pages`, `REPO_ROOT`, `IN_SCOPE_GLOBS`) into
  `scripts/doc_examples.py` (Pydantic, no `slayer` import); `tests/_doc_examples.py` keeps
  `query_failure`, `reference_failure`, `page_failures` and imports the rest. Verify: the existing
  doc-example tests pass unchanged.
- [ ] 2.2 `parse_page` reads Markdown image alt text (balanced brackets after `![`) as block-like units
  with the code-block rules (illustrative marker, `not-a-query` comment, `source_model` guard). Verify:
  task 1.1 tests and `test_documented_query_returns_rows` (now including the README alt-text query) pass.

## 3. Gate script

- [ ] 3.1 `scripts/release_gate.py` (`python -m scripts.release_gate --slayer <bin> --wheel <path>`):
  clean child env, provenance checks (wheel-metadata version vs `slayer --version`, installed package
  and `sys.path` outside the checkout), README queries via `scripts.doc_examples` (zero fails), CLI leg,
  MCP stdio leg with payload validation, timeouts, one `release gate: <check>: <reason>` line per
  failure, non-zero exit. Pydantic only. Verify: task 1.2 tests pass.

## 4. Action and workflows

- [ ] 4.1 `.github/actions/wheel-gate/action.yml`: `astral-sh/setup-uv`, resolve exactly one
  `dist/*.whl` to an absolute path, `uv tool install --python <job python>
  "motley-slayer[all] @ file://<abs>"` into isolated `UV_TOOL_DIR` / `UV_TOOL_BIN_DIR` under
  `$RUNNER_TEMP`, then `poetry run python -m scripts.release_gate --slayer <bin>/slayer --wheel <abs>`.
  Verify: the action YAML parses and task 1.3 passes once wired in.
- [ ] 4.2 Call `./.github/actions/wheel-gate` in `publish.yml` and `publish-test.yml` between "Build
  package" and the upload. Verify: task 1.3 passes.

## 5. Docs and gates

- [ ] 5.1 One sentence in `CONTRIBUTING.md` § Publishing on the gate. Verify: page reads correctly.
- [ ] 5.2 Full unit suite (`poetry run pytest -m "not integration"`), `ruff check slayer/ tests/
  scripts/`, `basedpyright`, `la-arch-check`, `openspec validate --strict` all green.
