## Why

Every test runs against a source checkout, and both publish workflows upload whatever `poetry build`
produces without installing it, so a wheel with a missing module or data file, a broken `[all]` extra,
an unresolvable entry point, or a newly resolved dependency that breaks the CLI or MCP server can reach
PyPI unnoticed. Separately, the README's headline query lives in an image's alt text, which the
doc-example check never reads.

## What Changes

- The doc-example check also reads query literals from Markdown image alt text, with the same rules as
  code blocks, so the README headline query runs on the demo on every PR.
- The pure extraction half of `tests/_doc_examples.py` moves to `scripts/doc_examples.py`, shared by the
  doc-example check and the release gate.
- New `scripts/release_gate.py`: given a freshly installed `slayer` binary and the built wheel, it checks
  the install's provenance, then runs every checked README query through the CLI (after
  `slayer datasources create demo --ingest`) and through `slayer mcp --demo` over stdio, requiring rows
  and `orders` joins to `customers` and `stores`.
- New composite action `.github/actions/wheel-gate`: installs the one built wheel with `[all]` via
  `uv tool install` into an isolated tool directory (no lockfile), then runs the gate.
- `publish.yml` and `publish-test.yml` run the action between the build and the upload; a failing gate
  stops the upload.

## Capabilities

### New Capabilities

### Modified Capabilities
- `demo/doc-examples`: image alt text joins code blocks as a source of checked queries; a new requirement
  that a release uploads only a wheel that runs the README's queries on a fresh install.

## Impact

- `tests/_doc_examples.py`, `tests/test_doc_examples.py`, new `scripts/doc_examples.py`,
  `scripts/release_gate.py`, `tests/test_release_gate.py`.
- `.github/workflows/publish.yml`, `.github/workflows/publish-test.yml`, new
  `.github/actions/wheel-gate/action.yml`.
- `CONTRIBUTING.md` § Publishing.
- No `slayer.*` package change; no architecture model change.
