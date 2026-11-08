## Why

CONTRIBUTING.md never mentions living-architecture, which this repo already enforces in CI (`la-arch-check`) and uses for its spec-driven PR flow. A contributor learns about `la-arch-check`, the arc42 principles or OpenSpec only when CI fails. The guide is also stale: its unit-test command collects integration tests, its checklist skips basedpyright, its structure tree and "Making Changes" recipes name code that no longer exists, and its `ruff format --check` steps fail on 1034 unformatted files. Nothing checks the guide against the repo, so it drifts silently.

## What Changes

- Rewrite `CONTRIBUTING.md`:
  - Ask contributors to drive changes through `/la:pr`, using the pinned `la` plugin with commands from the Poetry env.
  - Point at `architecture/` as the normative map and principles; `.c4`/`.arc42.md` edits need maintainer sign-off.
  - Give outside contributors a git-ignored `living-architecture.local.yaml` override.
  - Put CI's gates, `la-arch-check` included, in the verify block and the PR checklist.
  - Drop the structure tree, the "Making Changes" recipes and every `ruff format` instruction.
  - Absorb the scope-closure and publishing sections from `docs/development.md`.
- Turn `docs/development.md` into a pointer to CONTRIBUTING.md. It keeps its title, URL and nav entry.
- Correct `docs/database-support.md` → "Adding a new dialect" to match the `SqlDialect` registry.
- Remove the dead `ruff-format` hook from `.pre-commit-config.yaml`; DEV-2079 restores it after a repo-wide reformat.
- Git-ignore `living-architecture.local.yaml`.
- Bump the `living-architecture` pin to the first release containing DEV-2078, which adds the local override.
- Add `tests/test_contributing_guide.py`, a drift guard over the guide's paths, CI gates, plugin pin and the docs pointer.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. This is a docs and tooling change with no spec-level behaviour (`skip_specs: true`).

## Impact

- Docs: `CONTRIBUTING.md`, `docs/development.md`, `docs/database-support.md`.
- Tooling: `.pre-commit-config.yaml`, `.gitignore`, `pyproject.toml` and `poetry.lock` (the living-architecture pin).
- Tests: a new unit test, `tests/test_contributing_guide.py`.
- Dependencies:
  - Implementation is blocked on DEV-2078 shipping in a living-architecture release.
  - It reconciles with DEV-2079 (the ruff format adoption), whichever lands first.
- No architecture (`.c4`/`.arc42.md`) edits.
