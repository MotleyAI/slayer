# Proposal: Model-truth at every declared granularity; retire import-linter

## Why

The import law is split across two tools as a granularity workaround: `model-truth` enforces
the node-level whitelist while import-linter carries the module-granular grandfathered doors.
C4 models hierarchy natively, so the one whitelist law can apply at child level wherever
children are declared — one model, one checker, one law, and import-linter (plus its
`contracts-from-model` glue that would otherwise have to be built) retires entirely.

## What Changes

- **BREAKING (tooling)**: `[tool.importlinter]` and the `import-linter` dev dependency are
  deleted; `lint-imports` leaves the enforcement bundle. The one import law is arch_check's
  extended `model-truth`.
- `model-truth` measures import edges at the finest *declared* granularity: modules attribute
  to declared children (longest dotted-path prefix) or else to their node; coverage semantics
  license each measured edge by the most specific declared arrow; missing-arrow findings carry
  a module→module witness; dead (including fully-shadowed) arrows are findings.
- The 7 grandfathered door pairs become child-level `#legacy` arrows over newly declared
  children (`core.query`, `core.models`, `engine.syntax`, `sql.sql_predicate`,
  `sql.window_detect`, `storage.migrations`); node-level `core -> engine/sql/storage` legacy
  arrows are removed; sibling truth arrows (`core.query <-> core.models`,
  `sql.render -> sql.dialects`, `sql.sql_predicate -> sql.window_detect`) are declared, with
  `core.models -> core.query` tagged `#legacy`.
- The ratchet moves from per-contract `ignore_imports` counts to one declared-legacy-arrow
  count (`legacy_arrows: {baseline: 8}` in index.yaml); `contracts-known` and the pyproject
  TOML parsing leave arch_check; `root_package` moves to index.yaml.
- Diagrams render declared children as nested mermaid subgraphs with mixed-granularity
  arrows, governed by a per-view relative `view_depth` knob (default 3).
- CI (`ci.yml` lint-and-test) gains a `poetry run python tools/arch_check.py` step.
- Normative docs updated: system.arc42.md §3/§4/§5, ir.arc42.md and core.arc42.md tag/wording
  sweep (every `[enforced: layers]`/`[enforced: forbidden]` retagged), regenerated diagrams;
  `~/.claude` skills that name lint-imports updated out-of-repo.
- A permanent differential parity harness freezes the old law and proves the ban-set is
  preserved (exact on the cross-node domain at migration; monotone-safe as doors retire).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — this change alters enforcement tooling and normative architecture docs only; no
SLayer runtime behaviour changes. (`skip_specs: true`.)

## Impact

- `tools/arch_check.py`, `tools/arch_diagrams.py` (measurement, coverage, parser FQNs, views,
  mermaid subgraphs)
- `architecture/model/slayer.c4`, `architecture/views.c4` (unchanged), `architecture/index.yaml`,
  `architecture/{system,core,ir,engine,sql}.arc42.md`
- `pyproject.toml` + `poetry.lock` (import-linter removal), `.github/workflows/ci.yml`
- `tests/test_arch_check.py`, `tests/test_arch_diagrams.py` (fixtures rewritten, consented),
  new `tests/test_import_law_parity.py`
- Out-of-repo: `~/.claude/skills/{living-architecture,arch-slice,spec-review,spec-plan}`,
  deterministic-refactor docs sweep
