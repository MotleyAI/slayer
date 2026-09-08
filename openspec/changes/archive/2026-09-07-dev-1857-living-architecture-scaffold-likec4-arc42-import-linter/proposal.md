# Proposal: dev-1857-living-architecture-scaffold-likec4-arc42-import-linter

## Why

SLayer's structure (layering between core/sql/engine/storage, package ownership, structural
conventions) lives only in heads and CLAUDE.md one-liners, so boundary violations accumulate
invisibly — e.g. `core` currently imports `engine`, `sql`, and `storage`. This change
initializes the living-architecture layer (LikeC4 model + arc42 principles + import-linter
contracts + cross-walk checker) so subsequent arch-slice issues can burn the tangles down
deterministically, with monotonic machine-checked progress.

## What Changes

- **basedpyright** (dev dep, basic mode) with a committed baseline (`.basedpyright/baseline.json`);
  gate = no new errors vs baseline.
- **import-linter** (dev dep) with two contracts encoding the TARGET layering, all current
  violations grandfathered as exact `ignore_imports` edges,
  `unmatched_ignore_imports_alerting = "error"`:
  - `layers`: `slayer.engine` | `slayer.sql` | `slayer.core` (15 grandfathered edges);
  - `forbidden`: `slayer.core` ↛ `slayer.storage` (2 grandfathered edges).
- **`architecture/`** scaffold: ONE LikeC4 model (4 precise nodes + 5 virtual buckets,
  relations = measured runtime node-level edges, dying edges tagged `#legacy`), landscape +
  query-pipeline views, `index.yaml` cross-walk (package claims, contract baselines,
  spec mapping), `system.arc42.md` (global principles promoted from CLAUDE.md, tagged
  `[enforced: …]`/`[review]`), `sql.arc42.md` (the one node file earning prose now).
- **`tools/arch_check.py`** — cross-walk checker: package claiming exactly-once, contracts and
  arc42 paths exist, bidirectional node↔LikeC4-element identity, spec mapping exactly-once,
  per-contract ignore count ≤ baseline, and a model-truth check (AST-measured runtime
  node-level import edges must exactly match the model's relations). Covered by a compact
  pytest module with negative-case fixtures.
- **DECISIONS.md** folded (still-true prescriptive structural rules → owning arc42 files,
  preceded by a categorized migration inventory) and deleted; history stays in the openspec
  archive + git.
- **No CI changes**; enforcement runs through the flow gates (spec-review, arch-slice) only.
- **Zero production code moves** — tooling + docs + model only.

## Capabilities

### New Capabilities

None — this change adds tooling, documentation, and an architecture model; no runtime
behaviour of SLayer changes. `skip_specs: true` is set in `.openspec.yaml`.

### Modified Capabilities

None.

## Impact

- `pyproject.toml`: dev deps (basedpyright, import-linter), `[tool.basedpyright]`,
  `[tool.importlinter]`; `poetry.lock` updated.
- New: `architecture/` (model, views, index.yaml, arc42 files), `tools/arch_check.py`,
  `tests/test_arch_check.py`, `.basedpyright/baseline.json`.
- Modified: `CLAUDE.md` (one pointer line to `architecture/` + enforcement bundle commands).
- Deleted: `DECISIONS.md` (after inventory + explicit user confirmation).
- No `.github/workflows` changes; no changes under `slayer/`.
