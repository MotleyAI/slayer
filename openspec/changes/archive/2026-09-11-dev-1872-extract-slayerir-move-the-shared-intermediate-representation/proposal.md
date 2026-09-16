# Extract slayer/ir — move the shared intermediate representation out of engine

## Why

`slayer/sql` still imports representation types from `slayer/engine` through 9
grandfathered `ignore_imports` edges — the renderer depends on the planner's
internals. Extracting those types into a neutral `slayer/ir` package kills the
edges, realises the target layering `engine | sql | ir | core` anticipated by
the living-architecture scaffold, and creates the package the typed algebra
(DEV-1871) is built inside. Wave-1 semantic branches have landed, so move
churn is now minimal.

## What Changes

Pure structural move — zero behaviour change (full suite green, golden SQL
byte-identical). All Python moves via rope-based deterministic refactor
(`dr-refactor`, dry-run reviewed then `--apply`); no re-export shims; every
importer repointed to the one real home.

- New package `slayer/ir` (minimal `__init__.py`; import from submodules):
  - `ir/planned.py` ← `engine/planned.py` wholesale, plus
    `regroup_producer_identity` + `_structural_fingerprint` extracted from
    `engine/stage_planner.py` (pure identity functions over planned types).
  - `ir/bound.py` ← `BoundExpr`, `BoundFilter` out of `engine/binding.py`;
    `planned.py`'s `BoundExpr` re-export dies. The binder stays in engine.
  - `ir/source_bundle.py` ← the representation half of
    `engine/source_bundle.py`: `ResolvedSourceBundle` + pure helpers
    (`stage_bundle_with_siblings`, `synthetic_model_from_stage_schema`, and
    the pure private helpers they need). The async storage-backed builders
    (`build_resolved_source_bundle`, `_ModelReadCache`, `_resolve_source_spec`,
    `_collect_referenced_models`, `expand_query_backed_models_in_bundle`) stay
    in engine as `engine/bundle_builder.py`.
  - `ir/variables.py` ← `engine/variables.py` wholesale.
  - `ir/grain.py` ← `core/grain.py` (algebra vocabulary for DEV-1871).
- `walk_value_keys` moves `engine/binding.py` → `core/keys.py` (generic walker
  over the `ValueKey` `children()` traversal protocol).
- `engine/timing.py` → `core/timing.py` (execution metadata, not IR).
- `engine/column_expansion.py` → `sql/column_expansion.py` (Mode-A sqlglot
  analysis; its `sql.reserved_keywords` import becomes sql-internal; engine
  consumers import sql — the sanctioned direction).
- `engine/syntax.py` deliberately does NOT move (avoids re-pointing the
  grandfathered `core.query → engine.syntax` edge).
- Layers contract becomes `["slayer.engine", "slayer.sql", "slayer.ir",
  "slayer.core"]`; the 9 `sql → engine` `ignore_imports` entries are deleted;
  `layers` baseline 14 → 5.
- Architecture artifacts updated in the same PR (each `architecture/` edit
  individually approved per the normative-harness rule): `ir` node in the
  LikeC4 model + `index.yaml`, new `architecture/ir.arc42.md` (prose only, no
  embedded diagram), engine/sql/system arc42 updates, diagrams regenerated.
- basedpyright baseline: moved files' entries path-rewritten mechanically;
  zero unbaselined diagnostics; no wholesale regeneration.
- Test rewrites (approved): `tests/test_boundexpr_unification.py` asserts the
  single canonical `ir.bound.BoundExpr` home instead of the deleted re-export
  identity; `tests/test_dev1838_interning.py` / `test_dev1838_kernels.py`
  repoint `regroup_producer_identity` to `ir.planned`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — pure structural refactor; no spec-level behaviour changes
(`skip_specs: true`).

## Impact

- Affected code: `slayer/engine` (6 modules), `slayer/sql` (4 importers +
  gains `column_expansion`), new `slayer/ir`, `slayer/core` (gains `timing`,
  `walk_value_keys`; loses `grain`), `slayer/pg_facade`, `slayer/osi`, tests.
- Enforcement: pyproject import-linter contract, `architecture/index.yaml`
  baselines, LikeC4 model/views, arc42 docs, basedpyright baseline.
- No public API, storage, or wire-protocol change; no dependency changes.
- PR stacking: branch contains not-yet-merged DEV-1847; PR bases on
  `egor/dev-1847-…` until that lands, then retargets `main`.
