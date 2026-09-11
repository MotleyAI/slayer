# Tasks — extract slayer/ir

## 1. Red phase (spec-tests stage — the ratchet flip)

- [x] 1.1 Create `slayer/ir/__init__.py` (docstring only) and verify `python -c "import slayer.ir"` succeeds; add the new file to git
- [x] 1.2 Edit pyproject: `layers = ["slayer.engine", "slayer.sql", "slayer.ir", "slayer.core"]`, delete the 9 `sql → engine` `ignore_imports`; lower `architecture/index.yaml` `layers` baseline 14 → 5; verify `poetry run lint-imports` FAILS listing exactly the 9 edges and `poetry run pytest -m "not integration"` stays green

## 2. Moves (spec-implement; each via dr-refactor dry-run → review → --apply; gate per move: basedpyright zero unbaselined diagnostics after path-rewriting moved entries, dr-mock-lint)

- [x] 2.1 `move-module engine/timing.py → slayer/core`; verify dry-run diff repoints `sql/client.py` + `pg_facade/connection.py` and nothing else unexpected
- [x] 2.2 `move-module engine/variables.py → slayer/ir`; verify `source_bundle` + `query_engine` repointed
- [x] 2.3 Split `engine/source_bundle.py`: `move-symbol` the async builders (`build_resolved_source_bundle`, `_ModelReadCache`, `_resolve_source_spec`, `_collect_referenced_models`, `expand_query_backed_models_in_bundle`) to new `engine/bundle_builder.py`, then `move-module` the remaining representation module → `slayer/ir`; verify ir half imports only core + `ir.variables` and suite green
- [x] 2.4 `move-symbol` `BoundExpr`, `BoundFilter` from `engine/binding.py` → new `slayer/ir/bound.py`; delete `planned.py`'s `BoundExpr` re-export, repointing its importers to `ir.bound`; verify no references to the re-export remain
- [x] 2.5 `move-symbol` `walk_value_keys` from `engine/binding.py` → `slayer/core/keys.py`; verify sql.generator + tests repointed
- [x] 2.6 `move-symbol` `regroup_producer_identity` + `_structural_fingerprint` from `engine/stage_planner.py` → (to-be-moved) `engine/planned.py`; then `move-module engine/planned.py → slayer/ir`; verify `sql/generator.py`, `sql/render/order_terms.py`, engine planners repointed
- [x] 2.7 `move-module core/grain.py → slayer/ir`; verify `stage_planner` + `regroup_planner` + `tests/test_dev1867_grain.py` repointed
- [x] 2.8 `move-module engine/column_expansion.py → slayer/sql`; verify engine consumers (`binding`, `column_filter_paths`, `filter_reachability`, `column_dependency`, `query_engine`, `schema_drift`), `osi/converter`, and sql consumers repointed
- [x] 2.9 Approved test rewrites: `tests/test_boundexpr_unification.py` asserts the single `ir.bound.BoundExpr` home (assertions 2–4 kept); `tests/test_dev1838_interning.py` / `test_dev1838_kernels.py` only if an assertion literally names `stage_planner`; verify all three pass
- [x] 2.10 Post-move sweep: repo-wide grep over slayer/, tests/, docs/, .claude/ for `engine.planned`, `engine.source_bundle`, `engine.timing`, `engine.variables`, `engine.column_expansion`, `engine/planned`, `engine/source_bundle`, `engine/timing`, `engine/variables`, `engine/column_expansion`, `core.grain`, `core/grain` — verify zero stale references (docstrings included)

## 3. Architecture + enforcement (each architecture/ edit shown as an exact diff and individually approved before applying — normative-harness rule)

- [x] 3.1 Propose + apply `architecture/model/slayer.c4` edit: add `ir` node, delete `sql -> engine #legacy`, add measured edges; verify `tools/arch_check.py` model-truth green
- [x] 3.2 Propose + apply `architecture/views.c4` edit adding `ir` to `query_pipeline`; regenerate mermaid via `poetry run python tools/arch_diagrams.py`; verify diagrams-fresh green
- [x] 3.3 Propose + apply new `architecture/ir.arc42.md` (prose only, no embedded diagram: representation-only, imports core only, `__init__` stays empty, [enforced: layers]) + `index.yaml` `ir` node entry; add new file to git; verify arch_check green
- [x] 3.4 Propose + apply arc42 updates: `engine.arc42.md` (stage list / planned.py reference), `sql.arc42.md` (edges-die sentence → done), `system.arc42.md` P1 (`ir` now real); verify arch_check + `npx -y likec4@1.47.0 validate architecture` green
- [x] 3.5 `CLAUDE.md`: Layout section replaced by references into `architecture/` (James 2026-09-11 — duplicated content pruned repo-wide from CLAUDE.md); verify docs grep from 2.10 clean

## 4. Verification (exit gate)

- [x] 4.1 `poetry run lint-imports` green with zero `sql → engine` entries; `arch_check` green (baselines 5/2)
- [x] 4.2 `poetry run pytest tests/ -m "integration or not integration"` — full suite incl. integration (unavailable DBs skip; collection proves imports); golden SQL byte-identical (no golden file changes)
- [x] 4.3 `poetry run basedpyright` zero unbaselined diagnostics; baseline diff = path rewrites only; `poetry run ruff check slayer/ tests/` clean
- [ ] 4.4 Commit (new files added individually), push, open PR based on `egor/dev-1847-…` (or `main` if DEV-1847 landed) — with the standing go-ahead gate
