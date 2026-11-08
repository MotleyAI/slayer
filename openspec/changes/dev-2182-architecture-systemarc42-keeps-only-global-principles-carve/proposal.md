## Why

`architecture/system.arc42.md` §3 mixes genuinely global principles with rules that bind one node, and four concerns hide inside nodes that describe something else: live-database access inside "SQL generation", ingestion/drift inside the query pipeline, and the engine's row-supply and cardinality/association code on the bare `engine` node with no enforced arrows. The SLayer 2.0 slices that rewrite row supply (DEV-2212 slice 5) and cardinality (DEV-2216), and the 2.0 opener that edits ingestion (DEV-2231), need these boundaries enforced before they land.

## What Changes

- System §3 keeps only global principles (12 items, renumbered gaplessly, fixing the duplicate "18"); every node-specific principle moves to its node's arc42 file, including four new files (`database`, `live_schema`, `importers`, `protocols`) and a new Principles section and Law 7 in `semantics.arc42.md`.
- New top-level node `python.database` (`slayer/database/`): the SQL client and its contract, the engine factory, SQLite introspection, the client conformance check.
- New top-level node `python.live_schema` (`slayer/live_schema/`): ingestion, schema drift and scope, introspection helpers, broken-dependent checks, internal-table rules, FK-edge planning, storage-driven type refinement.
- New `engine` children `engine.population` (`slayer/engine/population/`) and `engine.cardinality` (`slayer/engine/cardinality/`).
- **BREAKING** (import paths only): every moved module's old path keeps a stub that raises `ImportError` naming the new path, tagged for removal in 3.0 through the removal tripwire; logger names follow the new module paths. Runtime behaviour is otherwise unchanged.
- Spec ownership: `sql/client-contract` and `sql/execution` move to a new `database` spec group owned by `python.database` (no text change); `models` adds `python.live_schema` to its touches.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `compatibility/removal-tripwire`: the one removal marker gains a raising form for module paths moved in 2.0; those paths are covered workarounds and fail with an `ImportError` naming their new location.

## Impact

- Code: ~20 modules move (pure moves, git rename detection preserved); stubs at every old path; `slayer/core/deprecation.py` gains the raising helper; `slayer/usage_reporting/recorder.py` maps the moved `SqlClientError` by its new qualified name.
- Architecture: `model.c4`, `views.c4`, `index.yaml`, every arc42 file; two new focus views.
- Docs: `docs/configuration/sql-clients.md`, `docs/concepts/ingestion.md`, `examples/seed.py`, `CLAUDE.md`.
- Tests: import rewrites, law-test path constants and logger-name strings (mechanical); docstrings citing moved principle numbers or spec paths; new tests for the moved-path errors and the usage-reporting classification.
- Downstream issues DEV-2212, DEV-2216, DEV-2231 get pointers to the new locations at merge.
