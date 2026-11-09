## 1. Harness: model, views, index (tests stage; each `.c4` / arc42 edit shown as an exact diff and approved first)

- [x] 1.1 `model.c4`: add top-level `database` ('Database access', `package 'slayer.database'`, `specs ['database']`) and `live_schema` ('Live schema', `package 'slayer.live_schema'`) nodes and `engine` children `population`, `cardinality`; derive every arrow from the post-move import graph (measure it with an AST script over the planned file mapping), none `#legacy`; verify the `#legacy` count is still 7
- [x] 1.2 `views.c4`: add `database_focus` and `live_schema_focus` (shaped like `sql_focus`); `index.yaml`: `models` touches += `python.live_schema`, `compatibility` touches += `python.sql`, `python.database`, `python.live_schema`, diagrams map += the two new arc42 files; verify with `npx likec4 validate architecture`
- [x] 1.3 Confirm `la-arch-check` is red only for the not-yet-existing packages / unmeasured arrows (the failing harness)

## 2. Harness: arc42 (tests stage; per-edit approval)

- [x] 2.1 `system.arc42.md`: §3 reduced to the 11 global items (design › Target), renumbered; §2 node enumeration replaced by the `model.c4` pointer; verify every item keeps its status tag
- [x] 2.2 `python.core.arc42.md`: P4 one key kind per meaning, P7 imports no other node, renumber, §1 drops its bottom-layer sentence
- [x] 2.3 `python.storage.arc42.md`: P3 versioned persistence (+3.1, 3.2); §1 drops `type_refinement.py`
- [x] 2.4 `python.sql.arc42.md`: remove §3.13–§3.15, renumber §3.16 → §3.13
- [x] 2.5 `semantics.arc42.md`: Law 7 "Measure closure"; new §4 Principles (1 dotted-canonical, 2 two expression layers + 2.1/2.2, 3 logical names); Notes → §5; §1 sentence; Axiom 2.4 reference
- [x] 2.6 New `python.database.arc42.md` (P1 one database door, P2 verbatim execution, P3 one engine owner, P4 one driver per client), `python.live_schema.arc42.md` (P1 ingestion idempotent/additive, P2 one treatment per model source), `python.importers.arc42.md` (P1), `python.protocols.arc42.md` (P1), each with the arc42-lite sections and `<!-- likec4:… -->` markers in the two node files
- [x] 2.7 `python.ir.arc42.md`: delete P2 (`ir` imports `core` only), renumber, §4 "(P1, P2)" → "(P1)"

## 3. Harness: tests (tests stage)

- [x] 3.1 New test: every moved module path and every tabled public name of `slayer.engine.population` / `slayer.engine.cardinality` raises `ImportError` naming its new location and SLayer 2.0; new paths and submodule imports succeed without warning (spec: compatibility/removal-tripwire › Moved module paths fail with their new location); red until the moves
- [x] 3.2 Tripwire: pinned set gains every stub site (removal 3.0) and the 2.0/3.0 scenarios (patched 2.0.0 names only 2.0 sites; patched 3.0.0 names every moved-path site); existing pinned entries take the moved file paths; the scan covers the raising helper
- [x] 3.3 New test: usage reporting classifies a real `SqlClientError` instance as `db_error` (green now; guards the qualified-name string through the move)
- [x] 3.4 Update test docstrings citing moved principle numbers or spec paths (wording only): `test_law_sql_built_as_ast`, `test_dev2187_one_reader_law`, `test_dev2187_root_qualifier`, `test_dev1892_home_rooting`, `test_library_growth`, `test_dev2171_importers_preserve_meaning`, `test_dev2172_*`, `test_dev1933_verbatim_execution`, `test_law_statement_timeout`, every `spec: sql/client-contract|execution`; and the `slayer/` docstrings citing moved principle numbers (`generator.py`, `storage/base.py`, `ingestion.py`, `statement_reads.py`)

## 4. Moves (implement stage; pure-move commits)

- [x] 4.1 Create `slayer/database/`, `slayer/live_schema/`, `slayer/engine/population/`, `slayer/engine/cardinality/` with docstring-only `__init__.py`; move the modules per design › Code moves via `la:deterministic-refactor` (tests in scope); commit the pure moves alone; verify `la-typecheck` no new errors
- [x] 4.2 `git mv openspec/specs/sql/{client-contract,execution}` → `openspec/specs/database/`; verify `openspec validate --specs --strict`
- [x] 4.3 String references: grep every moved dotted and file path (`patch("…")`, `caplog` logger names, law-test path constants, `importlib`, `getattr`, `__all__`, entry points, docs) and update; `slayer/usage_reporting/recorder.py` maps the new `SqlClientError` path; verify 3.3 stays green

## 5. Stubs (implement stage)

- [x] 5.1 `slayer/core/deprecation.py`: the raising helper (string-literal `removal`, `NoReturn`, `ImportError` naming old path, new path, SLayer 2.0)
- [x] 5.2 A stub at every old module path; `__getattr__` tables in `slayer/engine/population/__init__.py` and `slayer/engine/cardinality/__init__.py`; verify 3.1 and 3.2 green

## 6. Docs and pointers (implement stage)

- [x] 6.1 `docs/configuration/sql-clients.md`, `docs/concepts/ingestion.md`, `examples/seed.py` new paths; `CLAUDE.md` principle list and pointers match the new system §3; grep docs for every old path
- [x] 6.2 Root-fix the 5 baselined basedpyright errors in moved files; verify `poetry run basedpyright` shows no new errors and the baseline only shrank
- [x] 6.3 `la-arch-diagrams`; verify `la-arch-check` green and the legacy baseline still 7
- [x] 6.4 `slayer.engine.statement_reads` imports first in a fresh process: `StatementReads.touched_by` becomes `schema_drift.touches(reads, entry)`, so `statement_reads` imports nothing from `live_schema`; the fresh-import test covers it

## 7. Move gate (implement stage)

- [x] 7.1 `la-typecheck` no new errors; `la-arch-check` green; `npx likec4 validate architecture` green
- [x] 7.2 `poetry run pytest -m "not integration"` green; `poetry run pytest --collect-only tests/` collects with no import errors; a fresh `python -c "import <mod>"` for every moved module succeeds
- [x] 7.3 `poetry run ruff check slayer/ tests/` clean; `openspec validate dev-2182-architecture-systemarc42-keeps-only-global-principles-carve --strict` green

## 8. Tracker (review stage, at merge)

- [x] 8.1 Pointer comments: DEV-2212 (system Principle 8 is now semantics Law 7; `slayer/engine/population/`), DEV-2216 (`slayer/engine/cardinality/`), DEV-2231 (`slayer/live_schema/`, tripwire paths)
