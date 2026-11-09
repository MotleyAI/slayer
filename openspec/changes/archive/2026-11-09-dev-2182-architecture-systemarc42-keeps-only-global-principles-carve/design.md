## Context

See proposal.md › Why. The import law is `la-arch-check` model-truth: `model.c4` relations equal the AST-measured runtime imports at every declared granularity; an edge between a child and its own parent's unmodelled modules needs no arrow. The `#legacy` baseline is 7 and no legacy arrow touches a moved module. Every `architecture/**/*.arc42.md` and `.c4` edit needs the user's per-change approval of the exact diff.

## Goals / Non-Goals

**Goals:**
- system §3 holds only principles binding the whole system; each node-specific principle lives in its node's arc42 file, status tags carried verbatim, every file renumbered gaplessly, every reference updated.
- Four new enforced boundaries, every arrow taken from the measured imports, none `#legacy`.
- Pure-move commits (git rename detection carries later edits across merges).

**Non-Goals:**
- Breaking the real `engine ↔ live_schema` runtime cycle (`SlayerQueryEngine` delegates ingest/validate/apply-drift and checks drift at query time; live-schema code plans through `query_engine.short_lived_engine`). Inverting it is a real code change, out of scope for a behaviour-preserving move.
- Re-export shims at old paths (stubs raise instead, see D-stubs).
- Any runtime behaviour change beyond import paths and logger names.

## Decisions

### Target: system §3 (11 items)

| New | Item | Old |
|---|---|---|
| 1 | Claims | 3 |
| 2 | Model truth | 4 |
| 3 | Ratchet | 5 |
| 4 | SQL as sqlglot AST | 6 |
| 5 | Async-first | 7 |
| 6 | Pydantic only | 12 |
| 7 | RLS fails closed | 15 |
| 8 | One reader of Mode B | 18 |
| 9 | One writer of result values | 19 |
| 10 | One typing | 22 |
| 11 | Fail at save time | duplicate 18 |

system §2: the node enumeration is replaced by a pointer to `model.c4`.

### Relocations

| Item | Destination |
|---|---|
| §3.1 target layering | dropped: the model's arrows are the import law |
| §3.2 `core` imports no other node | `python.core.arc42.md` P7 (bars approving any new `core →` arrow); core §1 drops its bottom-layer sentence |
| §3.13 one key kind per meaning | core P4 (old P4, P5 → P5, P6) |
| §3.11 versioned persistence (+11.1, 11.2) | `python.storage.arc42.md` P3 (+3.1, 3.2); storage §1 drops `type_refinement.py` |
| §3.8 measure invariance | `semantics.arc42.md` new Law 7 "Measure closure" (adding a measure never changes the result's rows or any other field's values); the "algebra lives in semantics" pointer is dropped |
| §3.9, §3.10 (+10.1, 10.2), §3.16 | `semantics.arc42.md` new §4 Principles, items 1–3 (2.1, 2.2); Notes → §5; §1 notes the file also holds reference/expression principles; Axiom 2.4 "system §3.9" → "Principle 1" |
| §3.14, §3.20 | new `python.live_schema.arc42.md` P1, P2 |
| §3.23 | new `python.database.arc42.md` P1 |
| sql §3.13, §3.14, §3.15 | database P2–P4; sql §3.16 → §3.13 |
| §3.17 | new `python.protocols.arc42.md` P1 (bucket) |
| §3.21 | new `python.importers.arc42.md` P1 (bucket) |

The exact wording of every arc42 edit is presented for approval in the tests stage.

### Code moves

| Node | Package | Members |
|---|---|---|
| `python.database` 'Database access' (top-level; `specs ['database']`) | `slayer/database/` | `sql/client.py`, `sql/engine_factory.py`, `sql/sqlite_introspect.py`, `sql/conformance.py` |
| `python.live_schema` 'Live schema' (top-level) | `slayer/live_schema/` | `engine/ingestion.py`, `engine/schema_drift.py`, `engine/schema_scope.py`, `engine/introspect_utils.py`, `engine/broken_dependents.py`, `engine/internal_tables.py`, `engine/fk_edges.py`, `storage/type_refinement.py` |
| `python.engine.population` | `slayer/engine/population/` | `engine/population.py` → `population/inference.py`, `engine/home.py`, `engine/dimension_routing.py`, `engine/filter_reachability.py` |
| `python.engine.cardinality` | `slayer/engine/cardinality/` | `engine/cardinality.py` → `cardinality/inference.py`, `engine/join_safety.py`, `engine/unrelated.py` |

`sql/dialects/drivers.py`, `sql/dialects/reflection.py` and `sql/type_probe.py` stay in `sql`. Arrows come from the measured imports after the moves; expected among them: `engine ↔ live_schema` (both directions), `database → sql` (+ children as measured), `engine.population → engine.cardinality` (one way), `engine.cardinality → engine.elaborate_env`, the term-interface children → `population` / `cardinality`, `storage → live_schema`. Two views `database_focus`, `live_schema_focus` (shaped like `sql_focus`) embed in the two new node files; bucket files get none.

### D-stubs: old paths raise a located `ImportError`

- `slayer/core/deprecation.py` gains a raising helper beside `warn_deprecated` (same string-literal `removal`), typed `NoReturn`, raising `ImportError` naming old path, new path and SLayer 2.0. The tripwire scans both helpers; `removal="3.0"`.
- Each old module path keeps a stub file whose only content is a docstring, the helper import and one call. A stub imports only `slayer.core`, so it adds no arrow.
- `slayer/engine/population/__init__.py` and `slayer/engine/cardinality/__init__.py` (the old module paths are now these packages) define a module `__getattr__` over an explicit table of the old module's public names: a tabled name raises through the helper; anything else raises the normal `AttributeError`, so submodule imports keep working. Python passes an `ImportError` from `__getattr__` through `from X import y` unchanged.
- The tripwire's pinned set gains every stub site; its existing entries for moved files take the new paths.

### Specs and index

- `git mv openspec/specs/sql/{client-contract,execution}` → `openspec/specs/database/` (no text change).
- `index.yaml`: `models` touches += `python.live_schema`; `compatibility` touches += `python.sql`, `python.database`, `python.live_schema`; diagrams map += the two new files with their views.

### Decisions log

1. Items 17–22 and the duplicate 18 (the issue predates them): 18, 19, 22 and the duplicate 18 stay global; 17 → protocols, 20 → live_schema, 21 → importers. Alternative: keep 17/20/21 global and skip the bucket arc42 files. (user)
2. §3.6 stays global: SQL is built outside `sql` (facade/flight info-schema and probes, pg facade, storage), and its law test scans all of `slayer/`. (default)
3. Database access is a top-level `python.database`, not a child `sql.database`: honest placement. (user)
4. `dialects/drivers.py`, `dialects/reflection.py`, `type_probe.py` stay in `sql`, contrary to the issue's "driver loading": the BigQuery/Snowflake dialects' build hooks import `drivers` (moving it makes a `dialects ↔ database` cycle and breaks sql P2), `reflection` types are shared with `render`, the generator builds probes. sql §3.15 moves with §3.13/§3.14 (an extension of the issue's list). (user)
5. Live schema is a top-level `python.live_schema`, not an `engine` child, with the membership above, including `fk_edges.py` (measured: only `ingestion` imports it; the issue listed it as a cardinality candidate) and `storage/type_refinement.py` (makes 20 node-local). The two-way `engine ↔ live_schema` pair is declared, not hidden; breaking it is a separate inversion. (user)
6. `unrelated.py` goes to `engine.cardinality`, not `engine.population` (the issue's candidate list), so the children's edge is one-way; grounded in semantics Axiom 3.1 ⊂ Axiom 3 Association. (user)
7. The old `population.py` / `cardinality.py` move to `<package>/inference.py`; alternatives `<pkg>/<pkg>.py` or logic in `__init__.py`. (default)
8. The relocated §3.9, §3.10, §3.16 form a new semantics §4 Principles rather than being labelled axioms or laws. (default; user did not object)
9. §3.8 becomes standalone semantics Law 7, not folded into Law 3: Law 3 is about a term's denotation, and DEV-2212 slice 5 plans measure monotonicity as its own law. (user)
10. `sql/client-contract` and `sql/execution` move to a `database` spec group; `models` stays cross-cutting with `python.live_schema` added to its touches. (user)
11. Harness first: the tests stage writes the target `model.c4` / `views.c4` / `index.yaml` and every arc42 edit, leaving `la-arch-check` red; the implement stage moves the code. (user)
12. The 5 basedpyright-baselined errors that move with their files (engine_factory 1, ingestion 2, schema_drift 2) are root-fixed, never re-baselined. (default)
13. Pure-move commits stay pure; compliance fixes, stubs and reference updates go in separate commits. (default; the issue requires pure moves)
14. No re-export shims; every old module path (all ~20) gets an `ImportError` stub naming the new path, kept until 3.0 through the removal tripwire. (user)
15. Logger names follow the modules (`__name__`); test strings pinning old logger names are updated. Alternative: pin the old names. (user)
16. "Behaviour-preserving" is stated as "apart from import paths and logger names". (codex)
17. `slayer/usage_reporting/recorder.py` maps `SqlClientError` by its new qualified name, guarded by a new test classifying a real `SqlClientError` as `db_error` (no test covered it). (codex)
18. The move gate adds `pytest --collect-only tests/` (integration included) and a fresh-process import of every moved module. (codex)
19. rejected (codex): "system §3 must keep only the issue's global list (drop 9–12)" — the user classified the newer items in decision 1.
20. rejected (codex): "add compatibility re-export shims" — replaced by decision 14.
21. Principles the model's arrows already express are deleted, not relocated: system §3.1 (target layering), ir P2 (`ir` imports `core` only), and system §2's node enumeration. The core ban (system §3.2) stays as core P7: it bars approving any new `core →` arrow, which the model alone cannot say. (user)
22. The fresh-import cycles found on main: `schema_drift`, `broken_dependents` and `statement_reads` are fixed here (module imports; `touched_by` moves into `schema_drift`); `rank_direction_migration` goes to its own issue (migration-registry split). (user)

## Risks / Trade-offs

- [String references no static tool sees: `patch("…")`, `caplog` logger names, law-test path constants, the tripwire's pinned paths, docs] → the move gate greps every moved dotted path and file path; stale `patch` targets fail loudly at runtime.
- [Circular-import ordering changes once `query_engine ↔ ingestion` crosses packages] → fresh-process import of every moved module plus the full suite.
- [A stub file is mistaken for a module by an AST-scanning law test] → stubs hold no SQL, no engine construction and no Mode-B reads; the full suite runs every law.
- [Downstream slices DEV-2212 / DEV-2216 / DEV-2231 cite old locations] → pointer comments on each at merge.
