## 1. Tests first (pr-tests)

- [x] 1.1 Ask for the owner's approval to align these existing tests (name each file and change), then align them:
  - `tests/test_dev2073_join_edge_identity.py::TestSameEdge`: name, join type and cardinality no longer split an edge;
  - `tests/test_dev1842_query_backed.py` (~line 240) and `tests/test_dev1966_query_backed_inline.py` (~line 322): the measure rejection is lifted;
  - `tests/test_dev1922_source_model_union.py`: the typed extension in `source_model` becomes a conversion into `extensions`.
- [x] 1.2 Mechanically respell the legacy `source_model=ModelExtension(...)` / `{"source_name": ...}` construction in the existing test files and fixtures (about 45, found with `grep -rlE "ModelExtension" tests`) to `source_model=<name>` plus `extensions=[...]`, with no logic change. Verify that `grep` finds only the dedicated deprecation and migration tests still using the legacy form.
- [x] 1.3 `tests/test_dev2102_extensions_ir.py`: the IR, duplicate-extension rejection, unknown keys, `QueryRefinement` without `extensions`, the JSON schema (`source_model` = string | `SlayerModel` | null; `extensions`), REST 422, MCP input schema, and dump round-trip. Must fail before implementation.
- [x] 1.4 `tests/test_dev2102_legacy_conversion.py`:
  - fresh legacy input converts and warns (removal 3.0);
  - a Python instance warns;
  - legacy form plus `extensions` for the same model conflicts;
  - current-version legacy form is rejected;
  - profiling and the facade emit no deprecation warning.
- [x] 1.5 `tests/test_dev2102_stored_upgrade.py`: the v10 → v11 migration (v9 documents pass through the colon upgrade first) over a model's `source_queries`, a saved query, a current-version `Memory` holding a v9 query, and an unversioned legacy memory. No warning, extension intact, idempotent, recorded rows unchanged.
- [x] 1.6 `tests/test_dev2102_edge_merge.py`, executed on SQLite and DuckDB. Every `models/join-traversal` scenario:
  - reversed declaration, type not splitting, alias canonicalisation, alias over parallel edges, equal-pair ambiguity;
  - named-pair mismatch, cardinality conflict, alias stealing another edge's name, two names for one relationship;
  - multi-hop and cyclic query-local chains, stage shadowing.
- [x] 1.7 `tests/test_dev2102_query_inner_joins.py` (the Axiom 14 tag's test), executed on SQLite and DuckDB:
  - INNER on the root;
  - INNER over a stored LEFT edge;
  - fanning path (values plus the semi-join correlation);
  - multi-hop over a LEFT hop;
  - invariance when measures are added;
  - stage target and query-backed target with a flattened key;
  - target model filters;
  - forward and reverse composite keys with partial NULLs;
  - adjacent `M` with a second indirect route, adjacent named parallel edges, one safe and one fanning parallel edge;
  - unreachable and ambiguous errors naming the declaration;
  - LEFT never restricting, a reused stored INNER edge, rootless inference, raw-row mode.
- [x] 1.8 `tests/test_dev2102_scope_and_targets.py`: extensions of joined models (dimension and measure); scope invisibility to the next stage and to stored definitions; resolution order; unknown target; `time_spine` rejected; query-backed measures in a non-root stage; save-time refusal; usage-reporting features for root and non-root extensions.
- [x] 1.9 Run `poetry run pytest -m "not integration"` and confirm the new tests fail for the expected reasons, and nothing else regresses from the respelling.

## 2. IR, migration, deprecation

- [x] 2.1 `slayer/core/query.py`:
  - `SlayerQuery.extensions` with a `Field` description;
  - `SourceSpec` reduced to name | `SlayerModel`;
  - duplicate-extension validator;
  - the current-version legacy rejection;
  - `source_model_name` simplified.

  Verify with test 1.3.
- [x] 2.2 `slayer/storage/migrations.py`: `CURRENT_VERSIONS["SlayerQuery"] = 11` and a v10 migration hoisting the legacy root extension (one shared conversion). Freshness detection plus `warn_deprecated(..., removal="3.0")` at `_migrate_and_rewrite`. Verify with tests 1.4 and 1.5.
- [x] 2.3 Every stored-dict walker in `slayer/storage/*` that reads a query's `source_model` extension also walks `extensions` (granularity rename, rank direction, stored repair, sql-string, time-dimension text, distinct column, `base.py`). Verify with test 1.5 and the existing migration suites.

## 3. Engine

- [x] 3.1 Replace `same_edge` / `added_joins` (`slayer/core/join_edges.py`, `slayer/ir/source_bundle.py`) with the orientation-aware edge-merge function and its typed errors. Verify with test 1.6.
- [x] 3.2 The per-stage alias map: carried by the stage scope / bundle and consulted by path-segment resolution, routing, canonical-token generation, schema drift and dependency analysis. Verify with the alias scenarios in 1.6.
- [x] 3.3 One overlay step per stage, plus the walker helper:
  - delete every `ModelExtension` branch on `source_model` in `bundle_builder`, `plan`, `model_reads`, `source_bundle` and `stage_ordering`;
  - load extension sources and targets in the bundle build;
  - apply extensions to stored, inline, sibling-stage and spliced query-backed models;
  - delete the query-backed measure rejection.

  Verify with tests 1.8 and 1.6 plus the existing multi-stage and query-backed suites.
- [x] 3.4 The INNER restriction: build the parsed filter node per INNER declaration, bind it through the ordinary reference binder, and feed it to the population and filter assembly. Errors name the declaration. Verify with test 1.7.
- [x] 3.5 Reject extensions of `time_spine`; refuse at save the stored stages whose extensions are guaranteed to fail. Verify with test 1.8.

## 4. Consumers and surfaces

- [x] 4.1 Port `slayer/live_schema/schema_drift.py`, `slayer/memories/resolver.py`, `slayer/inspect/model_render.py`, `slayer/usage_reporting/features`, `slayer/engine/profiling.py` and `slayer/facade/translator.py` to `extensions` through the walker helper. Verify with tests 1.4, 1.8 and the existing suites for each module.
- [x] 4.2 MCP / REST `Field` descriptions: `source_model` drops the extension form; `extensions` is described with one per-query join and column example. Verify with test 1.3 and `tests/test_mcp_server.py`.

## 5. Architecture, docs, finish

- [x] 5.1 Apply the approved Axiom 14 wording to `architecture/semantics.arc42.md` (design decision 6, verbatim). Verify with `poetry run la-arch-check`.
- [x] 5.2 Docs, one sentence per change, with the old spelling no longer documented:
  - `docs/concepts/queries.md`, `docs/reference/mcp.md`, `docs/getting-started/mcp.md`, `docs/concepts/ingestion.md`;
  - `docs/examples` 02, 06, 08, 13, 14 (`.md` files);
  - `slayer/memories/help_content/06_joins.md`.

  Verify with `tests/test_doc_examples.py` and `tests/test_docs_guards.py`.
- [x] 5.3 Re-execute every edited notebook with `jupyter nbconvert --to notebook --execute --inplace`, and verify the outputs are fresh.
- [ ] 5.4 Full unit suite `poetry run pytest -m "not integration"`, the integration suite with the CI invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the baseline), and the enforcement bundle (`la-arch-check`, `npx -y likec4@1.47.0 validate architecture`). All green.

## 6. One static query scope (review round 3)

- [x] 6.1 Failing tests first, one per round-3 CodeRabbit thread, each failing on `11a594ed`:
  - model reads: a sibling stage named like a stored model shadows it (no read of it, no duplicate-column error);
  - model reads: the chained-sibling path `s1 → s2 → customers.regions` records `regions`;
  - model reads: a sibling with an inferred source in a query-backed model without a datasource still records its extension joins' targets;
  - schema drift: an extension column on a sibling stage reading through an extension join is a dependency of the query-backed model.
- [x] 6.2 The shared scope in `slayer/engine/extensions.py` (decision 18): one core for overlay, edge merge and the INNER restriction path, strict for the engine and lenient for the static readers. `apply_query_extensions` and the INNER routing in `bind_inputs` call it. `models_as_extended` in `slayer/ir/source_bundle.py` is folded into it and deleted.
- [x] 6.3 `slayer/engine/model_reads.py` reads the scope: delete `_scope`, `_scoped_model`, `_stage_host`, `_stage_hosts`, `_stage_data_sources`, `extension_definitions`, the `peers()` override and the per-query scoping in `unhosted_ref`.
- [x] 6.4 `slayer/live_schema/schema_drift.py` reads the scope: delete `_build_stage_graph`, `_StageGraph`, `_stage_cascade_state`'s own overlay, `_extension_fields`, `_inner_restriction_refs` and the stage-local hop walking in `_resolved_hops` / `_ref_edges`.
- [x] 6.5 `slayer/memories/resolver.py` reads the scope: delete `_extended_models`, `_query_scoped` and the `query_models` threading through `resolve_entity` → `_resolve_reference` → `_resolve_dotted_against_model` → `_resolve_join_path`.
- [x] 6.6 Verify: 6.1, every `tests/test_dev2102_*.py`, and the existing model-reads, schema-drift and memory suites pass; queries without extensions keep their results in all three readers, except that model reads now follows short-form routes and product reads, and schema drift and memory extraction resolve inline sources and short-form routes as the engine does. After the push, reply on each round-3 thread with the commit and its test.
- [x] 6.7 `tests/test_dev2102_static_reads_harness.py`, executed on SQLite: one parametrized corpus of query shapes, each planned without executing — extensions of root, joined, sibling and query-backed models; chained siblings; short-form routes; product reads; inline sources; INNER joins; stored named and parallel edges. For every shape, the access-filter read set covers each stored model the planned statement reads, and schema drift lists the query-backed model as a dependent of each non-key stored column the statement reads. Fix every divergence it finds; DEV-2265 replaces the static scope with the plan.
