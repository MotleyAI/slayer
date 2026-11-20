## 0. Calibration notes (read first)

- This branch fast-forwarded to the LOCAL `egor/dev-2277-qualified-references-every-name-is-dataset` tip `7a5fb8aa9` (83 commits ahead of its origin at planning time). Before each stage, merge the latest DEV-2277 branch (or `origin/main` once DEV-2277 lands) — merge, never rebase — and re-check the bare-name order (DEV-2277 design decision 14) and `mcp/tool-surface`, which DEV-2277 also modifies.
- Upstream apache/ossie was read at commit `94cf86bd8af44b74863ff40b76d388ab1b037549` (2026-11-20); pin the vendored corpus to the commit current when vendoring and record it in the provenance file.
- New test files are named `tests/test_dev2243_<topic>.py`; the semantics Principle 3 tag names `tests/test_dev2243_model_scoped_metrics.py`, so that file must exist.
- Every xfail is `strict=True` with the reason naming its issue (DEV-2212, DEV-2217, DEV-2282, DEV-2244).
- Tests executing SQL on in-process DuckDB/SQLite are unit tests — never mark them `integration`.
- `architecture/**` edits: only the approved semantics Principle 3 wording (design.md decision 12) may be applied without asking again; any `model.c4` / `index.yaml` change needs a fresh per-change approval.

## 1. Tests (pr-tests stage)

- [x] 1.1 Vendor the corpus under `tests/fixtures/ossie_corpus/` with `provenance.yaml` (upstream commit, file list): 4 example semantic models, `compliance/portability` m0–m8 plus `manifest.yaml` and `grader/`, `compliance/wide_table` (models, SQL, cases), `compliance/query/scott` model, upstream `validation/validate.py` for the oracle; verify the files are byte-identical to upstream at the pinned commit
- [x] 1.2 Extend `scripts/ossie_schema_drift.py` with the corpus file list and a harvest step that runs upstream `validation/tests` with a hook capturing every document a test asserts valid, writing `tests/fixtures/ossie_corpus/harvested/*.yaml`; commit the harvest output; verify `tests/test_ossie_schema_drift.py` covers the corpus list and the harvest writer
- [x] 1.3 `tests/test_dev2243_corpus.py`: every corpus document passes upstream `validate.py`, loads natively, compiles every metric (unevaluable entities raise their typed error), and round-trips byte-identically through store → load → save-unchanged → export; parametrized over a corpus root taken from an env var defaulting to the vendored snapshot (the weekly job points it at a fresh fetch)
- [x] 1.4 `tests/test_dev2243_wide_table_oracle.py`: every `compliance/wide_table` case on its fixture over in-process DuckDB; `a*` exact rows; row-rule cases strict-xfail(DEV-2212); `e*` strict-xfail(DEV-2217)
- [x] 1.5 `tests/test_dev2243_portability_grader.py`: vendored grader over m0–m8 through `slayer ossie-roundtrip` grades all `PORTABLE`
- [x] 1.6 `tests/test_dev2243_documents.py`: one test per scenario of `specs/models/semantic-model-documents` (storage binding, verbatim SQLite text, shape refusals, version, no introspection/write on load, decomposition incl. composite primary keys, relationships, model-scoped metrics, unevaluable entities, per-datasource namespace and dependency-aware quarantine, edit-through, text fidelity golden diffs, concurrent writes, fingerprint invalidation incl. external edits, delete cascades, access filtering incl. transitive metric pruning, unknown types, surfaces, search, converter-construct parity)
- [x] 1.7 `tests/test_dev2243_model_scoped_metrics.py`: one test per scenario of `specs/queries/model-scoped-metrics` (resolution order in binding, inference and `recommend_population`; 162.5 on Appendix B.1; inline equality; reference rules and save refusals; SLayer-only storage of metric-over-metric; AGG in all three positions and each refusal; inspect/summary)
- [x] 1.8 `tests/test_dev2243_import_export.py`: every ADDED/MODIFIED scenario of `specs/importers/osi-import` and `specs/importers/osi-export` and `specs/cli/commands` (incl. re-import of a 1.x-converted document, legacy split with introspected fields, merged export and split-back re-import, AGG in exported model-scoped metrics)
- [x] 1.9 `tests/test_dev2243_relationship_shapes.py`: scenarios of `specs/models/join-keys` and `specs/models/join-traversal` (computed keys on ON clause and semi-join spine, edge named like a dataset, ambiguous hop, self-relationship inert and refused when spelled, duplicate incident names)
- [x] 1.10 `tests/test_dev2243_drift_types.py`, `tests/test_dev2243_save_scope.py`, `tests/test_dev2243_custom_backend.py`, `tests/test_dev2243_mcp_surface.py`: scenarios of `specs/models/schema-drift`, `specs/models/save-validation`, `specs/storage/custom-backends`, `specs/mcp/tool-surface`
- [x] 1.11 Codex edge cases (design.md decision 33): case-only collisions and mixed-case references, reordered composite keys, a dataset named `source`, memory/stage references surviving re-import; verify each is a named test
- [x] 1.12 Ask the owner, naming each file, before aligning any existing test the plan contradicts (expected: `tests/test_osi_converter.py`, `tests/test_osi_*` import tests, `tests/test_cli_import_osi.py`, edge-name collision and self-relationship tests); verify every new test fails for the right reason before implementation

## 2. Core and storage

- [x] 2.1 Move `slayer/osi/slayer_extension.py` into `slayer/core` with `dr-refactor` (dry run, review, `--apply`), leaving a moved-module stub; verify `la-typecheck` and `la-arch-check` pass
- [x] 2.2 Strict Pydantic Ossie document model in `slayer/core` (Ossie shape, `extra="forbid"`, `0.3.0.dev0` only) plus native semantic validity checks mirroring upstream `validate.py`; verify corpus validity agreement tests pass
- [ ] 2.3 Move `ruamel-yaml` to runtime dependencies (`poetry`), and implement the round-trip loader/patcher that changes only edited nodes; verify the golden-diff tests pass
- [ ] 2.4 Storage hooks on `StorageBackend` with typed-unsupported defaults; YAML (`semantic_models/<ds>/<name>.yaml`, per-document flock) and SQLite (`semantic_models` table via `sqlite_conn`, `BEGIN IMMEDIATE` patch) implementations; `graph_fingerprint` covers documents; verify custom-backend and concurrency tests pass
- [ ] 2.5 Decomposition into datasets/relationships/model-scoped metrics, merged into `get_dataset`/`load_datasets`/`list_datasets`; per-datasource namespace with dependency-aware quarantine; unevaluable-entity load warnings; verify the decomposition and collision tests pass
- [ ] 2.6 Owner lookup routing every dataset mutation (decision 25) and the lossless SLayer → Ossie-core-or-`SLAYER` mapping (decision 26) built from the exporter's classification; verify edit-through tests pass
- [ ] 2.7 Delete cascades and broken-dependent warnings for document members and documents; save scope (decision 28); verify cascade and save-scope tests pass
- [ ] 2.8 Access filtering of documents with transitive model-scoped metric pruning; verify access tests pass
- [ ] 2.9 Lift the relationship refusals: edge-name/dataset-name namespace, self-relationships (inert, refused when spelled), computed keys (emission at ON, semi-join spine, profiling); ambiguous-hop refusal; verify relationship-shape tests pass

## 3. Engine and queries

- [ ] 3.1 Model-scoped metrics in the source bundle; bare-name order alias → model-scoped → legacy → unknown in binding, population inference and `recommend_population`; verify resolution tests pass
- [ ] 3.2 In-place expansion with one component per aggregate; result key = bare name; save-time reference rules and refusals (reserved word, library function, dataset name, circularity); verify model-scoped tests pass
- [ ] 3.3 `AGG(d.m)` in `core.syntax` (the one Mode B reader) and binding, in queries, model-scoped metrics and dataset metrics, with every refusal; verify AGG tests pass
- [ ] 3.4 Unknown datatype: explicit unknown on load; engine probe off the event loop, cached per engine + fingerprint; deferred save-time checks; verify unknown-type tests pass
- [ ] 3.5 Native parity for every construct the 1.x converter mapped (decision 34) and in-memory translation of expressions without a runnable form; verify parity tests pass against the converter's expected values before deleting the converter

## 4. Import, export, drift, surfaces

- [ ] 4.1 `import-osi`: verbatim native store after the validity check, earlier-shape upgrades, legacy split with introspected fields, re-import replacing 1.x-converted datasets; retire `slayer/osi/converter.py` (keep the legacy-upgrade introspection); verify import tests pass
- [ ] 4.2 `export-osi`: merged document per datasource, verbatim single-document case, `SLAYER` boundary entry, `AGG(…)` for model-scoped references; `slayer ossie-roundtrip`; verify export and grader tests pass
- [ ] 4.3 `validate-datasets` untyped finding on all surfaces and CLI `--force-clean` write-in; verify drift-type tests pass
- [ ] 4.4 MCP tools, `inspect`/`datasets_summary`, REST `/semantic-models`, Python client (sync + async), CLI `semantic-models`; query-tool guidance and a help topic; search indexing of documents and model-scoped metrics; verify surface and search tests pass

## 5. CI, docs, architecture, tracker

- [ ] 5.1 Add the dedicated weekly-only job (schedule + `workflow_dispatch`, never on PRs) that fetches the corpus from apache/ossie `main`, runs the harvest, and runs the corpus, oracle and grader tests against the fresh copy; verify with a manual dispatch run
- [ ] 5.2 Docs: new `docs/concepts/semantic-models.md` in the `zensical.toml` nav; update datasets, storage/configuration, `docs/osi/osi_import.md` (incl. the stale `osi_datatype` line) and `osi_export.md`, `schema-drift.md`, MCP/CLI/REST/Python reference pages; `docs/upgrading/2.0.md` section; verify `tests/test_doc_examples.py` passes
- [ ] 5.3 Apply the approved semantics Principle 3 wording (design.md decision 12) verbatim; run `la-arch-diagrams` if needed; verify `la-arch-check` passes
- [ ] 5.4 Post tracker records: DEV-2217 (its strict run takes over all wide-table cases incl. `e*`), DEV-2220 (upstream grader registration as a release task, entry point `slayer ossie-roundtrip`, expected all-PORTABLE), DEV-2212 (the row-rule xfails), DEV-1661 (layout `semantic_models/<ds>/<name>.yaml` and the new storage hooks), DEV-2264 (import-osi's native path refuses invalid documents with every reason; closing it is the owner's call); verify each comment URL is recorded in the handoff comment
- [ ] 5.5 Enforcement bundle: `la-arch-check`, `npx -y likec4@1.47.0 validate architecture`, `la-typecheck` (no new errors vs baseline), `poetry run ruff check slayer/ tests/`; verify all green
