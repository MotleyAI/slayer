## 1. Base

- [x] 1.1 Before writing tests, merge DEV-1607's branch (`origin/egor/dev-1607-ossie-shapes-on-top-of-the-vocabulary-rename`, which contains `main` with DEV-2318) into this branch with a merge commit whose first line quotes the merged ref; verify `DatasetField`, `SlayerDataset` and the string metric `expression` exist and `openspec validate --strict` still passes against the merged corpus.
- [ ] 1.2 Renumber this change's `SlayerModel` version step to follow the latest one on the merged base; verify `CURRENT_VERSIONS` and the migration tests agree.

## 2. Tests first (pr-tests)

- [x] 2.1 Write `tests/test_dev2175_definition_names.py` (the arc42 P3 enforcement file) covering every scenario of `models/field-expressions` on in-process SQLite and DuckDB; verify every test fails for the right reason before implementation.
- [x] 2.2 Write tests for each MODIFIED/ADDED requirement in `models/column-definitions`, `models/column-filters`, `models/model-filters`, `models/join-keys`, `models/stored-upgrade`, `compatibility/removal-tripwire`; include direct `model_validate`, bulk and missing-peer loads, a missing datasource config, `DatasetField(sql=…)` in Python, and a field whose dialect entry and portable `sql` deliberately differ; verify they fail before implementation.
- [x] 2.3 Write importer tests for each MODIFIED/ADDED requirement in `importers/meaning-preservation`, `cube-import`, `dbt-import`, `osi-import`, `osi-export`, including helper reuse and idempotent re-import, quoted and mixed-case physical names, export/re-import values for derived, filtered, joined, quoted and dialect-only fields; verify they fail before implementation.
- [x] 2.4 Remove the strict-xfail markers from the 33 tests tied to this change (`tests/_dev2171_fixtures.py::MASKED` users, `tests/test_osi_converter.py` `SELF_NAMED`, `tests/test_dbt_converter.py::TestJoinKeyColumns`, `tests/test_osi_source_columns.py::test_source_column_behind_a_renamed_field_reads_the_physical_column`), respelling their inputs only; verify they fail before implementation.
- [x] 2.5 Apply the six approved test alignments of design decision 30 exactly; no other existing test's assertions change without the owner's consent.

## 3. Field model and conversion

- [ ] 3.1 Add Ossie's expression object (`sql`, `dialects`) to fields and metrics, with the string shorthand stored as written and the field-without-expression default; verify the shape tests of 2.1 pass.
- [ ] 3.2 Make field `sql` a deprecated input: the pure registry step (version stamp; bare or quoted identifier → shorthand; other text kept as deprecated `sql`), fresh-input warnings through `deprecated_input`, refusal of `sql` beside `expression`; add field `sql` to the removal tripwire's covered forms; verify the conversion and tripwire tests pass.
- [ ] 3.3 Add the load-pipeline refinement with datasource and peer context (sibling qualification by today's resolver, concrete dialect token, version-gated `source.` path migration, unloadable on a non-unique target), ordered after the join-key repairs and inside `stored_document_boundary`; the same conversion runs at save and at query time for unresolved fields; verify the stored-upgrade tests, including the 0.10.2 and 1.8.2 fixtures, return their previous results.

## 4. Engine

- [ ] 4.1 Select the running form per datasource (dialect entry, else `sql`, else an `OSSIE_SQL_2026` entry), with case-insensitive tokens, duplicate refusal, the no-runnable-form refusal and the native metric refusal, each run wherever the dialect is known; verify the dialect-selection tests pass.
- [ ] 4.2 Bind portable `sql` through `core.syntax` with source columns as leaves (bare and `source.x`) and dotted paths as fields; type source columns by declared `datatype`, else live type, else `Opaque`; verify the Mode-B field tests pass on SQLite and DuckDB.
- [ ] 4.3 Replace the sibling-first rule in the native walker with the row-definition rule (lexical aliases, self-qualifier, hop, else opaque) for dialect entries, `filter` and dataset `filters`; remove `physical_read_exempt`; verify the native-expression and filter tests pass.
- [ ] 4.4 Build one dependency graph from both extractors and use it for save-time validation and query-time expansion; verify cycle, fanning, ambiguity, self-named and self-cycle tests pass in both forms.
- [ ] 4.5 Define the physical-reader predicate (bare source column, no `filter`) and use it for `physical_name`, join keys, drift and type refinement; verify the join-key and drift tests pass.
- [ ] 4.6 Evaluate metric `expression` objects (only `sql` or `OSSIE_SQL_2026`) with `source.x` as a source column unless a field is named `source`; verify the metric tests pass.

## 5. Importers, ingestion, exporter

- [ ] 5.1 Remove only the masking outcomes from `slayer/importers/physical.py` and every caller's drop path; keep the absent, ambiguous and undecidable-case verdicts; verify their existing tests still pass.
- [ ] 5.2 Cube and dbt converters: write native dialect entries or the shorthand; read `{other}.x` and physical join keys through a physical reader or the shared deterministic helper; verify the Cube and dbt tests, including the former xfails, pass.
- [ ] 5.3 Ossie importer: store expression objects verbatim, translate only when no form can run (reported), drop a metric's datasource-dialect entry when `sql` exists, store metric `source.x` as written; delimited names stay skipped; verify the Ossie import tests pass.
- [ ] 5.4 Ingestion writes `expression`; verify ingestion and re-ingest tests pass.
- [ ] 5.5 Ossie exporter: export stored forms with the real token; inline field references per layer with each `filter` applied once; never label native text as portable; report unrepresentable definitions; write the `SLAYER` payload at version 3 with a field's definition under `expression`, and read versions 1–3 (field `sql` converting with the import's datasource dialect); verify the export, payload-version and round-trip tests pass.

## 6. Suite respelling

- [ ] 6.1 Value pass: a script built on the production conversion rewrites test field `sql` values that need qualification or a dialect entry, in Python sources and YAML fixtures; re-run each touched test; rewrite test reads of a field's `.sql` (outside the deprecated-spelling tests) to read its stored `expression` (shorthand string or object), keeping each assertion's meaning; list for the owner any test whose expected value would change and wait for consent before touching it.
- [ ] 6.2 Keyword pass: once DEV-2295 and DEV-2292 are released, bump the la pin and respell `sql=` and the field-level dict keys to `expression` with `la:deterministic-refactor` (dry-run, review, apply, full gate); verify no field-level `sql=` remains outside the deprecated-spelling tests and the gate is green.

## 7. Architecture and docs

- [ ] 7.1 Apply the approved `architecture/semantics.arc42.md` §4 P1–P3 wording verbatim (design decision 27); verify `la-arch-check` passes.
- [ ] 7.2 Update `docs/concepts/models.md`, `formulas.md`, `references.md`, the Cube, dbt and Ossie import/export pages, `docs/upgrading/2.0.md`, MCP and REST field descriptions and the `help.*` memories; re-execute every touched notebook; verify the docs build and `zensical.toml` nav.
- [x] 7.3 Post on DEV-2244 that delimited Ossie names (DEV-2175's 2026-11-12 comment) moved to its item 7; verify the comment is on the issue.

## 8. Finish

- [ ] 8.1 Run the local test scope (`la-affected-tests`), `ruff`, `basedpyright` against its baseline and the enforcement bundle (`la-arch-check`, LikeC4 validation, `la-typecheck`); verify all green.
