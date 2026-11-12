## 1. Tests first (pr-tests stage)

- [x] 1.1 Vendor `tests/fixtures/osi/ossie-schema.json` from `open-semantic-interchange/OSI@89e2e17` with the source SHA recorded beside it; add `jsonschema` as an explicit dev dependency (`poetry lock` + `poetry install -E all` succeed)
- [x] 1.2 Add a current-spec fixture set under `tests/fixtures/osi/` (flat shape, top-level `sql`, `OSSIE_SQL_2026` entries, dataset-scoped metrics, a FILTER metric, renamed key fields, `datatype`s); keep `shop.yaml` as the legacy-coverage fixture; verify each current-spec fixture validates against the vendored schema
- [x] 1.3 Parser tests for every scenario of "Current-spec documents import", "Legacy wrapper documents keep importing" and "Invalid document shapes are skipped with a warning" (flat, legacy array, legacy object, mixed, empty/null/scalar wrapper) — failing before implementation
- [x] 1.4 Model tests for "Expressions carry an optional portable form…" (sql only, dialects only, both, neither, open token, blank token rejected)
- [x] 1.5 Converter tests for "Deterministic dialect selection" and "Unreadable expressions are reported, never raised" (every order step, document-order tie-break, duplicate-dialect info note, override readable/unreadable, `postgresql`/`postgres` alias, case-insensitive tokens)
- [x] 1.6 Converter tests for "Selected expressions are rendered for the target datasource" (IFF/ZEROIFNULL/DATEADD/STARTSWITH/`||` rendered per target incl. MySQL and T-SQL emission, target-native verbatim, quoted mixed-case identifiers, unparseable expression reported) — execution tests on in-process DuckDB/SQLite, emission tests for the rest
- [x] 1.7 Converter tests for "Current-spec models expose their declared fields" and "Field expressions are validated against the live source" (renamed field, derived over undeclared source column, no-fields dataset warning, typo reported)
- [x] 1.8 Converter tests for "Keys and relationships name declared fields" and "Primary keys" (renamed join keys executed end to end, source-column key/relationship reported, computed key fields reported (both sides), cross-document relationship reported, absent/empty/partly-unresolved/renamed/composite/valid primary keys, empty unique-key entry)
- [x] 1.9 Converter tests for "Metric name resolution", "Identifier case in metric references" and "Legacy documents keep lenient resolution" (unqualified model-scoped, source column, foreign dataset-scoped ref, case-insensitive, quoted exact, case-only ambiguity, legacy unqualified metric and legacy source-column relationship)
- [x] 1.10 Converter tests for "Dataset-scoped metrics become measures on their dataset" (measure on its model, executed value, field-name collision, duplicate metric names)
- [x] 1.11 Expression/converter tests for "FILTER aggregates" (SUM, AVG, MIN, MAX, COUNT(*), COUNT(x), COUNT(DISTINCT), percentile/median with FILTER, executed values incl. the NULL group, compound predicates, NULL checks, scalar calls and quoted identifiers in predicates; aggregate/window/subquery predicate, cross-dataset predicate and SUM(DISTINCT) reported)
- [x] 1.12 Expression/converter tests for "Aggregate operands" (arithmetic and CASE inline with no hidden column, cross-dataset operand executed, non-allowlisted function falls back to a probed hidden column, AND/OR/NOT, `=`/`<>`, NULL, booleans, casts, `||` operands)
- [x] 1.13 Converter tests for "Field datatypes", "Time-dimension role" and "Metric datatypes" (each mapping, probe-failure fallback, disagreement info entry, Time→UNKNOWN, role defaulted from datatype, explicit opt-out, Integer year keeps INT, metric datatype in meta with type unset)
- [x] 1.14 Drift-guard tests: every vendored current-spec fixture and `docs/examples/13_osi_import/shop.osi.yaml` validate against the vendored schema; legacy fixtures are asserted NOT to validate; schema-to-model coverage test (every schema property modelled or on an explicit ignore list)
- [x] 1.15 End-to-end test: `slayer import-osi` on a flat current-spec document into an in-process DuckDB datasource, models saved to storage, reloaded and queried (dataset-scoped metric, FILTER metric, renamed keys across a join, omitted source columns)
- [x] 1.16 CLI test: `--dialect` defaults to unset and an explicit `--dialect` is honoured first

## 2. Document models and parser

- [x] 2.1 Rework `slayer/osi/models.py`: flat document model, open-string dialect, `Expression.sql` + optional `dialects`, `datatype` on fields and metrics, `metrics` on datasets, key presence preserved; tests 1.4 pass
- [x] 2.2 Rework `slayer/osi/parser.py`: raw-mapping shape detection, legacy wrapper split into flagged flat documents, corrected version/stability docstrings (`KNOWN_OSI_VERSIONS` unchanged); tests 1.3 pass

## 3. Expression selection and normalization

- [x] 3.1 One selection function implementing the dialect order with document-order tie-break and report notes, using `dialect_for_ds_type` for the target; tests 1.5 pass
- [x] 3.2 One normalization path: parse once with the dialect's reader (Snowflake reader for `OSSIE_SQL_2026`/`ANSI_SQL`), render raw-SQL surfaces in the target dialect, target-native verbatim; every importer-side raw-SQL parse uses the target dialect; tests 1.6 pass

## 4. Resolution and models

- [x] 4.1 Per-document resolution scope in the converter (relationships, joins, metric anchors); duplicate dataset names across the import set still refused
- [x] 4.2 Current-spec field-only visibility with live-source validation of field references; legacy documents keep today's behaviour; tests 1.7 pass
- [x] 4.3 Key and relationship resolution against declared fields, computed/filtered key fields reported, PK presence vs emptiness, renamed-PK inheritance; tests 1.8 pass
- [x] 4.4 Metric reference resolution (qualification rules, identifier case, ambiguity, canonical emitted names); tests 1.9 pass

## 5. Metrics

- [x] 5.1 Dataset-scoped metrics converted onto their dataset's model; tests 1.10 pass
- [x] 5.2 SQL-AST → formula renderer for aggregate operands, validated by the canonical formula parser, hidden raw-SQL column (target dialect, probed type) only as fallback; tests 1.12 pass
- [x] 5.3 FILTER lowering to `agg(CASE WHEN p THEN x END)` / `count(CASE WHEN p THEN 1 END)` with row-level and same-dataset predicate checks; tests 1.11 pass

## 6. Datatypes

- [x] 6.1 Field datatype mapping, precedence, cross-check entry and meta; `is_time` role defaulting without retyping; default time dimension limited to DATE/TIMESTAMP; metric datatype in meta; tests 1.13 pass

## 7. CLI, drift workflow, docs

- [x] 7.1 `import-osi --dialect` defaults to unset, help text updated; tests 1.16 pass
- [x] 7.2 Add `.github/workflows/ossie-schema-drift.yml` (weekly + `workflow_dispatch`, never on PRs): fetch upstream `core-spec/ossie-schema.json`, compare canonicalized JSON with the vendored copy, print the upstream SHA, fail distinctly on fetch error vs schema difference; verify the comparison script locally against the vendored copy and a modified copy
- [x] 7.3 Rewrite `docs/osi/osi_import.md` (shapes, dialect order, normalization, name resolution, dataset-scoped metrics, FILTER, datatype/is_time incl. the database-type-wins deviation, metric datatype in meta); verify `zensical.toml` nav still links it
- [x] 7.4 Rewrite `docs/examples/13_osi_import/shop.osi.yaml` in the flat current shape (top-level `sql`, dataset-scoped metrics, a FILTER metric, `datatype`s, qualified model-scoped metrics); update `docs/examples/13_osi_import/osi_import.md` and `setup_osi.py` if needed; re-execute both notebooks with `jupyter nbconvert --to notebook --execute --inplace` and confirm no unconverted entries in their output; test 1.14 passes for the example

## 8. Gates

- [x] 8.1 `poetry run pytest -m "not integration"` passes in full
- [x] 8.2 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no new errors vs baseline) pass
- [x] 8.3 `poetry run la-arch-check` passes (incl. the new `specs ['importers']` mapping) and `openspec validate dev-2160-osiossie-importer-catch-up-with-current-ossie-core-spec --strict` passes
