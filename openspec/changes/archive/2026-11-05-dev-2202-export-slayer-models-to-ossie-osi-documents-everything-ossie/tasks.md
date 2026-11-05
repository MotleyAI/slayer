## 1. Tests first (pr-tests)

- [x] 1.1 Export spec tests (`tests/test_osi_export_*.py`): one test per `importers/osi-export` scenario — schema validation, determinism, failing-client export, subset, sources, fields, datatypes/time role, keys, relationships, metrics, portable/dialect-only expressions, extension content, `--no-extensions`, complete report; verify they fail before implementation.
- [x] 1.2 Round-trip matrix tests on DuckDB and SQLite (design decision 28), including `docs/examples/13_osi_import/` import → export → import; verify they fail before implementation.
- [x] 1.3 Importer tests for every new `importers/osi-import` scenario (extension restore, fingerprint fallback, per-entity validation, source restore, SLayer-only content, new aggregates, explicit `is_time`); verify they fail before implementation.
- [x] 1.4 Dialect-token map tests covering every supported datasource alias (decision 22); CLI tests for both `cli/commands` scenarios; verify they fail before implementation.

## 2. Shared foundations

- [x] 2.1 Extract the pure aggregate dispatch from `SQLGenerator._build_agg` into `slayer/sql/render/aggregates.py` (decision 14); the generator delegates. Verify: full unit suite green with no test changes.
- [x] 2.2 Add the datasource-type ↔ Ossie-token ↔ reader map to `slayer/osi/dialects.py` and route import selection and native-verbatim detection through it. Verify: 1.4 map tests and existing `tests/test_osi_dialects.py` pass.
- [x] 2.3 Extract a `ConversionReport` base in `slayer/ingest_report.py` shared by `ConversionResult` and the export result. Verify: existing importer report tests pass.
- [x] 2.4 Add `slayer/osi/slayer_extension.py`: versioned Pydantic payloads per entity kind (fail-closed dispatch) and the fingerprint over normalized expression ASTs + restorable content. Verify: payload/fingerprint unit tests from 1.1/1.3 pass.

## 3. Exporter

- [x] 3.1 `slayer/osi/exporter.py`: document, datasets and sources (table, sql-mode, filter wrapper, query-backed snapshot via the engine), fields (inlining, masks, descriptions, datatypes, `is_time`), keys. Verify: corresponding 1.1 tests pass.
- [x] 3.2 Relationships: FK direction, mirrored merge, key-proven undetermined cardinality, reported many-to-many / out-of-subset joins, name allocator. Verify: relationship tests pass.
- [x] 3.3 Metrics: bind with `bind_expr`, classify (Ossie component rule, decision 29), render portable SQL through the shared aggregate dispatch and the canonical scalar renderer, scope placement, quoting. Verify: metric tests pass.
- [x] 3.4 Extensions: per-entity `SLAYER` payloads, SLayer-only columns/measures, foreign-extension validation, `--no-extensions` reporting and access-tag refusal, the report contract. Verify: extension/report tests pass.

## 4. Importer

- [x] 4.1 New aggregates (`STDDEV_*`, `VAR_*`, `CORR`, `COVAR_*`, `APPROX_COUNT_DISTINCT`, with `FILTER`) in `slayer/osi/expression.py`; record explicit `is_time` both ways. Verify: 1.3 aggregate and time-role tests pass.
- [x] 4.2 Two-phase restore in `slayer/osi/converter.py` (decision 20): decode payloads, fingerprint check, per-entity validated restore of joins, columns, measures, sources (query-backed after its inputs), SLayer-only content. Verify: 1.3 restore tests and 1.2 round-trip tests pass.

## 5. Surfaces and docs

- [x] 5.1 CLI `slayer export-osi` in `slayer/cli.py` (YAML/JSON by suffix, report printing, errors). Verify: CLI tests pass.
- [x] 5.2 Docs: `docs/osi/osi_export.md` + `zensical.toml` nav, `docs/reference/cli.md`, one sentence each in `docs/osi/osi_import.md` for extension restore and new aggregates. Verify: `tests/test_doc_examples.py` passes and the page is in the nav.

## 6. Gates

- [x] 6.1 Full unit suite (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors), `poetry run la-arch-check` all green.
