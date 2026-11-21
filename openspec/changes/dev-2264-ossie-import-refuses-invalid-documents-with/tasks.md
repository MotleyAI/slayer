## 1. Tests first (pr-tests)

- [x] 1.1 Write one refusal test per rule id of design.md (`unreadable-file`, `duplicate-key`, `legacy-shape`, `schema`, `extension-data`, `name-length`, `duplicate-name`, `unknown-dataset`, `undeclared-key-field`, `relationship-arity`, `ossie-sql`, `field-scope`, `metric-scope`), each asserting the error names the file, the entity and the rule; verify each fails on the current code
- [x] 1.2 Write aggregation tests: one document with several violations, two invalid files plus a valid one, and a CLI run asserting nothing is saved and exit 1 without a traceback; verify they fail
- [x] 1.3 Write converter-entry tests: an invalid document built in code is refused at construction, before `collect_metadata` reads the database (a client that fails on any read); verify they fail
- [x] 1.4 Write upgrade tests: a wrapper that resolves under the current rules imports with one info entry; a wrapper relying on source-column names is refused listing each reference; a root/model key collision is refused; a flat `0.2.0.dev0` document imports with one info entry; an unknown version and a flat document without a version are refused; the SLayer 1.8 `custom_extensions` export restores; an undecodable legacy string is refused
- [x] 1.5 Write identifier tests: case-insensitive key and relationship names store the declared spelling; `SUM("AMOUNT")` reaches `Amount` and `SUM("Amount")` is refused; a 129-character name is refused; names holding a double quote, a dot, whitespace or non-ASCII letters pass; an own-dataset-qualified field column reads the source column
- [x] 1.6 Write `ossie-sql` tests: `AGG` in any case, a subquery, a CTE, a set operation, several statements and a cross-dataset `FILTER` predicate in an Ossie form are refused; the same text in a vendor dialect is not a violation; a superseded `OSSIE_SQL_2026` entry gives info when unparseable or different and nothing when identical
- [x] 1.7 Write limit tests: each per-entity report of the proposal's "stays a report" list still imports with its entry (MDX-only metric, computed join key, a field reading a column the live source lacks, unknown `source.column`, ambiguous query label, a name SLayer cannot store, second relationship to one target, no join path, malformed `SLAYER` payload)
- [x] 1.8 Write stable-order tests across recursively discovered files and wrapper entries, and export round trips asserting every exporter output (with and without `SLAYER` and foreign extensions) passes the validator and re-imports
- [x] 1.9 Align existing tests that pin skip-and-warn, lenient resolution, cross-dataset field references or exact key matching (ask approval file by file); verify the vendored upstream examples and `docs/examples/13_osi_import/shop.osi.yaml` still import cleanly and the ontology examples are skipped

## 2. Packaging

- [ ] 2.1 Move the pinned schema to `slayer/osi/ossie-schema.json` as package data, repoint `tests/fixtures/osi/ossie-schema.source.yaml`, `scripts/ossie_schema_drift.py` and the test helpers, and delete the test copy; verify `tests/test_ossie_schema_drift.py` passes
- [ ] 2.2 Move `jsonschema` to the core dependencies in `pyproject.toml` and update `poetry.lock`; verify `poetry install -E all` and an import of the validator succeed

## 3. Validator and upgrade

- [ ] 3.1 Add `slayer/osi/validation.py` with `OssieViolation`, `OsiConversionError` (still importable from `slayer.osi.converter`), the shared Ossie-identifier helper and every rule of design.md's table; verify the 1.1, 1.5 and 1.6 tests pass
- [ ] 3.2 Rework `slayer/osi/parser.py`: duplicate-key-rejecting YAML and JSON loaders, the upgrade step (wrapper, earlier flat versions, `custom_extensions`, one info note per upgraded document), private provenance, the ontology/mapping skip, and one aggregated raise from `parse_osi_path`; verify the 1.2 and 1.4 tests pass
- [ ] 3.3 Validate at converter construction (semantic rules plus the schema on the typed dump) and carry the parser's upgrade notes into the report; verify the 1.3 tests pass

## 4. Converter cleanup

- [ ] 4.1 Delete the legacy path (`OSIDocument.legacy`, `_overlay_fields`, `_apply_legacy_primary_key`, the `_shadowed` revert, legacy metric resolution) and the relationship-walking resolution of field-expression references, keeping what `SLAYER` restoration needs; verify the full OSI test set passes
- [ ] 4.2 Delete the skip-and-warn branches for rules the validator owns (empty or undeclared keys, relationship datasets, arity and undeclared key fields, duplicate metric names, `_check_dataset_namespaces`, `_check_duplicate_dataset_names`, the `_ExtensionCheck` drop path, `expression: {}`), resolve keys and metric references through the shared identifier helper, and store declared spellings; verify the 1.5 and 1.7 tests pass

## 5. Docs and gates

- [ ] 5.1 Update `docs/osi/osi_import.md` (refusal, the upgrade of earlier shapes, what stays a report) and `docs/reference/cli.md` (`import-osi` failure behaviour); verify `zensical.toml` nav still lists every page
- [ ] 5.2 Add a `docs/upgrading/2.0.md` section with a before and after for each breaking change (invalid documents refused; lenient resolution removed; cross-dataset field references refused; quoted-identifier matching; unknown versions refused)
- [ ] 5.3 Run the local test scope, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors) and `poetry run la-arch-check`; verify all pass
