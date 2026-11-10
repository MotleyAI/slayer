## 1. Vendored upstream

- [x] 1.1 Replace `tests/fixtures/osi/ossie-schema.json` with `core-spec/ossie-schema.json` from `apache/ossie@c1a33181c815852f059ce7986729612332240ff8`, byte-identical; verify `const` is `0.3.0.dev0`
- [x] 1.2 Vendor all six upstream `examples/` files byte-identical into `tests/fixtures/osi/upstream/`; verify with a byte compare against the pinned commit
- [x] 1.3 Rewrite `tests/fixtures/osi/ossie-schema.source.yaml` as the provenance of all seven files (repository `apache/ossie`, commit, date, upstream path → vendored path); verify the drift tests read it

## 2. Tests (pr-tests stage; failing first)

- [x] 2.1 Align the approved existing tests and fixtures (design.md, "Existing tests and fixtures aligned"); verify each fails only for the planned reason before implementation
- [x] 2.2 Parser tests: known `0.3.0.dev0` silent; unknown version warns and parses; flat `0.2.0.dev0` imports as before; wrapped `0.3.0.dev0` skipped with a warning while a flat file imports; versionless wrapper is legacy with no declared version; ontology document skipped
- [x] 2.3 `source.column` value tests (DuckDB): helper column, pass-through reuse, metric named like the column, `SOURCE.` qualifier (quoted and unquoted), compound operand, `FILTER` predicate, `COUNT(*) FILTER`, percentile, query-source output alias and quoted mixed-case alias; each asserts the visible columns stay the declared fields
- [x] 2.4 `source.column` report tests: unknown column (no column added), quoted wrong-case leaf, two live columns matching (ambiguous), field named `source`, model-scoped metric, legacy wrapped document; the renamed collision as a strict xfail pinned to DEV-2175 asserting the correct value
- [x] 2.5 Postgres folding test (local pytest-postgresql, unit suite): `SUM(source.Quantity)` reads `quantity`
- [x] 2.6 Duplicate output labels: a field and a `source.column` reading a duplicated label of a query source are reported; the other field imports
- [x] 2.7 Upstream examples: each core model validates against the vendored schema and imports into DuckDB tables built to its sources with the expected models; `renamed_fields` value check on `gross_revenue`; tpcds unconverted entries are exactly its three window metrics; flights imports with none; the three ontology documents are each skipped with a warning naming them
- [x] 2.8 Export: version `0.3.0.dev0` and schema-valid; the `source.column` round trip (export with extensions → validate → re-import → export) is byte-identical, keeps the helper hidden, matches every fingerprint, and keeps measure values
- [x] 2.9 Drift script: all identical exits 0; a drifted example exits 1 and prints its diff; a missing upstream example and a failed example fetch exit 2; URLs name `apache/ossie`

## 3. Parser and models

- [ ] 3.1 Add `0.3.0.dev0` to the known versions; skip a wrapped document declaring it with a warning naming the file; verify 2.2 passes
- [ ] 3.2 Make `OSIDocument.version` optional, recording only the declared version; verify 2.2 and the aligned model tests pass

## 4. Importer

- [ ] 4.1 Resolve `source.column` in dataset-scoped metrics of flat documents over every root-scope column reference before owner resolution: qualifier casefold, field-named-`source` precedence, live-column matching with the datasource's folding, pass-through reuse, otherwise a hidden helper created only when the metric converts, renamed collision reported; verify 2.3, 2.4 and 2.5 pass
- [ ] 4.2 Record duplicated output labels of a query source and report every field and `source.column` reading one; verify 2.6 passes and the other importers' tests are unchanged
- [ ] 4.3 Verify 2.7 passes (fix only what the examples expose within this change's specs)

## 5. Exporter

- [ ] 5.1 Write `version: 0.3.0.dev0`; verify 2.8 passes

## 6. Drift check

- [ ] 6.1 Point `scripts/ossie_schema_drift.py` at `apache/ossie`, read the file list from the provenance file, compare the schema canonically and the examples as text, keep exit codes 0/1/2 and stdlib only; update the workflow comment; verify 2.9 passes and a live run against upstream exits 0

## 7. Docs

- [ ] 7.1 `docs/osi/osi_import.md`: one sentence each for the known versions with `0.3.0.dev0`, the wrapped-0.3 skip, the `source.column` rule, duplicate query-output labels, and `apache/ossie` links; verify links resolve
- [ ] 7.2 `docs/osi/osi_export.md`: version `0.3.0.dev0`, `apache/ossie` link, and "every entity that exists only in the extension" wording; verify against the spec
- [ ] 7.3 Move `docs/examples/13_osi_import/shop.osi.yaml` to `0.3.0.dev0`; verify the shipped-example test passes

## 8. Finish

- [ ] 8.1 Full non-integration suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors), `la-arch-check`, `npx -y likec4@1.47.0 validate architecture`; all green
