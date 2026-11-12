## 1. Tests (pr-tests)

- [x] 1.1 Re-vendor `tests/fixtures/osi/ossie-schema.json`, `tests/fixtures/osi/upstream/tpcds_semantic_model.yaml` and the provenance file (`ossie-schema.source.yaml`) at upstream `4de9943e`, or at a later `main` after sweeping its diff (stop and ask if the sweep finds a relevant change); verify `poetry run python scripts/ossie_schema_drift.py` exits 0
- [x] 1.2 Add `ruamel.yaml` as a dev-only dependency (`poetry add --group dev ruamel.yaml`); verify it imports under `poetry run`
- [x] 1.3 Migrate `tests/fixtures/osi/shop.{yaml,json}` to `nonportable_extensions` with object `data`, and add `tests/fixtures/osi/legacy_custom_extensions.yaml` in the SLayer 1.8 export shape (legacy key, `SLAYER` and foreign string data, matching fingerprints)
- [x] 1.4 Import tests for every `importers/osi-import` delta scenario: tpcds imports with SALESFORCE/DBT stored as objects; document-level entries kept; foreign entry stored as object; malformed payload; legacy key at all five levels (parameterised) with exactly one `info` entry per document; both keys on one entity; legacy non-object strings dropped and reported; every invalid-`data` case (unquoted date and timestamp, nested `.inf`/`-.inf`/`.nan`, nested non-string key, list and scalar `data`) dropping only its entry
- [x] 1.5 Export tests for every `importers/osi-export` delta scenario: `SLAYER` payload and foreign entries under `nonportable_extensions` as objects, no `custom_extensions` key anywhere, schema validation; document-level foreign entries valid and malformed with extensions on and off; the 1.8-stored-model scenario
- [x] 1.6 Migration tests for `SlayerModel` 22→23: each meta location (model, column, measure, join, `osi_semantic_model`) converted; object `data` passes unchanged (idempotent); order preserved; mixed valid/invalid list handled per entry, undecodable entries moved with their `data` unchanged; non-list meta value untouched
- [x] 1.7 YAML quoting test: the strings of the scenario (and the sign/case/exponent variants) round-trip identically through `yaml.safe_load` and a `ruamel.yaml` YAML 1.2 core-schema loader
- [x] 1.8 Round-trip tests: export → import → export keeps every `SLAYER` payload, fingerprints match, the two exports are byte-identical; a 1.8 legacy export restores the same structure as its current-key twin
- [x] 1.9 Align existing tests that pin the old key, string data or `SlayerModel` version 21 (`tests/test_osi_*.py`, `tests/_osi_export_helpers.py`, version-pinning tests); verify each fails only for the planned reason before implementation

## 2. Implementation (pr-implement)

- [x] 2.1 `slayer/osi/models.py`: `OSINonportableExtension` (raw optional `vendor_name` and `data`, legacy marker not settable from input); `nonportable_extensions` on document, dataset, field, metric, relationship; a validator moving legacy `custom_extensions` entries under the new key marked legacy, undecoded; verify the model tests pass
- [x] 2.2 `slayer/osi/slayer_extension.py`: the shared entry checker (decode legacy strings; require a JSON-representable object at every depth) and the codec encoding/decoding the payload as an object; `PAYLOAD_VERSION` stays 1; verify the codec tests pass
- [x] 2.3 `slayer/osi/converter.py`: check every entry through the checker, report and drop rejects with entity and vendor, emit one legacy `info` entry per document, store foreign entries in `meta["osi_nonportable_extensions"]` and the document's in `meta["osi_semantic_model"]["nonportable_extensions"]`; verify the import tests pass
- [x] 2.4 `slayer/osi/exporter.py`: write `nonportable_extensions` with object `data` for `SLAYER` and foreign entries (entity and document level), checked through the same checker; verify the export tests pass
- [x] 2.5 `slayer/storage/migrations.py`: `SlayerModel` 22→23 migration and `CURRENT_VERSIONS["SlayerModel"] = 23`; verify the migration tests pass
- [x] 2.6 `slayer/cli.py` export-osi YAML writer: one dumper quoting every string PyYAML or the YAML 1.2 core schema would retype; verify the quoting test passes
- [x] 2.7 Migrate `docs/examples/13_osi_import/shop.osi.yaml` to the new key and re-execute both notebooks (`jupyter nbconvert --to notebook --execute --inplace`), saving the outputs

## 3. Docs and tracker

- [x] 3.1 Update `docs/osi/osi_import.md`, `docs/osi/osi_export.md`, `docs/reference/cli.md` (`--no-extensions` wording) and `docs/examples/13_osi_import/osi_import.md` to `nonportable_extensions` with object `data`; verify `grep -rn custom_extensions docs/` lists only the legacy-key mentions
- [x] 3.2 Merge `origin/main`; `docs/upgrading/2.0.md` must exist (PR #590). If it does not, STOP and ask. Add a section on the exported document's `nonportable_extensions` and the legacy-key import; verify the page's nav entry in `zensical.toml`
- [ ] 3.3 Full unit suite, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs the baseline) and the enforcement bundle (`poetry run la-arch-check`) green
