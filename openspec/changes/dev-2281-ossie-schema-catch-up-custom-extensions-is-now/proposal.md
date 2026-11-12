## Why

apache/ossie `08d2ad3a` (#522) renamed `custom_extensions` to `nonportable_extensions` and made each entry's `data` a JSON object, still under `0.3.0.dev0`; the schema rejects the old key. SLayer's exports therefore fail upstream validation, and the importer silently ignores a current document's extensions.

## What Changes

- Re-vendor the Ossie schema and the `tpcds_semantic_model.yaml` example at upstream `4de9943e` (or a later swept `main`); the drift script is green again.
- The importer reads `nonportable_extensions` with object `data` at every level (document, dataset, field, metric, relationship). An entry whose `data` is not a JSON-representable object is dropped with a report; the rest of the document imports.
- The importer keeps reading the legacy `custom_extensions` key (string `data`, as SLayer 1.8 exports wrote it) on any version, with one `info` report entry per document suggesting a re-export.
- **BREAKING (exported document shape)**: the exporter writes `nonportable_extensions`, with the `SLAYER` payload and re-emitted foreign entries as objects.
- Imported foreign extensions are stored in `meta["osi_nonportable_extensions"]` with object `data`; a `SlayerModel` 21→22 migration rewrites models stored under `osi_custom_extensions`.
- The YAML export quotes every string a YAML 1.2 core-schema loader would read as a non-string.

## Capabilities

### New Capabilities

### Modified Capabilities
- `importers/osi-import`: current-spec key list; `SLAYER` restore reads the new key with object data; legacy `custom_extensions` input; extension data must be a JSON object.
- `importers/osi-export`: the `SLAYER` payload and foreign entries travel under `nonportable_extensions` as objects, including from models stored before the migration; YAML quoting of retypable strings.

## Impact

- Code: `slayer/osi/models.py`, `slayer/osi/converter.py`, `slayer/osi/exporter.py`, `slayer/osi/slayer_extension.py`, `slayer/storage/migrations.py` (+ `CURRENT_VERSIONS["SlayerModel"]` 22), `slayer/cli.py` (YAML dumper).
- Fixtures: `tests/fixtures/osi/` (schema, provenance, upstream example, shop fixtures, new legacy fixture), `docs/examples/13_osi_import/` (document and notebooks).
- Dependencies: `ruamel.yaml` as a dev-only dependency (YAML 1.2 oracle in tests).
- Docs: `docs/osi/osi_import.md`, `docs/osi/osi_export.md`, `docs/reference/cli.md`, `docs/examples/13_osi_import/osi_import.md`, `docs/upgrading/2.0.md` (lands with PR #590).
