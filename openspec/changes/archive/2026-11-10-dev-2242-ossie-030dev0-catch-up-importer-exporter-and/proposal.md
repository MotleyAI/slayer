## Why

Upstream Ossie (now `apache/ossie`) replaced `0.2.0.dev0` with `0.3.0.dev0`: flat documents only, every key,
relationship and metric name is a dataset field, and dataset-scoped metrics may read source columns as
`source.column`. SLayer still reads, writes and vendors `0.2.0.dev0`, so its exports fail the current schema and the
weekly schema-drift run fails.

## What Changes

- The importer knows `0.3.0.dev0`, skips a `semantic_model`-wrapped document that declares it, and resolves
  `source.column` in dataset-scoped metrics of every flat document to the physical source column (an existing
  pass-through field, else a hidden helper column), reporting every case it cannot read with the source's meaning.
- A sql-mode dataset whose query has duplicate output labels reports every field or `source.column` reading that label.
- The exporter writes `version: 0.3.0.dev0`.
- The vendored schema moves to `apache/ossie` 0.3.0.dev0; every upstream `examples/` file is vendored beside it with
  its provenance, and the weekly drift check compares the schema and the examples against `apache/ossie`.
- Docs: `docs/osi/osi_import.md`, `docs/osi/osi_export.md`, and the worked example `shop.osi.yaml`.

## Capabilities

### New Capabilities

### Modified Capabilities
- `importers/osi-import`: known versions and the 0.3 flat-only shape, `source.column` resolution, duplicate output
  labels of query sources, legacy documents limited to the wrapper shape, upstream examples import.
- `importers/osi-export`: the exported document declares `0.3.0.dev0`.

## Impact

- Code: `slayer/osi/parser.py`, `slayer/osi/models.py`, `slayer/osi/converter.py`, `slayer/osi/exporter.py`,
  `slayer/importers/column_types.py` (query-source output labels).
- Tooling: `scripts/ossie_schema_drift.py`, `.github/workflows/ossie-schema-drift.yml`.
- Fixtures: `tests/fixtures/osi/ossie-schema.json`, its provenance file, `tests/fixtures/osi/upstream/`.
- No architecture model or arc42 change; no API change beyond the exported version string.
