## Why

SLayer can import Ossie (OSI) semantic-model documents but cannot write them, so models built in SLayer cannot move to other Ossie tools, and Ossie cannot serve as a SLayer backup or migration format. Export closes the loop with the same clean-fail contract as import: everything Ossie can express is exported, nothing is silently dropped.

## What Changes

- New `slayer export-osi --datasource X [--models …] --out file.yaml|.json [--no-extensions]` and a Python API `export_osi(...)` that write one datasource's models as a flat current-spec Ossie document (`version: 0.2.0.dev0`) validating against the pinned schema, plus an export report (entity, reason, workaround).
- Datasets, fields, keys, relationships and metrics are exported wherever Ossie defines the same meaning as SLayer; filtered columns become masked field expressions; query-backed and filtered models become query sources.
- A versioned `SLAYER` custom extension per entity carries what Ossie has no home for (join type/name/cardinality, measure formula and home, column display attributes, SLayer-only columns and measures, source structure, access tags), with a fingerprint of the Ossie content it was exported alongside.
- `import-osi` reads `SLAYER` extensions and restores SLayer structure where the fingerprint shows the Ossie content is unedited, falling back to the Ossie content otherwise; it also learns the standard statistical aggregates (`STDDEV_*`, `VAR_*`, `CORR`, `COVAR_*`) and `APPROX_COUNT_DISTINCT`, and keeps an explicit `is_time: false`.
- The aggregate SQL dispatch moves out of the SQL generator into one shared function, so executed and exported aggregate SQL cannot drift.

## Capabilities

### New Capabilities
- `importers/osi-export`: exporting SLayer models to Ossie documents — mapping, expression rendering, the `SLAYER` extension, and the export report.

### Modified Capabilities
- `importers/osi-import`: restoring from `SLAYER` extensions; statistical and approximate-distinct aggregates; explicit `is_time: false` round trip.
- `cli/commands`: the new `export-osi` command.

## Impact

- New `slayer/osi/exporter.py`, `slayer/osi/slayer_extension.py`, a datasource-type ↔ Ossie-token map in `slayer/osi/dialects.py`; changes in `slayer/osi/converter.py`, `slayer/osi/expression.py`, `slayer/ingest_report.py` (shared report base), `slayer/sql/render/aggregates.py` and `slayer/sql/generator.py` (shared aggregate dispatch), `slayer/cli.py`.
- No new package, dependency, MCP tool or REST endpoint; no architecture model change.
- Docs: new `docs/osi/osi_export.md` (+ `zensical.toml` nav), `docs/reference/cli.md`, `docs/osi/osi_import.md`.
