## Why

The OSI importer (`slayer/osi/`) models a frozen mid-September snapshot of the upstream Ossie core spec (`open-semantic-interchange/OSI`, now Apache Ossie). A document written to the current spec (`core-spec/ossie-schema.json`, `version: 0.2.0.dev0`, pinned at `89e2e17`, 2026-10-27) does not import at all, and several newer constructs are silently dropped. Upstream moves daily, so the fix also needs a drift signal.

## What Changes

- **BREAKING (input shape)**: accept the flat single-model document (`version`, `name`, `datasets`, `relationships`, `metrics` at the root). The legacy `semantic_model` wrapper (array or object) keeps importing, split into one document per model.
- `Expression` gains the optional top-level `sql` (authoritative `OSSIE_SQL_2026`); `dialects` becomes optional; `dialect` is any non-empty string.
- Deterministic dialect selection: `--dialect` override, top-level `sql`, `OSSIE_SQL_2026`, `ANSI_SQL`, the target datasource's own dialect, then any other sqlglot-readable dialect. Unreadable dialects are reported as unconverted, never raised. **BREAKING**: `--dialect` default changes from `ANSI_SQL` to unset.
- One normalization path: the selected expression is parsed once and raw-SQL outputs are rendered in the target datasource's dialect; target-native expressions are stored verbatim.
- Current-spec name resolution: models expose their declared fields only; metrics, keys and relationships resolve against declared fields with the spec's qualification and identifier-case rules. Legacy-wrapper documents keep today's lenient behaviour.
- Dataset-scoped metrics (`datasets[].metrics`) become measures on that dataset's model.
- `FILTER (WHERE …)` aggregates convert to masked aggregates; aggregate operands are emitted inline in the formula when expressible, with the hidden raw-SQL column only as fallback.
- `datatype` on fields and metrics is honoured: field type fallback plus cross-check, `is_time` as a role flag defaulted from temporal datatypes, metric datatype kept in meta.
- Empty `primary_key` / `unique_keys` entries are reported and ignored.
- Drift guard: vendored upstream schema, a schema-validation and model-coverage test, and a weekly drift workflow.
- Docs and the worked example updated to the current shape.

## Capabilities

### New Capabilities
- `importers/osi-import`: importing Ossie (OSI) semantic-model documents into SLayer models — document shapes, dialect selection and normalization, name resolution, metric conversion, datatypes, keys, and the reporting contract.

### Modified Capabilities
<!-- none -->

## Impact

- Code: `slayer/osi/models.py`, `slayer/osi/parser.py`, `slayer/osi/converter.py`, `slayer/osi/expression.py`, `slayer/cli.py` (`import-osi` `--dialect`).
- Architecture: `architecture/model.c4` — `importers` bucket gains `specs ['importers']` (approved).
- Tests: `tests/test_osi_*.py`, `tests/test_cli_import_osi.py`, new current-spec fixtures and the vendored `tests/fixtures/osi/ossie-schema.json`.
- CI: new scheduled `.github/workflows/ossie-schema-drift.yml`.
- Dependencies: `jsonschema` as an explicit dev dependency.
- Docs: `docs/osi/osi_import.md`, `docs/examples/13_osi_import/` (`shop.osi.yaml`, `osi_import.md`, both notebooks re-executed).
- Out of scope: re-import overwriting user-edited models (all importers) — DEV-2162.
