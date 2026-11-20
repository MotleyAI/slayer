## Why

With OpenTelemetry on, `slayer datasources create <url> --ingest` and the MCP `create_datasource` tool export no `slayer.ingest` span, no `slayer.ingest.duration` point and no `slayer.ingest.datasets` counts (reported by an external user on 1.9.0). The cause is structural: these two callers run a second, hand-rolled ingest pipeline (scan → save every model → fill the description) beside the idempotent ingest every other surface uses. Besides the missing probe, that pipeline overwrites stored models, breaking `system.arc42.md` §3.14 (ingestion is idempotent and additive-only), and skips name adoption, merge, broken-dependents reporting, drift validation, the embeddings refresh and the effective hidden-internals check.

## What Changes

- `slayer datasources create <url> --ingest` and MCP `create_datasource` (with `auto_ingest`) run the idempotent ingest; the second pipeline is deleted. The `ingest` span, duration and dataset counts come from the one existing probe.
- **BREAKING** (behaviour): re-creating a datasource whose models are stored merges the fresh scan additively instead of overwriting the models; the CLI model-collision prompt is removed. The datasource-overwrite prompt stays.
- Create output reuses each surface's ingest renderer: the CLI prints `Created:` / `Updated:` lines and the drift / skipped / hidden / errors sections exactly as `slayer ingest`; MCP appends the `ingest_datasource_models` rendering. The `Ingested: X (n columns, m measures)` lines go.
- CLI `slayer datasources create` opens a `slayer.request` span (surface `cli`, operation `datasources.create`).
- Both CLI ingest commands show the empty-ingest message only when the pass reports nothing at all; today `slayer ingest` with every requested schema skipped hides the skip reasons behind "No tables or views found".
- New law: surfaces and protocols ingest only through the idempotent pass (`tests/test_law_ingest_door.py`), recorded as the "One ingest door" clause of `system.arc42.md` §3.14.

## Capabilities

### New Capabilities

### Modified Capabilities
- `otel/collection`: `slayer datasources create` opens a CLI request span; every surface that ingests produces the ingest span tree, and `slayer ingest` exactly one.
- `cli/commands`: `datasources create --ingest` runs the idempotent ingest, renders like `slayer ingest`, exits 0 once the datasource is created; the empty-ingest message needs an empty result.
- `mcp/tool-surface`: `create_datasource` auto-ingest runs the idempotent ingest and renders like `ingest_datasource_models`.

## Impact

- Code: `slayer/cli.py` (`_run_datasources_create`, `_create_ingest_and_report`, `_persist_ingested_models` removed, `_ingest_workflow` empty gate), `slayer/mcp/server.py` (`create_datasource`), `slayer/engine/ingestion.py` (a shared full-shape emptiness check beside the renderers).
- Architecture: `architecture/system.arc42.md` §3.14 gains the approved "One ingest door" clause; no model or import change.
- Tests: new OTel span/metric tests for both create paths and `slayer ingest`, behaviour tests, `tests/test_law_ingest_door.py`; existing tests pinning the old create output are aligned.
- Docs: `docs/reference/cli.md`, `docs/reference/mcp.md`, `docs/concepts/ingestion.md`, the OTel docs page listing CLI request spans.
- Release: targets `1.x`; reaches `main` via the 1.x → main forward-merge.
