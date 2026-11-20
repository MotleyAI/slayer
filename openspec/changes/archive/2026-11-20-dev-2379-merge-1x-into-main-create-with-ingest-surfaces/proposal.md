## Why

#611 (DEV-2377) fixes `1.x`'s create-with-ingest surfaces: `slayer datasources create --ingest` and MCP `create_datasource` ran a second, hand-rolled ingest pipeline that exported no `slayer.ingest` span or metrics and overwrote stored models. `main` has the same second pipeline. Merging `1.x` into `main` brings the fix over and keeps `main` containing every `1.x` commit, instead of hand-porting it and resolving it again at the next forward merge.

## What Changes

- Merge #611's head now and `origin/1.x` again once #611 is merged and archived; never rebase.
- `slayer datasources create --ingest` and MCP `create_datasource` (with `auto_ingest`) run the idempotent ingest on `main`; the second pipeline (`_persist_ingested_datasets`, the hand-rolled MCP save) is deleted. The `slayer.ingest` span, `slayer.ingest.duration` and `slayer.ingest.datasets` come from the one existing probe.
- **BREAKING** (behaviour): re-creating a datasource whose datasets are stored merges the fresh scan additively instead of overwriting them; the CLI dataset-collision prompt is removed. The datasource-overwrite prompt stays.
- Create output reuses each surface's ingest renderer (CLI `Created:` / `Updated:` lines and sections; MCP the `ingest_datasource` rendering), key-read failures included.
- CLI `slayer datasources create` opens a `slayer.request` span (surface `cli`, operation `datasources.create`).
- Both CLI ingest commands show the empty-ingest message only when the pass reports nothing at all, key-read failures included.
- New law `tests/test_law_ingest_door.py`, recorded as the "One ingest door" clause of `python.live_schema.arc42.md` §3.1 (`main`'s home of the ingestion axiom).
- `openspec/forward-merge-ledger.yaml` lists the #611 requirements `main` words in its own vocabulary.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `otel/collection`: `slayer datasources create` opens a CLI request span; every surface that ingests produces exactly one ingest span tree per datasource, with the duration and dataset-count metrics.
- `cli/commands`: `datasources create --ingest` runs the idempotent ingest, renders like `slayer ingest`, exits 0 once the datasource is created; the empty-ingest message needs an empty result.
- `mcp/tool-surface`: `create_datasource` auto-ingest runs the idempotent ingest and renders like `ingest_datasource`.

## Impact

- Code: `slayer/cli.py`, `slayer/mcp/server.py`, `slayer/live_schema/ingestion.py`.
- Architecture: `architecture/python.live_schema.arc42.md` §3.1 gains the "One ingest door" clause; `architecture/system.arc42.md` stays `main`'s; no model or import change.
- Tests: `tests/test_otel_ingest_surfaces.py`, `tests/test_create_ingest_idempotent.py`, `tests/test_law_ingest_door.py` (new); `tests/test_gateway_features.py`, `tests/test_ingest_cli_ux.py` aligned.
- Docs: `docs/reference/cli.md`, `docs/getting-started/cli.md`, `docs/concepts/ingestion.md`, `docs/reference/mcp.md`, `docs/reference/opentelemetry.md`.
- OpenSpec: `openspec/forward-merge-ledger.yaml`; #611's DEV-2377 change folder joins `main`'s archive.
