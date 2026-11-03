## Why

The one-database-door change made the Python ingestion, drift and importer entry points take an engine-owned client (and made two of them async), so code written for SLayer 1.5.0 breaks on upgrade. 1.6.0 is not yet released, so the 1.5.0 call forms can be restored before any user sees the break. Separately, nothing reminds us to remove compatibility code at 2.0: deprecations are ad hoc warnings or help text with no shared marker.

## What Changes

- The 1.5.0 call forms work again, unchanged, each emitting one `FutureWarning` that names the call, the new form and "until SLayer 2.0", attributed to the caller's line. Without a `client`, they run through a short-lived query engine (no client factory), closed afterwards.
- `ingest_datasource` / `ingest_datasource_report` return to their 1.5.0 synchronous signatures with an optional keyword-only `client=`; their async forms become `ingest_datasource_async` / `ingest_datasource_report_async` (`client=` required). `ingest_datasource_idempotent` and `validate_datasource` take an optional `client`; `validate_datasource` accepts and ignores `sql_clients=`.
- `DbtToSlayerConverter` / `OsiToSlayerConverter` accept `sa_engine=` again in its 1.5.0 position; `convert()` on such a converter collects its own metadata through the caller's engine.
- New engine methods `ingest` / `ingest_sync` and `ingest_idempotent` / `ingest_idempotent_sync`, which supply the engine's own client.
- `SlayerSQLClient` gains an optional caller-owned `sa_engine`, used on the caller's thread and never disposed by SLayer.
- One deprecation helper both warns and tags every piece of compatibility code scheduled for removal in 2.0; a tripwire test fails once the package's major version reaches 2 while any tagged site remains.
- Newly warning: CLI `--models-dir`, the `SLAYER_MODELS_DIR` environment variable, the MCP `inspect_model` tool, `MemoryNotFoundError.identifier`. The 0.9.x storage-backend adapter warnings move onto the helper.
- Docs and examples show only the new forms; every surface recommending `inspect_model` points at `inspect`; Dockerfile and example scripts use `--storage` / `SLAYER_STORAGE`.

## Capabilities

### New Capabilities
- `compatibility/legacy-call-forms`: the 1.5.0 Python call forms, their warnings and fallback engines, the sync/async split and the engine ingestion methods that replace them.
- `compatibility/removal-tripwire`: the single deprecation marker, the 2.0 tripwire, and which workarounds are covered and warn.

### Modified Capabilities
<!-- none: the 0.9.x adapter warnings keep their specified text and timing -->

## Impact

- Code: `slayer/core/deprecation.py` (new), `slayer/engine/ingestion.py`, `slayer/engine/schema_drift.py`, `slayer/engine/query_engine.py`, `slayer/sql/client.py`, `slayer/async_utils.py`, `slayer/dbt/converter.py`, `slayer/osi/converter.py`, `slayer/storage/legacy_backends.py`, `slayer/storage/base.py`, `slayer/core/errors.py`, `slayer/cli.py`, `slayer/mcp/server.py`, `slayer/api/server.py`, `slayer/demo/jaffle_shop.py`, `slayer/inspect/model_render.py`.
- Architecture: `architecture/index.yaml` maps the new `compatibility` capability; no arc42 or LikeC4 change.
- Docs/examples: `docs/getting-started/python.md`, `docs/concepts/ingestion.md`, `docs/concepts/memories.md`, `docs/reference/mcp.md` and other `inspect_model` mentions, `docs/examples/**`, `examples/**`, `Dockerfile`.
- Tests: existing callers of the async ingestion functions move to the `*_async` names or engine methods; new compatibility and tripwire suites.
