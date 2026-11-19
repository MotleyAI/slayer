## Why

PR #602 adds opt-in OpenTelemetry (traces, metrics and logs; issue [#483](https://github.com/MotleyAI/slayer/issues/483)) to `main`, which becomes SLayer 2.0. Operators on 1.x need the same visibility before 2.0 ships, so the 1.x line carries the same feature in a 1.x release.

## What Changes

- Port #602, in the state that merges to `main`, onto `1.x`: `slayer/core/probes.py`, the `slayer.otel` package, the `otel` extra (also in `all`), the `slayer otel status` and `slayer otel test` commands, the hook points, the docs, `examples/otel` and the tests.
- Port the fixes that #602 bundles:
  - Warnings keep naming the caller's file and line through a probed function.
  - Ingest reports metadata-only updates (`+key metadata`).
  - The Flight server logs translation failures.
  - The v2→v3 query migration logs its message as a template with arguments.
- OTel stays off by default. With OTel off, 1.x behaves as before. The base install gains no dependency.
- The hook points in three files that `main` moved are re-applied at their 1.x paths: `slayer/sql/client.py`, `slayer/engine/ingestion.py` and `slayer/engine/schema_drift.py`.
- 1.x deviations from `main`:
  - An EXPLAIN shows up as `slayer.db.execute`, because 1.x runs EXPLAIN through `client.execute`.
  - The `ingest` and `ingest.drift` probes sit on the async implementations behind 1.x's sync deprecation shims.
  - Names that 1.x's code fixes use 1.x vocabulary (`ModelNotFoundError`, `/models/{name}`, the 1.x MCP tool names). The telemetry names SLayer chooses are `main`'s (`slayer.ingest.datasets.*`).
- New tests beyond #602's: a 1.x test pins the EXPLAIN span, and request-span tests cover Flight SQL and the Postgres facade.

## Capabilities

### New Capabilities

- `otel/controls`: how OTel is switched on and off, the opt-in for sensitive data, and the `slayer otel status` and `slayer otel test` commands.
- `otel/collection`: which spans, attributes, metrics and log records SLayer produces, and what stays in the process.
- `otel/delivery`: how the data leaves the process, how SLayer shares OTel with a host, and the bounds on its cost to the host.

The three deltas are #602's merged deltas with a `## Purpose` added, and the names fixed by 1.x's code translated to 1.x vocabulary (design.md, decision 16).

### Modified Capabilities

None.
- `usage-reporting` already requires one fixed usage key per leaf command, and a report of the installed extras. The new keys `cli:otel.status` and `cli:otel.test`, and the new token `otel`, follow these requirements.
- Reporting metadata-only ingest updates fills a gap in the existing ingest report, without changing a requirement.

## Impact

- New code: `slayer/core/probes.py`, `slayer/otel/`.
- Hook points: `slayer/cli.py`, `slayer/api/server.py`, `slayer/mcp/server.py`, `slayer/flight/handlers.py`, `slayer/pg_facade/connection.py`, `slayer/engine/query_engine.py`, `slayer/engine/cache.py`, `slayer/sql/client.py`, `slayer/engine/ingestion.py`, `slayer/engine/schema_drift.py`, `slayer/usage_reporting/payload.py`.
- Bundled fixes:
  - `slayer/core/deprecation.py` (`outside_stacklevel`) and the fixed-`stacklevel` warning sites a probed call reaches;
  - `ModelAddition.has_changes` / `metadata_changed` in `slayer/engine/schema_drift.py`, with the MCP renderer using them;
  - `slayer/flight/server.py`;
  - `slayer/storage/v3_migration.py`.
- Packaging: the `otel` extra in `pyproject.toml`, including `requests`; `poetry.lock` regenerated on 1.x.
- Architecture:
  - `architecture/model.c4` gains the nested `core.probes` element, the `otel` node (claims `slayer.otel`), and the edges `protocols -> otel`, `surfaces -> otel` and `otel -> core.probes`.
  - `architecture/index.yaml` maps the cross-cutting `otel` spec.
  - New file `architecture/python.otel.arc42.md`.
  - The arc42 diagrams are regenerated, and the node-list paragraph in `system.arc42.md` takes `main`'s wording.
- Docs: `docs/reference/opentelemetry.md` (in the `zensical.toml` nav), the `slayer otel` section in `docs/reference/cli.md`, the metadata-fill sentence in `docs/concepts/ingestion.md`, a README section, and `examples/otel`.
- Tests:
  - new: `tests/test_probes*.py`, `tests/test_otel_*.py`, `tests/_otel_helpers.py`, `tests/perf/test_otel_bench.py`, `tests/flight/test_translation_failure_log.py`;
  - changed: `tests/conftest.py`, `tests/pg_facade/test_connection.py`, `tests/test_dev2073_surfaces.py`, `tests/test_migrations.py`.
- Forward-merge (1.x → `main`): the reconciliation steps are recorded on DEV-2358.
