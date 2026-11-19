## 1. Source gate (first step of pr-tests)

- [x] 1.1 #602 has merged to `main`. Take its merge commit `M`. Verify that:
  - `git diff --name-only M^1 M` lists exactly the files in proposal.md's Impact, at their `main` paths;
  - for each of `collection`, `controls` and `delivery`, this change's `specs/otel/<c>/spec.md` equals `## Purpose` plus `M`'s `gh-483-opentelemetry-instrumentation/specs/otel/<c>/spec.md` with design.md decision 16's translation applied, byte for byte. #602 may have archived its change before merging; if so, take the delta from the archived folder in `M`.

  On any difference, STOP and present the exact plan edit for approval. Use `M^1..M` as the source for every later task.

## 2. Tests (ported from #602 unless marked new)

- [x] 2.1 `tests/test_probes.py` and `tests/test_probes_decorator.py`, including the warning-location test, run through a query whose library-name-shadow warning comes from `_emit_python_warnings` (1.x has no deprecated query form that warns during `execute`). Verify they fail only because `slayer.core.probes` doesn't exist yet.
- [x] 2.2 `tests/_otel_helpers.py`, plus the `tests/conftest.py` fixtures: autouse `_otel_off_by_default`, and `otel_env`.
- [x] 2.3 `tests/test_otel_controls.py` and `tests/test_otel_guards.py`.
- [x] 2.4 `tests/test_otel_collection.py`, with the 1.x vocabulary for code-fixed names (design.md decision 16) and `main`'s telemetry names (`slayer.ingest.datasets.*`). Any patch or import of a moved module points at its 1.x path. The ingest span tree must hold with the probes on `_ingest_idempotent` / `_validate_datasource`.
- [x] 2.5 `tests/test_otel_privacy.py`.
- [x] 2.6 `tests/test_otel_logs.py`, with logger `slayer.database.client` → `slayer.sql.client`.
- [x] 2.7 `tests/test_otel_scope.py`, with the 1.x routes (`/models/{name}`) and MCP tool names.
- [x] 2.8 `tests/test_otel_isolation.py`.
- [x] 2.9 `tests/perf/test_otel_bench.py`. Verify that `tests/perf/conftest.py` on 1.x collects it.
- [x] 2.10 Ported changes to existing tests, adapted to 1.x names:
  - `tests/pg_facade/test_connection.py`: a failed query marks its request span.
  - `tests/test_dev2073_surfaces.py`: `addition.has_changes` in place of `_addition_has_changes` / `_addition_update_details`; a metadata-only update renders `+key metadata`; a table-rename-only update has changes.
  - `tests/test_migrations.py`: the v2→v3 log names the query as an argument.
  - New `tests/flight/test_translation_failure_log.py`.
- [x] 2.11 New, 1.x only, in `tests/test_otel_collection.py`. Run `engine.execute(..., explain=True)` under `otel_env` and assert:
  - exactly one database span, `slayer.db.execute`, of kind `CLIENT`, a child of `slayer.query`, with `db.system.name` and the other known `db.*` attributes;
  - no `slayer.query.run` span;
  - no `db.query.text` without content capture, and `db.query.text` present with it.
- [x] 2.12 New, in `tests/test_otel_scope.py`. Use the setups in `tests/flight/test_handlers.py` and `tests/pg_facade/test_connection.py`. For one query on Flight, covering `_derive_query_schema` and `_execute_full` separately, and one on the Postgres facade, assert:
  - exactly one `slayer.request` span, with `slayer.surface` `flight` / `pg`, `slayer.operation=query` and kind `SERVER`;
  - `slayer.query` nests under it.
- [x] 2.13 Grep every ported or changed test for `slayer.database`, `slayer.live_schema`, `Dataset`, `dataset_name` and `/datasets`. Verify that only `main`'s telemetry names (`slayer.ingest.datasets*`) remain.

## 3. Implementation

- [x] 3.1 Copy `slayer/core/probes.py` from `M`. Verify that 2.1 passes, apart from the warning-location test, which waits for 3.4.
- [x] 3.2 Copy `slayer/otel/{__init__,adapter,control,exporters,logs,middleware,recorder,settings}.py` from `M`. Verify that 2.3 passes.
- [x] 3.3 Three-way apply #602's hunks to the files at shared paths: `slayer/cli.py`, `slayer/api/server.py`, `slayer/mcp/server.py` (the request hook, plus the `has_changes` / `_updated_detail_lines` renderer with 1.x names), `slayer/flight/handlers.py`, `slayer/flight/server.py`, `slayer/pg_facade/connection.py`, `slayer/engine/query_engine.py`, `slayer/engine/cache.py`, `slayer/storage/v3_migration.py`, `slayer/usage_reporting/payload.py`.
- [x] 3.4 Warning location:
  - add `outside_stacklevel()` to `slayer/core/deprecation.py`;
  - use it at `query_engine._emit_python_warnings` and at every other fixed-`stacklevel` warning site that a probed call reaches. Audit 1.x's `stacklevel=<N>` sites against the probed call paths, mirroring `main`'s `storage/base.py` and `storage/document_loading.py` sites.

  Verify the warning-location test from 2.1.
- [x] 3.5 Re-apply the hooks by hand:
  - `slayer/sql/client.py`: `connection_url()`; `_db_probe_attributes` without the `statements` branch; `_returned_rows`; `db.execute` on `_typed_execute` and on a new `_typed_execute_sync` split out of `execute_sync`'s sync-driver path; `db.metadata` on `_metadata`; `_record_retry` after the backoff sleep in `_retry_with_backoff` and `_execute_with_retry_sync`.
  - `slayer/engine/ingestion.py`: `ingest` on `_ingest_idempotent`; `ingest.merge`, `ingest.scan.schema` and `ingest.embeddings`; `_ingest_probe_attributes` and `_ingest_outcome` (counting through `has_changes`); `metadata_changed` in `_addition_fields`; `+key metadata` in `_updated_detail_lines`.
  - `slayer/engine/schema_drift.py`: `ModelAddition.metadata_changed` and `has_changes`; `ingest.drift` on `_validate_datasource`.

  Verify that 2.4, 2.5, 2.7 and 2.10–2.12 pass.
- [x] 3.6 `pyproject.toml`: the `otel` extra (`opentelemetry-api`, `opentelemetry-sdk` and `opentelemetry-exporter-otlp-proto-http`, each `>=1.45`, plus `requests>=2.25`, all optional), also added to `all`. Run `poetry lock` on 1.x. Verify that the lock diff only adds what the extra needs, and that `poetry install -E all` succeeds.
- [ ] 3.7 Architecture, applying design.md decision 5 verbatim:
  - the `model.c4` `core.probes` element, the `otel` node and its three arrows;
  - the `index.yaml` `otel` entry;
  - `architecture/python.otel.arc42.md`;
  - `poetry run la-arch-diagrams`;
  - the `system.arc42.md` prose replacement.

  Verify that `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass. Until this task lands, `la-arch-check` reports "spec group otel is mapped by no element"; that's expected, because the mapping can't exist before `slayer/otel/` and `slayer/core/probes.py` do.
- [x] 3.8 Docs:
  - `docs/reference/opentelemetry.md` from `M`, adapted:
    - the database-span row drops `slayer.db.explain` and notes that EXPLAIN appears as `slayer.db.execute`;
    - the sample `slayer otel status` output shows `slayer 1.8.5`;
    - code-fixed names use 1.x vocabulary;
    - one sentence says the ingest metric names use the 2.0 term "dataset" for a 1.x model.
  - The `slayer otel` section in `docs/reference/cli.md`.
  - The metadata-fill sentence in `docs/concepts/ingestion.md`, in 1.x terms (`ModelAddition`, `+key metadata`).
  - The README "OpenTelemetry" section and the `zensical.toml` nav entry.
  - `examples/otel/*` from `M`, with `examples/otel/README.md` naming `slayer.sql.client`.

  Verify that the page is linked from the nav.

## 4. Gates, PR and archive

- [ ] 4.1 Run the local test scope from `la-affected-tests --base 1.x`, plus every 2.x test. Run `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors against the baseline), `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture` and `openspec validate dev-2358-backport-to-1x-opt-in-opentelemetry --strict`. All must pass.
- [ ] 4.2 Open the PR against `1.x` after the user's go-ahead.
- [ ] 4.3 After `openspec archive`, verify that `openspec/specs/otel/{collection,controls,delivery}/spec.md` carry our `## Purpose`, with no `TBD` placeholder, and that `openspec validate --specs --strict` passes.
- [ ] 4.4 Check that the DEV-2358 comment "Forward-merge notes (1.x → main)" matches what shipped, and update it if any deviation changed.
