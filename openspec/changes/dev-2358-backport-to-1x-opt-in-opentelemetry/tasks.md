## 1. Source gate (first step of pr-tests)

- [ ] 1.1 #602 has merged to `main`. Take its merge commit `M`. Verify that:
  - `git diff --name-only M^1 M` lists exactly the files in proposal.md's Impact, at their `main` paths;
  - `git show M:openspec/changes/gh-483-opentelemetry-instrumentation/specs/otel/<c>/spec.md` matches this change's `specs/otel/<c>/spec.md` under `cmp`, for each of `collection`, `controls` and `delivery`. #602 may have archived its change before merging; if so, take the delta from the archived folder in `M`.

  On any difference, STOP and present the exact plan edit for approval. Use `M^1..M` as the source for every later task.

## 2. Tests (ported from #602 unless marked new)

- [ ] 2.1 `tests/test_stages.py` and `tests/test_stages_decorator.py`. Verify they fail only because `slayer.core.stages` doesn't exist yet.
- [ ] 2.2 `tests/_otel_helpers.py`, plus the `tests/conftest.py` fixtures: autouse `_otel_off_by_default`, and `otel_env`.
- [ ] 2.3 `tests/test_otel_controls.py` and `tests/test_otel_guards.py`.
- [ ] 2.4 `tests/test_otel_collection.py`. Any patch or import of a moved module points at its 1.x path. The ingest span tree must hold with the stages on `_ingest_idempotent` / `_validate_datasource`.
- [ ] 2.5 `tests/test_otel_privacy.py`.
- [ ] 2.6 `tests/test_otel_logs.py`, with logger `slayer.database.client` → `slayer.sql.client`.
- [ ] 2.7 `tests/test_otel_scope.py`.
- [ ] 2.8 `tests/test_otel_isolation.py`.
- [ ] 2.9 `tests/perf/test_otel_bench.py`. Verify that `tests/perf/conftest.py` on 1.x collects it.
- [ ] 2.10 Grep every ported test for `slayer.database` / `slayer.live_schema` and map each hit to its 1.x path. Verify the grep comes back empty.
- [ ] 2.11 New, 1.x only, in `tests/test_otel_collection.py`. Run `engine.execute(..., explain=True)` under `otel_env` and assert:
  - exactly one database span, `slayer.db.execute`, of kind `CLIENT`, a child of `slayer.query`, with `db.system.name` and the other known `db.*` attributes;
  - no `slayer.query.run` span;
  - no `db.query.text` without content capture, and `db.query.text` present with it.
- [ ] 2.12 New, in `tests/test_otel_scope.py`. Use the setups in `tests/flight/test_handlers.py` and `tests/pg_facade/test_connection.py`. For one query on Flight, covering `_derive_query_schema` and `_execute_full` separately, and one on the Postgres facade, assert:
  - exactly one `slayer.request` span, with `slayer.surface` `flight` / `pg`, `slayer.operation=query` and kind `SERVER`;
  - `slayer.query` nests under it.

## 3. Implementation

- [ ] 3.1 Copy `slayer/core/stages.py` from `M`. Verify that 2.1 passes.
- [ ] 3.2 Copy `slayer/otel/{__init__,adapter,control,exporters,logs,middleware,recorder,settings}.py` from `M`. Verify that 2.3 passes.
- [ ] 3.3 Three-way apply #602's hunks to the files at shared paths: `slayer/cli.py`, `slayer/api/server.py`, `slayer/mcp/server.py`, `slayer/flight/handlers.py`, `slayer/pg_facade/connection.py`, `slayer/engine/query_engine.py`, `slayer/engine/cache.py`, `slayer/usage_reporting/payload.py`. Verify that the diff adds lines only.
- [ ] 3.4 Re-apply the hooks by hand:
  - `slayer/sql/client.py`: `_db_stage_attributes` without the `statements` branch; `db.execute` on `_typed_execute`; `db.metadata` on `_metadata`; the `db.retry` event in `_log_transient_or_raise`.
  - `slayer/engine/ingestion.py`: `ingest` on `_ingest_idempotent`; `ingest.merge`, `ingest.scan.schema` and `ingest.embeddings`; `_ingest_stage_attributes`, `_ingest_outcome` and `_reports_change`.
  - `slayer/engine/schema_drift.py`: `ingest.drift` on `_validate_datasource`.

  Verify that 2.4, 2.5, 2.7, 2.11 and 2.12 pass.
- [ ] 3.5 `pyproject.toml`: the `otel` extra (`opentelemetry-api`, `opentelemetry-sdk` and `opentelemetry-exporter-otlp-proto-http`, each `>=1.45`, optional), also added to `all`. Run `poetry lock` on 1.x. Verify that the lock diff only adds OTel packages and their dependencies, and that `poetry install -E all` succeeds.
- [ ] 3.6 Architecture, applying design.md decision 5 verbatim:
  - the `model.c4` node and its three arrows;
  - `poetry run la-arch-diagrams`;
  - the `system.arc42.md` prose replacement.

  Verify that `poetry run la-arch-check` and `npx -y likec4@1.47.0 validate architecture` pass. Until this task lands, `la-arch-check` reports "spec group otel is mapped by no element"; that's expected, because the node can't exist before `slayer/otel/` does.
- [ ] 3.7 Docs:
  - `docs/reference/opentelemetry.md` from `M`, adapted. The database-span row drops `slayer.db.explain` and notes that EXPLAIN appears as `slayer.db.execute`. The sample `slayer otel status` output shows `slayer 1.8.5`.
  - The README "OpenTelemetry" section and the `zensical.toml` nav entry.
  - `examples/otel/*` from `M`, with `examples/otel/README.md` naming `slayer.sql.client`.

  Verify that the page is linked from the nav.

## 4. Gates, PR and archive

- [ ] 4.1 Run the local test scope from `la-affected-tests --base 1.x`, plus every 2.x test. Run `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors against the baseline), `poetry run la-arch-check`, `npx -y likec4@1.47.0 validate architecture` and `openspec validate dev-2358-backport-to-1x-opt-in-opentelemetry --strict`. All must pass.
- [ ] 4.2 Open the PR against `1.x` after the user's go-ahead.
- [ ] 4.3 After `openspec archive`, overwrite `openspec/specs/otel/{collection,controls,delivery}/spec.md` with `git show origin/main:openspec/specs/otel/<c>/spec.md`. Verify each one with `cmp` against `origin/main`, and confirm that `openspec validate --specs --strict` passes (design.md decision 15).
- [ ] 4.4 Check that the DEV-2358 comment "Forward-merge notes (1.x → main)" matches what shipped, and update it if any deviation changed.
