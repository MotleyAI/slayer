## 1. Tests on main, before the merge (pr-tests)

Port each test from `origin/egor/dev-2358-backport-to-1x-opt-in-opentelemetry` (commits `42c82f0d9`, `da62128e7`, `5289a208a` and the backport)
in `main`'s vocabulary and paths (`slayer.database.client`, `population` / `metrics`, `DatasetNotFoundError`,
`/datasets/{name}`).

- [ ] 1.1 `tests/test_otel_guards.py`: `_GatedProvider`, `_export_text_hidden`, `_IterHook`; `test_exporter_log_text_is_shown_again_once_a_late_shutdown_ends`, `test_a_restart_after_uninstrument_closes_the_first_providers`, `test_a_restart_after_uninstrument_detaches_the_first_log_handlers`, `test_a_shutdown_thread_that_cannot_start_keeps_the_bound_and_stays_pending`, `test_a_later_reset_retries_a_shutdown_thread_that_could_not_start`, `test_an_earlier_shutdown_ending_during_the_handoff_keeps_the_text_hidden`; `test_concurrent_instrument_installs_one_listener` asserts `(1, True)` inside `try` before `otel.uninstrument()` in `finally`; `test_exporter_log_text_stays_hidden_while_a_shutdown_is_still_running` sets `recorder._export_text_hidden = True` (aligned to the fix). Verify: each new assertion fails on `main`.
- [ ] 1.2 `tests/test_otel_controls.py`: `test_test_sends_the_span_to_the_endpoint_it_probed`, `test_exporters_send_where_the_diagnostics_point`, `gc.collect()` before the redirect test's warning capture. Verify: the two endpoint tests fail on `main`.
- [ ] 1.3 `tests/test_otel_scope.py`: `test_an_uncaught_rest_failure_is_a_500_request` (fails on `main`); the Flight schema, Flight query and PG query request-span tests (coverage; expected to pass on `main`).
- [ ] 1.4 `tests/test_otel_collection.py`: `test_database_attributes_name_the_cached_sync_engine` and `test_database_attributes_name_the_loops_async_engine` (fail on `main`); `test_a_synchronous_call_inside_a_running_loop_keeps_its_parent` (coverage); the four EXPLAIN tests adapted to `slayer.db.explain`: exactly one database span, `slayer.db.explain`, kind `CLIENT`, child of `slayer.query`, with `db.system.name`, `db.namespace`, `db.response.returned_rows` (fails on `main`: no row count); no `slayer.query.run`; no `db.query.text` by default; `db.query.text` starting with `EXPLAIN` under content capture.
- [ ] 1.5 `tests/test_dev2074_save_ambiguity.py::TestEdgeSetChangeWarnings::test_warning_names_the_callers_line` in `main`'s helper vocabulary (coverage).

## 2. The merge (pr-implement)

- [ ] 2.1 `git merge --no-ff origin/egor/dev-2358-backport-to-1x-opt-in-opentelemetry` (or `origin/1.x` if #605 has merged), message first line `Merge branch 'origin/<ref>' into egor/dev-2365-merge-1x-into-main-the-otel-backport-dev-2358`. Verify: the merge commit has both parents.
- [ ] 2.2 Take the `1.x` side verbatim: `slayer/otel/{recorder,exporters,control,middleware}.py`. Verify: `git diff 5289a208a -- slayer/otel` is empty.
- [ ] 2.3 Take `main`'s side (notes 1–9, 14–17): `architecture/*`, `docs/concepts/ingestion.md`, `docs/reference/{cli,opentelemetry}.md`, `examples/otel/*`, `pyproject.toml`, `slayer/api/server.py`, `slayer/async_utils.py`, `slayer/core/deprecation.py`, `slayer/engine/{cache,query_engine}.py`, `slayer/engine/{column_dependency,ingestion,schema_drift}.py` and `slayer/sql/client.py` (stubs), `slayer/flight/handlers.py`, `slayer/mcp/server.py`, `slayer/storage/{base,v3_migration}.py`, `openspec/specs/otel/*`, `tests/conftest.py`, `tests/_dev2210_fixtures.py`, `tests/_dev2216_fixtures.py`, `tests/test_dev2073_surfaces.py`, `tests/test_migrations.py`, `tests/test_otel_{isolation,logs,privacy}.py`, `tests/test_probes_decorator.py`, `tests/test_library_name_shadow_warnings.py`, `tests/test_one_driver_per_client.py`, `tests/test_osi_roundtrip.py`. Verify: `git diff origin/main -- <each>` is empty; `git diff origin/main -- architecture/` is empty.
- [ ] 2.4 `tests/test_otel_{guards,controls,scope,collection}.py` and `tests/test_dev2074_save_ambiguity.py` resolve to the section 1 versions. Verify: no 1.x vocabulary (`source_model`, `measures`, `ModelNotFoundError`, `/models/`, `slayer.sql`) left in them.
- [ ] 2.5 Port `connection_url()` and `_running_loop_async_engine()` from 1.x `slayer/sql/client.py` into `slayer/database/client.py`. Verify: 1.4's two `connection_url` tests pass.
- [ ] 2.6 Add `result=_returned_rows` to the `db.explain` probe in `slayer/database/client.py`. Verify: 1.4's EXPLAIN span test passes.
- [ ] 2.7 Audit every clean auto-merge: `git diff origin/main --stat` after resolving; each remaining path must be in proposal.md's Impact, or an added archive folder. In particular `slayer/{cli.py,flight/server.py,pg_facade/connection.py,usage_reporting/payload.py,storage/document_loading.py}`, `README.md`, `zensical.toml`, `tests/pg_facade/test_connection.py` must equal `main`'s.

## 3. DEV-2314 on main

- [ ] 3.1 `examples/comparisons/slayer/run_slayer.py`: the `osi_roundtrip` mode and the expectation checks (reject expectations the runner would skip; `same_models` alone; dry-run steps take text checks; expected 0 counts) in `main`'s vocabulary. Verify: `tests/test_comparison_matrix.py` passes.
- [ ] 3.2 `examples/comparisons/probes.yaml`: the five C23 round-trip probes in `main`'s query and model vocabulary; Q22 / Q27 / C9 probes stay `main`'s.
- [ ] 3.3 `examples/comparisons/slayer/models/customers.yaml` stays `main`'s except the `tags` field gains `db_type: 'VARCHAR[]'`; `matrix.yaml` keeps `main`'s Q27 caveat ("List fields …"); C23 = `'yes'` and C4 = "9 engines tested live, 4 more by SQL generation." from `1.x`.
- [ ] 3.4 `tests/test_comparison_matrix.py`: `1.x`'s new tests in `main`'s vocabulary, `main`'s existing tests kept.
- [ ] 3.5 Run the SLayer probe runner on the C23 probes against `main`; every one passes. Regenerate `matrix.json` with `examples/comparisons/export_matrix.py`; its `--check` passes and probe links point at `main`.

## 4. Packaging

- [ ] 4.1 Run `poetry lock` on the merge result, then `poetry check --lock`. Verify: both succeed and `pyproject.toml` equals `main`'s.

## 5. Gate

- [ ] 5.1 Every `tests/test_otel_*.py` and `tests/pg_facade/test_connection.py`, serially and with `-n 8`; the section 1 tests; `la-affected-tests` scope; `tests/test_comparison_matrix.py`; `tests/test_forward_merge_specs.py`. Verify: all pass.
- [ ] 5.2 `poetry run ruff check slayer/ tests/`, `la-typecheck` (no new errors), `poetry run la-arch-check`, `openspec validate dev-2365-merge-1x-into-main-the-otel-backport-dev-2358 --strict`. Verify: all clean.

## 6. Before the PR opens

- [ ] 6.1 Wait until #605 is merged into `1.x` and its OpenSpec change archived; then `git merge origin/1.x` (message quoting `'origin/1.x'`). Resolve by the same rules; re-run section 5.
- [ ] 6.2 `poetry run python -m scripts.forward_merge_specs --base origin/main --head HEAD --one-x origin/1.x`; add a ledger entry (title, `1.x` hash, reason "main keeps its own wording: …") to `openspec/forward-merge-ledger.yaml` for each 1.x OTel requirement it names. Verify: the check passes.
