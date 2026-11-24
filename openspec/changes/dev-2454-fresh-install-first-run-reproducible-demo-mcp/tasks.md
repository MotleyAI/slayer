## 1. Tests first (pr-tests stage)

- [x] 1.1 Determinism tests (`tests/` demo area): CSV hashes equal across two separate seeded child processes started from different working directories; UUIDs come from a dedicated RNG (UUID draws do not shift jafgen's `random` stream); both `generate_data` output branches (inherited fd and pumped `StringIO`) produce identical data; jafgen not importable → error naming jafgen and no data generated. Verify: tests fail on the current code.
- [x] 1.2 Stamp tests: unstamped DB rebuilt; stamp with another jafgen / Faker / recipe rebuilt; up-to-date reused; explicit `--years` 2→4 and 4→1 rebuild; implicit years (`mcp --demo`, `datasources create demo` without `--years`) reuse a 4-year DB; `slayer ingest --datasource jaffle_shop --all-schemas` on a stamped DB reports no model / skipped / hidden entry for the stamp. Keep demo builds cheap (stub generation or `years=1` where the data is irrelevant).
- [x] 1.3 Documented-count test: on the shared per-run demo fixture, run the CLI count query and assert its output equals the expected-output block in `docs/getting-started/cli.md` (read from the doc). Not marked integration.
- [x] 1.4 Readiness-gate tests over real stdio with a stubbed, event-blocked preparation: `initialize` + `tools/list` answer while blocked; a held `query` returns rows after release; progress notifications for a call with a progress token, none without; concurrent tokens; a failing notification does not fail preparation; cancelling one of two waiting calls leaves preparation running and the other succeeding; demo failure → every call returns `Failed to set up the Jaffle Shop demo: …` and stderr has the reason; startup ingest with a failing datasource and with table-level errors does not block or fail calls; no preparation → no waiting; nothing written to stdout; `serve --demo` still prepares before serving. Event-loop affinity: startup ingest with an async datasource client does not leak loop-bound resources into the serving engine.
- [x] 1.5 Ingest-renderer tests: golden output identical for one non-empty result across `slayer ingest`, `datasources create <url> --ingest`, `datasources create demo --ingest` and the startup orchestrator; demo create prints `Created: <model>` per model and no `Ingested:`; demo ingest exception → one `Ingestion failed:` line, exit 1; `slayer ingest` keeps its stricter exit rules and empty-result message; `_prepare_demo`'s "Demo ready … N model(s)" line and `ensure_demo_datasource`'s return keep working.
- [x] 1.6 Validation-error tests: two unknown keys named with reasons on one line; nested list path dotted with its index; >10 errors capped with `…and N more`; a secret-looking rejected value is not echoed; a multi-line message stays on one line; `--debug` still prints the traceback; the database-driver scenario keeps its class + `--debug` hint.
- [x] 1.7 Release-gate tests (`tests/test_release_gate.py`): a slow MCP handshake (>10s for `initialize` + `tools/list`) fails the gate naming the handshake check; a fast handshake with a slow first call passes within the per-call timeout.

## 2. Deterministic demo

- [ ] 2.1 Seeding bootstrap in `slayer/demo/jaffle_shop.py` (`generate_data` / `_jafgen_cmd`): seeded `random` and `np.random`, dedicated-RNG `uuid.uuid4` replacement, `PYTHONHASHSEED=0` in the child env, then `jafgen.cli.app()`; remove the `shutil.which("jafgen")` fallback. Verify: 1.1 passes.
- [ ] 2.2 Pin `jafgen = "0.4.14"` in `pyproject.toml` with a one-line comment that the demo data depends on this exact version; `poetry lock` (no unrelated upgrades). Verify: `poetry check --lock` passes.
- [ ] 2.3 Generator stamp as the DuckDB scalar macro `_slayer_demo_stamp()` (jafgen version, Faker version, seed-recipe constant, years), written into the sibling DB before the swap; `build_jaffle_shop` rebuilds on schema / stamp mismatch and on a years mismatch only for explicit years; `datasources create demo --years` default becomes unset (2 when building). Verify: 1.2 passes.
- [ ] 2.4 Update `docs/getting-started/cli.md`'s expected-output block to the real output. Verify: 1.3 passes on 3.11 and 3.14 in CI.

## 3. MCP readiness gate

- [ ] 3.1 Implement the preparation job + gate in `slayer/mcp/server.py` per design.md decision 4 (lifespan-owned task, `anyio.to_thread` for blocking work, dedicated preparation engine closed with `aclose()`, `asyncio.shield`, request-local progress, explicit Pydantic outcome state); move startup ingest out of the synchronous factory into the job; `_run_mcp` passes the demo setup into the job instead of calling `_prepare_demo` first. Verify: 1.4 passes.
- [ ] 3.2 Docs: replace the `docs/getting-started/cli.md:32` "kept small so … startup timeouts" claim (handshake answers at once, demo builds in the background, the first tool call waits); adjust `slayer mcp --ingest-on-startup` wording wherever docs describe it, keeping `serve`'s "before the port opens". Verify: `grep` finds no stale claim.
- [ ] 3.3 Release gate (`scripts/release_gate.py`): time `initialize` + `tools/list` against a 10s limit as its own named check; keep the 300s per-call timeout. Verify: 1.7 passes.

## 4. One ingest output

- [ ] 4.1 One shared renderer for an ingest result (additions + all report sections) used by `_ingest_and_print`, `ingest_all_datasources_idempotent` and the demo create path; `ingest_demo_models` returns a Pydantic result with the ingest result and the enriched models; `_run_datasources_create_demo` prints via the renderer and drops the `Ingested:` line. Verify: 1.5 passes.

## 5. CLI validation errors

- [ ] 5.1 Render pydantic `ValidationError` in `_error_line` per design.md decision 14. Verify: 1.6 passes.

## 6. Docs

- [ ] 6.1 `docs/getting-started/mcp.md:63`: name the `datasources/` folder under the storage path for Linux, macOS and Windows, keeping the storage-page link. Verify: matches `docs/configuration/storage.md`.

## 7. CI

- [ ] 7.1 Remove the rossbowen jafgen fork install from `ci.yml` (both unit jobs and `JAFGEN_SHA`), `latest-deps.yml` and `pg-facade-e2e.yml`. Verify: `grep -rn rossbowen .github` is empty.
- [ ] 7.2 `latest-deps.yml`: clean venv on Python `["3.11", "3.14"]`; one `pip install ".[all]" <dev tools from [tool.poetry.group.dev.dependencies]>` resolve without the lockfile; `pip check`; print `pip freeze`; run the unit suite. Verify: the workflow runs green on the PR (it is path-triggered by its own change).
- [ ] 7.3 Fix every test or notebook that depended on fork-shaped demo data (CI failures after 7.1); re-execute affected notebooks with saved outputs. Verify: CI unit and notebook jobs green.

## 8. Finish

- [ ] 8.1 Lint (`poetry run ruff check slayer/ tests/`), type gate (`la-typecheck`, no new errors), `la-arch-check`, `npx -y likec4@1.47.0 validate architecture`, and the local test scope. Verify: all green.
