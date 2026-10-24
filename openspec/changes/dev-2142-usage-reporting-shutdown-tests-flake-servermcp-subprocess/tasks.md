## 1. Tests (pr-tests stage; each must fail on today's code)

- [ ] 1.1 Subprocess contract tests for `_serve_until_terminated` (design D1–D3), driven via `run_python` scripts: first signal before `run` starts blocking; during `join` while `run` blocks in code that never checks Python signals; during a stuck `stop` (second signal, SIGTERM and SIGINT, force-exits with 128+signum and spools); after `run` returned; racing a worker failure (signal wins). `stop` called exactly once. Worker `Exception` / `SystemExit` / `KeyboardInterrupt` without a signal re-raised with normal diagnostics. Pre-installed handler called once, including one that raises; inherited `SIG_IGN` stays ignored. Verify: tests fail now (helper absent), pass after section 2.
- [ ] 1.2 stdio `mcp` SIGTERM/SIGINT tests in `tests/test_usage_reporting_delivery.py`: rc 143/130, stderr has no `Traceback` and no `unhandled exception`, `mcp:list_datasources` ok == 1, `cli:mcp` ok == 1, no error. Keep `test_stdio_mcp_sigterm_chains_existing_handler` asserting rc 143. Verify: fails now (traceback, `exit` error).
- [ ] 1.3 Extend `test_server_shutdown_spools` to `serve` / `pg-serve` / `flight-serve` × SIGTERM / SIGINT: rc 128+signum, `cli:<command>` ok == 1 with no error, no traceback on stderr (stderr captured, not DEVNULL). Show the user the exact test diff before it lands (existing test logic changes — approved in pr-plan). Verify: fails now (`serve`/`pg-serve` SIGINT rc 0, flight SIGINT rc −2 + traceback).
- [ ] 1.4 Startup-signal test: a server command signalled during startup exits 143 and spools; synchronise on the first-run usage NOTICE on stderr (printed after handlers are installed), not on `Popen` return. Verify: fails now for at least one command or on the rc.
- [ ] 1.5 `pg-serve` with an established idle client connection: SIGTERM → rc 143, spooled. Verify: hangs past the wait today (Python 3.12 `wait_closed`).
- [ ] 1.6 Tighten existing stdio tests (approved in pr-plan): `test_stdio_mcp_end_of_input_spools` asserts rc 0 and `cli:mcp` + `mcp:list_datasources` ok; `test_stdio_mcp_broken_pipe_spools` asserts the run is not recorded as a signal stop. Show the exact diff first.
- [ ] 1.7 Switch the `uvicorn.run` stubs in `tests/test_startup_ingest.py` and `tests/test_usage_reporting_features.py` to stub `uvicorn.Config` / `uvicorn.Server` (mechanical seam change, approved in pr-plan); keep every assertion (e.g. `create_app` before the server runs).

## 2. Shutdown path in `slayer/cli.py`

- [ ] 2.1 Replace `_unwind_on` with the D2 state machine: install SIGTERM/SIGINT handlers in `main()` right after `parse_args` for every server command; chain callable prior handlers once (exceptions swallowed); leave inherited `SIG_IGN`; second signal → flush, bounded send wait, `os._exit(128+signum)`. Verify: 1.1 chaining / `SIG_IGN` / second-signal cases pass.
- [ ] 2.2 Add `_serve_until_terminated(*, run, stop)` (D1, D3). Verify: all 1.1 cases pass.
- [ ] 2.3 Count a signal stop as ok (D5): catch the termination exception inside `usage_reporting.counting`, exit 128+signum after it; `mcp` exits via the hard-exit path after flushing. Verify: 1.2, 1.3 ok-count assertions pass; short-command interrupt tests unchanged.
- [ ] 2.4 `mcp` through the helper (`run = mcp.run`, no `stop`). Verify: 1.2, 1.6 pass.
- [ ] 2.5 `serve` through the helper with `uvicorn.Server` / `uvicorn.Config` (`stop` sets `should_exit`). Verify: 1.3 `serve` cases and 1.7 pass.

## 3. Protocol servers

- [ ] 3.1 `slayer/flight/cli.py`: take the injected helper; build the server on the main thread; `run = server.wait`, `stop = server.shutdown` (once); no `serve()`. Verify: 1.3 flight cases pass.
- [ ] 3.2 `slayer/pg_facade/server.py`: take the injected helper; track connection tasks in `serve()`; thread-safe `stop` closes the listener and cancels and awaits connection tasks (stop before loop start honoured); keep `ValueError → SystemExit(2)`; drop `except KeyboardInterrupt: pass`. Verify: 1.3 pg cases and 1.5 pass; existing pg-facade tests pass.

## 4. Docs and architecture

- [ ] 4.1 `docs/reference/usage-reporting.md`: one sentence — a server stopped by a signal counts as a normal run.
- [ ] 4.2 `docs/reference/cli.md`: one sentence — server commands exit 143 on SIGTERM and 130 on SIGINT; a second signal forces the stop.
- [x] 4.3 `architecture/index.yaml`: add `cli: {touches: [python.surfaces, python.protocols]}` under `cross_cutting_specs` (user-approved in pr-plan). Verify: `poetry run la-arch-check` green.

## 5. Verification

- [ ] 5.1 `poetry run pytest -m "not integration"` green.
- [ ] 5.2 `poetry run ruff check slayer/ tests/` and `poetry run basedpyright` (no baseline growth) green.
- [ ] 5.3 20 consecutive `poetry run pytest tests/test_usage_reporting_delivery.py -n auto` runs all pass, timeouts unchanged.
