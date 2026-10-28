## 1. Tests (pr-tests)

- [x] 1.1 In `tests/test_server_shutdown.py`, make `_assert_signal_stop` and `test_signal_during_startup_stops_and_spools` assert `"Traceback" not in proc.stderr, proc.stderr` (design 6); verify by reading the diff.
- [x] 1.2 Add script scenario `startup_raised_into` to `tests/_server_shutdown_script.py` (startup signals itself inside `try: _forever.wait(10) except BaseException: mark("RAISED_INTO_STARTUP"); raise`) and a test over both signals expecting exit 128 + sig, no traceback, no `RAISED_INTO_STARTUP`, `cli:serve` spooled ok; verify it FAILS on the current code.
- [x] 1.3 Add script scenario `serve_after_signal` (the startup worker signals itself, waits inside `try/except BaseException: mark("RAISED_INTO_STARTUP"); raise` until the guard has recorded the signal, then calls `_serve_until_terminated` with `run`/`stop` marking `RUNNING`/`STOP`) and a test over both signals expecting exit 128 + sig, no `RUNNING`, `STOP` or `RAISED_INTO_STARTUP` (design 3, 9); verify it FAILS on the current code.
- [x] 1.4 Add an in-process guard-handler test over phases `before`, `running`, `closing`, `after`: first and second signal, with a raising prior handler, `_exit_now` monkeypatched; assert `_handle` never raises into the caller, the prior handler is called exactly once, forced exit happens on the second signal outside `closing`, and the usage record happens only in `before` / `running`; handlers restored afterwards (design 2, 10); verify it FAILS on the current code.
- [x] 1.5 Update `test_guard_stays_installed_after_signal_races_restore` mechanically to the new `finish` signature (design 5; signature only, no logic change).
- [x] 1.6 Retarget the signal-inside-worker-start test at the `slayer-command` worker start (script flag `--signal-inside-command-start`, scenario `join`); expect exit 128 + sig, no traceback, `cli:serve` ok, no `RUNNING` / `STOP` (design 1, 3); verify it FAILS on the current code.
- [x] 1.7 Add an in-process `finish()` test: with `abandoned` set it calls `_exit_now(128 + signum)`, otherwise it raises `SystemExit(128 + signum)` (design 5); verify it FAILS on the current code.

## 2. Implementation (pr-implement)

- [ ] 2.1 `_TerminationGuard`: phases `before → running → closing → after`; `_handle` never raises; delete `_Terminated`; `_force` record gate `phase in ("before", "running")`; guard lock + registered `stop` + abandoned flag (design 2–4); verify 1.4 passes.
- [ ] 2.2 `_guarded_dispatch`: run `_dispatch` in one daemon worker `slayer-command`; main waits for done / signalled; re-raise worker `BaseException`; skip dispatch when a signal was recorded before start; stop-or-abandon on a signal (design 1, 4); verify 1.2 and the existing `startup`, `worker_*` and `signal_*` scenarios pass.
- [ ] 2.3 `_serve_until_terminated`: under the guard lock refuse to run after a recorded signal, else register `stop`; then call `run()` in the calling thread; no guard → `run()` (design 3); verify 1.3 and the `early`, `stuck_stop`, `failing_stop*`, `slow_unwind` scenarios pass.
- [ ] 2.4 `finish`: `_exit_now` when the worker was abandoned, `sys.exit` when joined; remove the `stdio` parameter and its call-site argument in `_run_command` (design 5); remove `except _Terminated` in `_run_command`; verify 1.5 and the stdio MCP tests in `tests/test_usage_reporting_delivery.py` pass.
- [ ] 2.5 Update the `_TerminationGuard` / `_serve_until_terminated` docstrings to the new phases (one line each).

## 3. Verification

- [ ] 3.1 Full unit suite green: `poetry run pytest -m "not integration"`.
- [ ] 3.2 Load verification on Python 3.11 and 3.12: run `tests/test_server_shutdown.py` and `tests/test_usage_reporting_delivery.py` repeatedly (≥ 20 rounds each) under `-n logical` alongside the full unit suite; no `Traceback` in any shutdown test's stderr, no failures (design 11; the issue's done-when). Record the counts in the PR description.
- [ ] 3.3 `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `poetry run la-arch-check` all clean.
