## Context

`_TerminationGuard` (`slayer/cli.py`) handles SIGTERM / SIGINT for the server commands
through phases `before → armed (⇄ serving) → closing → after`. Serving already runs in a
worker thread (`_serve_until_terminated`) so the main thread is never interrupted while
serving; startup still runs on the main thread in the `armed` phase, where `_handle` raises
`_Terminated` asynchronously. See proposal.md — Why.

Reproduced: raising inside `BaseSelectorEventLoop._make_self_pipe` during `asyncio.run`
leaves a half-built loop; its `__del__` → `close()` → `_close_self_pipe` raises
`AttributeError: '_UnixSelectorEventLoop' object has no attribute '_ssock'` — the CI
traceback frame for frame (3.11.15 and 3.12).

`slayer.cli` belongs to the `surfaces` bucket (no arc42 file); no `system.arc42.md` §3
principle is touched. Behaviour is already specified by `cli/server-shutdown` and
`usage-reporting/delivery`.

## Goals / Non-Goals

**Goals:**
- No exception is ever raised asynchronously into server-command code, in any phase.
- A signal during any startup step still ends the process at once with 128 + signum,
  spooling usage counted as ok.
- One waiting path for startup and serving.

**Non-Goals:**
- Running an abandoned startup worker's `finally` blocks (see Risks).
- Changing exit codes, usage counting, or the graceful / forced stop semantics.

## Decisions

1. **The command runs in one worker; the main thread only waits and reacts.**
   `_guarded_dispatch` runs `_dispatch` in one daemon worker thread `slayer-command`. The
   main thread waits (polling `_SIGNAL_POLL_SECONDS`, as `_wait_for`) for worker-done or
   signalled. Any `BaseException` from the worker is re-raised on the main thread, so the
   error door, `--debug`, `SystemExit(n)` and `KeyboardInterrupt` behave as today. A signal
   recorded before the worker starts (phase `before`, e.g. during
   `usage_reporting.start()`) means the command never runs: main returns normally and
   `finish` turns it into 128 + signum.
   Alternative: abort only at safe points between startup steps (the issue's proposal).
   Rejected: a signal during a blocking step (demo build, slow connect, heavy import) would
   wait for the step, contradicting `cli/server-shutdown` ("whatever the process is doing …
   starting up") and the `startup` scenario, and needs hand-placed per-call-site checks. (user)
2. **The handler never raises.** In every phase it records `signum`, calls the prior handler
   and sets the event; a second signal forces as today. Phases become
   `before → running → closing → after` (`armed` and `serving` merge into `running`).
   `_Terminated` and `except _Terminated` are deleted. `_force`'s record gate becomes
   `phase in ("before", "running")`. (default)
3. **Serving registers its stop under the guard lock; no server starts after a signal.**
   `_serve_until_terminated(run, stop)` runs in the command worker: under a guard lock it
   checks for a recorded signal (if one, it returns without running), else registers `stop`;
   then it calls `run()` directly, with no extra thread. Without a guard it just calls `run()`.
   `stop` keeps its existing contract of taking effect even when called before `run` starts
   (pg `_StoppableRun`, uvicorn `should_exit`, Flight `shutdown` before `wait`; pinned by the
   `early` scenario), so the window between registration and `run()` needs no lock.
   (default; register→run race finding rejected (codex) on this contract)
4. **On a signal, main stops or abandons.** A registered `stop` → `_stop_server(stop)`,
   then, if it returned, wait for the worker; if it failed, abandon the worker. No `stop`
   registered (startup, or stdio MCP's `stop=None`) → abandon the worker at once. Either way
   `_guarded_dispatch` returns normally, so `counting` records ok and the spool flushes; the
   guard records whether the worker was abandoned. (default)
5. **Abandoned worker ⇒ `_exit_now`; this replaces `finish(stdio=)`.** `finish` exits via
   `_exit_now(128 + signum)` whenever the worker was abandoned and `sys.exit(128 + signum)`
   when it was joined. Stdio MCP is just an always-abandoned worker on a signal; interpreter
   teardown with a live daemon worker is unsafe. (default)
6. **Full stderr in the traceback assertions** (`_assert_signal_stop`,
   `test_signal_during_startup_stops_and_spools`): `assert "Traceback" not in proc.stderr,
   proc.stderr`. (user, via the issue)
7. **No spec delta (`skip_specs: true`).** The behaviour is already specified; this restores
   conformance. (default)
8. **Docs unchanged.** `docs/reference/cli.md` and `docs/reference/usage-reporting.md` state
   only exit codes and counting, which do not change. (default)
9. **`serve_after_signal` must fail on the old code.** Its wait for the recorded signal is
   wrapped in `try/except BaseException: mark("RAISED_INTO_STARTUP"); raise`, and the test
   asserts the mark is absent, so the old asynchronous raise fails it. (codex)
10. **The guard-handler test covers first and second signals in every phase**, with a raising
    prior handler and `_exit_now` monkeypatched, asserting no raise into the caller, the prior
    handler called exactly once, and when a forced exit / usage record happens. (codex)
11. **Real-transport coverage is the existing suite.** `tests/test_usage_reporting_delivery.py`
    already starts all four real commands, reaches serving, signals them, and covers stdio
    MCP end of input; it joins the repeated-load verification. (codex, folded as verification)
12. **Rejected (codex): checkpoints in startup for the abandoned worker's side-effect window.**
    The abandoned worker may run on until `_exit_now`'s bounded usage send wait ends; that
    window is bounded, and decision 3 stops it from starting a server. Checkpoints are the
    rejected safe-point design.
13. **Rejected (codex): records made by an abandoned worker after the final flush.** Not new:
    stdio MCP is abandoned with a live worker today; startup records nothing.
14. **Rejected (codex): demo setup partially applied by an abandoned worker.**
    `ensure_demo_datasource(assume_yes=True)` re-saves the datasource and re-ingests the
    models on every start, so a partial setup heals on the next start.

## Risks / Trade-offs

- [An abandoned startup worker's `finally` blocks never run] → it is SIGKILL-equivalent at
  `_exit_now`; the demo DuckDB is built in a `.building` sibling and moved with
  `os.replace` (the next run deletes a stale sibling); a `jaffle_*` temp dir may leak
  (accepted). (default)
- [Startup code now runs off the main thread] → nothing in `slayer/` calls `signal.signal`
  or needs the main thread; `asyncio.run` and uvicorn already skip their signal setup off the
  main thread (serving runs there today).
- [The CI flake is timing-dependent] → verification runs the shutdown suites repeatedly under
  load on 3.11 and 3.12 (tasks.md).
