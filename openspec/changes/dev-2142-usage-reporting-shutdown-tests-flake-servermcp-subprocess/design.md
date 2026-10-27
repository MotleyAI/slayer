## Context

Root causes (faulthandler stack dumps of hung processes, 32–64 parallel processes):

- `_unwind_on` (`slayer/cli.py`) makes a signal raise `SystemExit` at whatever bytecode the main thread runs. For stdio `mcp` that is inside anyio's asyncio loop: inside an MCP task it escapes the TaskGroup as `BaseExceptionGroup([SystemExit])` and `asyncio.run` logs it through FastMCP's `RichHandler(rich_tracebacks=True)` (~29 KB stderr, ~1.7 s CPU); outside a task, `asyncio.run`'s `_cancel_all_tasks` waits on MCP's `stdin_reader`, which awaits a non-abandonable anyio worker thread blocked in `stdin.readline()` — a hang until stdin EOF (~1/64 under load). `_exit_stdio_now` only runs after `asyncio.run` returns.
- `flight-serve`: the gRPC port opens at `build_server`; pyarrow `serve()` installs its own SIGINT/SIGTERM handlers only inside nogil C++ (`ServeWithSignals`). A signal in that gap only trips Python's pending flag, never checked again → hangs forever (a re-sent signal unblocks it). A Flight server answers RPCs without `serve()`, and `shutdown()` releases a thread blocked in `wait()`.
- `pg-serve`: same raise-into-loop for SIGTERM; on Python 3.12 `async with server` → `wait_closed()` waits for every client connection, so an idle client blocks a graceful stop.
- `serve`: uvicorn's handlers only set `should_exit`; a repeated SIGINT forces, a repeated SIGTERM does not (`uvicorn/server.py` `handle_exit`). `uvicorn.run` swallows `KeyboardInterrupt` (exit 0).

Constraint: the model allows `surfaces → protocols`, not `protocols → surfaces`; `slayer/cli.py` (surfaces) injects helpers into `run_pg_serve` / `run_flight_serve` (protocols), as it already does with `resolve_storage`.

## Goals / Non-Goals

**Goals:** one shutdown path for all four server commands; signals never raise into a running server loop; deterministic tests.

**Non-Goals:** shutdown of non-server commands (`query`, `ingest`, …) — unchanged; interpreter-teardown time after the spool is written; a committed stress test (verification step only).

## Decisions

### D1 — Server off the main thread, main thread only waits
`_serve_until_terminated(*, run, stop)` in `slayer/cli.py` runs `run` (the server's blocking loop) in a daemon worker thread and joins it from the main thread. Signals are only ever handled by the main thread, which is then in `join()` — a defined point. The main thread joins with a periodic wake-up (0.1 s): the kernel may hand a process signal to any thread, and CPython runs the handler only once the main thread wakes. Alternative considered: per-runtime native handling (anyio signal receiver for MCP, `loop.add_signal_handler` for pg, a Python-level wait for Flight, uvicorn's own) — four mechanisms that each must be written right; rejected for the single path.

### D2 — Termination state machine
Handlers for SIGTERM and SIGINT are installed in `main()` right after `parse_args` for every server command (before `host_clock`, `usage_reporting.start`, storage, demo, server construction). `main()` runs the command as `with usage_reporting.counting(...): try: try: <arm>; _dispatch() finally: <closing> except <termination exception>: ...`, with both catch sites lexically inside the scope. Phases, in order: *before* (handlers installed → the arm statement), *armed* (→ that inner `finally`), *closing* (→ the scope has recorded), *after*.

- **First signal**: record the signal number, then call a callable pre-existing handler once (its exceptions swallowed — they cannot skip spool/exit; later signals do not call it). While *armed*, raise the private termination exception (a `SystemExit` subclass carrying 128+signum). One recorded *before* is raised by the arm statement. From *closing* on the run's outcome is committed: the signal only turns a normal end into exit 128+signum at the end of `main()`.
- **Further signal**: hard exit — mark the hard exit, count the command `ok` unless *closing* has begun (a no-op while recording is not yet active; the scope's own count can no longer run), flush the spool, bounded send wait (`usage_reporting.shutdown`), flush stdio, `os._exit(128+signum)` (the existing `_exit_stdio_now` path). While *closing* it is only recorded (no server stop or join runs then). A signal during the hard exit calls `os._exit(128+signum)` at once.
- In `_serve_until_terminated`, catching the termination exception → `stop()` exactly once, in its own thread while the main thread keeps waiting (none for `mcp`) → join the worker → re-raise. The phase stays *armed* throughout, so a further signal during startup unwind, `stop` or re-join hard-exits.
- An inherited `SIG_IGN` disposition is left in place (no handler installed). `SIG_DFL` is never called as a handler.

Alternatives considered: entering the counting scope before `usage_reporting.start`, or reordering/shielding `start()` — rejected: `start()` prints the first-run notice before it activates recording, and a raise out of it is never counted.

### D3 — Precedence of outcomes
A termination signal recorded before *closing* (D2) decides the exit status (143/130) and the count (`ok`) even when the worker also failed; from *closing* on the outcome is committed. With no signal, a worker `BaseException` (`Exception`, `SystemExit`, `KeyboardInterrupt`) is re-raised on the main thread with its normal diagnostics. `run` returning normally → normal exit (stdio EOF → 0).

### D4 — Per-command `run` / `stop`
- `serve`: `run = uvicorn.Server(uvicorn.Config(app, host, port)).run`; off the main thread uvicorn's `capture_signals` installs nothing. `stop = should_exit = True`.
- `mcp`: `run = mcp.run`; no `stop` — the stdin reader thread cannot be stopped, so a signal goes straight to the hard-exit path (spool, `os._exit(128+signum)`).
- `pg-serve`: `run` is `asyncio.run` of an async main that registers its loop and task; `serve()` tracks connection tasks. `stop` (thread-safe; a stop before the loop exists is honoured at loop start) closes the listener and cancels and awaits the connection tasks. Bind-validation `ValueError → SystemExit(2)` kept; `except KeyboardInterrupt: pass` dropped.
- `flight-serve`: the server is built (bound) on the main thread; `run = server.wait`, `stop = server.shutdown` (called at most once). `serve()` is no longer used.

### D5 — Usage counting of a signal stop
The CLI catches the termination exception *inside* the `usage_reporting.counting` scope, so the scope records `ok`, then exits 128+signum after leaving it (for `mcp` via the hard-exit path, after flushing). A signal during `host_clock` or `usage_reporting.start` is raised by the scope's arm statement, and a hard exit before *closing* records the `ok` itself (D2), so every signal stop is counted once. Ordinary `SystemExit`, startup failures and short-command interrupts keep today's error tokens. `slayer.usage_reporting` stays CLI-agnostic.

## Risks / Trade-offs

- [A pre-existing handler that blocks runs on the main thread] → not guardable; documented by its single call.
- [Daemon worker abandoned on hard exit] → intended: hard exit happens only on a second signal or for stdio MCP, after the spool is written.
- [Tests stubbing `uvicorn.run` break] → stubs switch to `uvicorn.Config` / `uvicorn.Server` (mechanical, user-approved).
- [Behaviour change: SIGINT exit 0 → 130 for `serve` / `pg-serve`] → user-approved; consistent with shell convention.
