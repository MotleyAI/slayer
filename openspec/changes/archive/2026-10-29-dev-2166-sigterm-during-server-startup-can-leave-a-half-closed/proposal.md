## Why

A server command signalled during startup exits with the right status but can print an
`Exception ignored in: BaseEventLoop.__del__` traceback, breaking the "no traceback"
requirement of `cli/server-shutdown` and flaking
`test_signal_during_startup_stops_and_spools` on CI (Python 3.11). The cause: while startup
runs on the main thread, the signal handler raises an exception at whatever bytecode that
thread is executing; one landing inside an event loop's `__init__` leaves a half-built loop
whose `__del__` → `close()` raises. Any startup library code can be corrupted the same way.

## What Changes

- The main thread no longer runs a server command's code: the whole command (startup and
  serving) runs in one worker thread, and the main thread only waits and reacts to signals.
- The termination signal handler never raises, in any phase; `_Terminated` is removed.
- A signal during startup abandons the worker and exits at once with 128 + the signal
  number, spooling usage counted as ok; a server never starts after a signal is recorded.
- Shutdown tests print full stderr when a traceback assertion fails, and new tests pin that
  no exception is raised into startup code.

## Capabilities

### New Capabilities

### Modified Capabilities

None: `cli/server-shutdown` already requires a signal during startup to stop the command
without a traceback; this change makes the implementation conform (`skip_specs: true`).

## Impact

- `slayer/cli.py`: `_TerminationGuard`, `_run_command`, `_guarded_dispatch`,
  `_serve_until_terminated`.
- `tests/test_server_shutdown.py`, `tests/_server_shutdown_script.py`.
- No user-facing CLI, API or docs change.
