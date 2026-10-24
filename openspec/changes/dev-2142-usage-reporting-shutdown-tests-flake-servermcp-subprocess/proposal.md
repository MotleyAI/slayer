## Why

Server commands sometimes do not exit after SIGTERM/SIGINT (DEV-2142). Stdio `slayer mcp` raises `SystemExit` into its anyio event loop: usually that dumps a ~29 KB rich traceback, occasionally `asyncio.run` waits forever on the uncancellable stdin reader thread. `flight-serve` loses a signal that lands before pyarrow's `serve()` installs its own handlers. `pg-serve` raises into its loop the same way, and its graceful stop waits for idle clients on Python 3.12. `serve` (uvicorn) cannot be forced down by a second SIGTERM. Exit codes and usage counting on signal stop differ per command.

## What Changes

- One CLI-owned shutdown path for every server command (`serve`, `mcp`, `pg-serve`, `flight-serve`): the server's blocking loop runs in a worker thread; the main thread only waits; a termination signal calls the server's own stop and never raises into server code.
- SIGTERM and SIGINT handlers installed as soon as a server command is parsed, chaining any callable handler already installed and leaving an inherited `SIG_IGN` ignored.
- **BREAKING** Every server command stopped by SIGTERM/SIGINT exits 143/130 with no traceback (`serve`/`pg-serve` SIGINT exited 0; `flight-serve` SIGINT died by signal with a traceback).
- A second SIGTERM/SIGINT during a graceful stop spools and exits at once.
- `flight-serve` no longer blocks in pyarrow `serve()`; `pg-serve` closes client connections on stop; `serve` runs `uvicorn.Server` off the main thread.
- A server command stopped by a signal counts as a successful run in usage reporting.

## Capabilities

### New Capabilities
- `cli/server-shutdown`: how server commands react to termination signals — exit status, no traceback, graceful stop, forced stop, stdio MCP exit.

### Modified Capabilities
- `usage-reporting/delivery`: SIGTERM spooling covers every server command, and a signal-stopped server counts as `ok`.

## Impact

- Code: `slayer/cli.py` (signal handling, shared shutdown helper, `serve`/`mcp` runners, usage counting of signal stop), `slayer/flight/cli.py`, `slayer/pg_facade/server.py`.
- Tests: `tests/test_usage_reporting_delivery.py`, `tests/_usage_reporting_helpers.py`, new shutdown-contract tests; uvicorn stubs in `tests/test_startup_ingest.py` and `tests/test_usage_reporting_features.py` switch to `uvicorn.Config`/`uvicorn.Server`.
- Docs: `docs/reference/usage-reporting.md`, server CLI docs (exit codes).
- Architecture: `architecture/index.yaml` attaches the new `cli` spec domain as cross-cutting over `python.surfaces` and `python.protocols`.
