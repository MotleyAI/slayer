## Why

`SlayerSQLClient` hands each blocking database call to a short-lived worker thread whose executor
is shut down with `wait=True`, so cancelling the awaiting task joins the worker on the event-loop
thread: `asyncio.wait_for` cannot bound a metadata read, `ping` or statement, and the whole loop
stalls until the driver gives up (storyline's 8 s connection probe overruns against a host that
accepts TCP but never answers).

## What Changes

- Cancelling a task that awaits a worker-dispatched blocking call of `SlayerSQLClient` raises the
  cancellation at once; the worker is abandoned and finishes on its own, and its outcome never reaches
  the caller or the loop's exception handler.
- An uncancelled call keeps its behaviour: it returns its own result or raises its own error and
  leaves no worker thread running.
- The other awaited thread hops in SLayer (`asyncio.to_thread` sites, `on_caller_thread`) were audited:
  none joins its worker on cancellation, so none changes.

## Capabilities

### New Capabilities

### Modified Capabilities
- `sql/execution`: adds the requirement that cancelling a client's worker-dispatched blocking call
  returns at once.

## Impact

- `slayer/sql/client.py` (`_run_sync_in_thread`).
- `docs/configuration/sql-clients.md` (one sentence on cancellation).
- New unit tests over in-process SQLite; no API, dependency or storage change.
