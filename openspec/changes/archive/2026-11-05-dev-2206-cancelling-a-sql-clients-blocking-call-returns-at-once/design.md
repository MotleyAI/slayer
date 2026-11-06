## Context

`slayer/sql/client.py::_run_sync_in_thread` is the one worker hop of `SlayerSQLClient`: metadata reads
and `ping` (`_metadata` → `_blocking`) and sync-in-thread statements
(`_execute_with_retry_threaded(run=self._blocking)`) over a client-owned or factory engine. It runs the
call inside `with ThreadPoolExecutor(max_workers=1)`, whose `__exit__` is `shutdown(wait=True)`, so a
cancelled await joins the worker on the event-loop thread. A probe confirmed it: a SQLite file held
under `BEGIN EXCLUSIVE` by another connection makes `list_objects(None)` and `execute(...)` bounded by
`wait_for(..., 0.2)` take 5.01 s (the SQLite busy timeout) with a concurrent ticker stalled.

Audit of the other awaited thread hops (the issue asks for it):
- `asyncio.to_thread` sites (`storage/sqlite_storage.py`, `storage/sidecar_embedding_store.py`,
  `storage/yaml_storage.py`, `search/graph.py`, `cli.py`) use the default executor and never join on
  cancellation: already prompt.
- `async_utils.on_caller_thread` awaits `asyncio.wrap_future`: already prompt.
- `run_sync` / `run_sync_on_caller_thread` are synchronous bridges, never awaited: nothing to cancel.

So the defect is local to one helper; no structural duplicate exists to fold.

Architecture: element `python.sql` (spec attach `sql` already in its model metadata). Applicable
principles: system §3.7 async-first; sql §3.14 one engine owner — ownership and disposal are unchanged,
an abandoned worker merely holds one pooled connection until its call returns. No arc42/.c4 edit.

## Goals / Non-Goals

**Goals:**
- A cancelled worker-dispatched blocking call raises the cancellation at once and never stalls the loop.
- The abandoned worker finishes on its own; its outcome is dropped silently, also after its loop closed.
- The uncancelled path keeps returning its own result/error and leaves no live worker.

**Non-Goals:**
- Interrupting the driver call itself; bounding the connect is DEV-2178.
- A caller-owned engine used directly from async code: `on_caller_thread` runs it inline on the loop
  thread by design (thread-bound pools), and an inline call cannot be interrupted. The spec requirement
  is scoped to calls over a datasource's own connection settings for that reason.
- Process exit still joins a non-daemon abandoned worker; the driver's own timeouts bound it.

## Decisions

1. **Keep the scoped single-worker executor**, not `asyncio.to_thread`: its documented reason
   (pytest-asyncio hangs on default-executor threads after SQLite tests) stands; fixing the shutdown
   suffices. (default)
2. **Cancellation shuts down with `wait=False, cancel_futures=True` and re-raises at once; every other
   exit shuts down with `wait=True`.** From the issue. (user — issue)
3. **Cleanup in a `finally` keyed on whether the await was cancelled**, so `KeyboardInterrupt`,
   `SystemExit` and any other `BaseException` still shut the executor down as today (Codex: the
   proposed `except CancelledError … else` left those paths unhandled). No dedicated test for
   `KeyboardInterrupt`: under `asyncio.run` SIGINT reaches the task as a cancellation, which the
   cancellation tests cover. (codex)
4. **Spec requirement ADDED to `sql/execution`**, not a MODIFIED "Live database access never blocks
   the event loop": cancellation semantics, dropped outcome and no leaked worker are their own
   contract. (default)
5. **Spec scoped to calls over a datasource's own connection settings** (client-owned and factory
   engines), so it does not over-promise the inline caller-engine path. (codex)
6. **Tests: deterministic barriers for the core semantics plus SQLite exclusive-lock cases through the
   public client API with a generous bound** (0.2 s timeout, assert well under the 5 s busy timeout);
   tests track the specific worker thread (captured inside the call) rather than enumerating threads
   by name. (codex)
7. **Worker threads named `slayer-blocking`** for observability only; not part of the specified
   behaviour and not a test mechanism. (codex)
8. **Closing a client right after a cancellation is tested** (returns without waiting or raising; the
   abandoned call ends). Tracking in-flight abandoned calls so `close()` waits for them is rejected:
   `close()` is synchronous and would reintroduce the stall; SQLAlchemy releases a checked-out
   connection of a disposed pool when it is returned, and a client-owned in-memory engine never blocks
   on another party. (codex, partially folded)
9. **Closed-loop completion is tested**: cancel, let the loop close, then release the worker; nothing
   reaches an exception handler and the worker ends. (codex)
10. **Retry interplay is tested**: cancellation during an attempt and during the backoff sleep starts no
    further attempt and reaches the caller untransformed (`CancelledError` is not an `Exception`, so
    `_RETRYABLE_ERRORS` and `_typed` never catch it). (codex)
11. **Cancellation before the call starts is tested** (the call never runs). The race "worker done
    but task not yet resumed" is not tested: it cannot be staged deterministically and is asyncio's own
    guarantee. rejected (codex)
12. **An abandoned call's authentication failure does not evict the engine**: the caller is gone, so
    `_typed` does not run; the next call over that engine meets the same rejection and evicts it then.
    Accepted and documented, no test. rejected (codex)
13. **Docs: one sentence** in `docs/configuration/sql-clients.md` "Lifecycle and concurrency". (default)

## Risks / Trade-offs

- An abandoned worker keeps one pooled connection checked out until the driver returns; a burst of
  timed-out calls against a black-hole host can exhaust a factory pool until the driver's timeouts
  fire → bounded by DEV-2178's connect timeouts.
- Wall-clock assertions can flake on loaded CI → generous bounds and barrier-based core tests.
