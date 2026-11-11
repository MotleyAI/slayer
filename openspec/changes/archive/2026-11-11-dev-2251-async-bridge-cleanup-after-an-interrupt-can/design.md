## Context

The proposal (Why) covers the motivation. Diagnosis, reproduced on Python 3.14.10 with a standalone replay of the
test and the process pinned to one CPU (`taskset -c 3`):

**A. Test-helper deadlock.** This is the CI "call hung" failure, which takes exactly 15.00 s.
`_interrupt_scheduling(after=started)` patches the bridge loop's `call_soon_threadsafe` and blocks on
`after.wait(_BOUND)` inside it. That call runs within `_Workflow.start()`'s `with bridge.lock:`, so the caller
waits for the bridge loop while it holds `bridge.lock`. The helper's own preceding `run_sync(_running_loop())`
finishes in `_transfer`, and `done.set_result` runs the done-callbacks in registration order:
- `serve.put(None)` runs first. It wakes the caller, which returns and enters the next `start()`, taking the lock.
- `_settled` runs next and takes `bridge.lock` on the bridge thread.

When the caller wins this race, the bridge thread blocks in `_settled`. It never runs `_run`, so `started` is
never set until the 15 s wait expires. A stack dump of a hung run confirmed it: the caller sat in
`_interrupted → started.wait` and the bridge thread in `_settled → with bridge.lock`. About 2 of 115 pinned runs
hung. Production is unaffected, because no production holder of `bridge.lock` waits on the bridge loop.

**B. Production overrun.** `_serve_caller_calls(bounded=True)` checks its deadline only through
`calls.get(timeout=max(0, deadline - now))`. `get(timeout=0)` still returns a call that is already queued. A
workflow that keeps queueing `on_caller_thread` work after cancellation therefore holds the interrupted caller
for as long as the queue is non-empty when the caller looks. Pinned runs overran a 0.5 s bound by up to 1.81 s,
serving 36 calls instead of about 10.

Principles that apply:
- system.arc42.md §3 P5 (async-first; sync entry points bridge via `run_sync`).
- `slayer.async_utils` is claimed by `python.core`, so core P7 applies (core imports no other node).

Both stay satisfied. The change adds no imports and edits no arc42 or `.c4` file.

## Goals / Non-Goals

**Goals:**
- An interrupted `run_sync` returns within `_CLEANUP_SECONDS`, plus at most the one caller-thread call already
  running at the deadline, however much caller-thread work the cancelled workflow keeps queueing.
- The interrupt-injection test helpers can never deadlock with the bridge loop.
- Every changed test fails promptly without its fix, with no test-local thread left running against
  monkeypatched state.

**Non-Goals:**
- Calls queued after the caller has left stay unserved. Their workflow task stays pending until it ends or
  bridge shutdown cancels it. This is a pre-existing property of an abandoned workflow that ignores
  cancellation, and it is not part of this issue.
- No change to `_settled` / `bridge.lock` in production.

## Decisions

1. **Fix A in the test helper, not in production** (default). Production never holds `bridge.lock` while it
   waits on the bridge loop. The alternative, a lock-free `_settled`, would replace a documented lock with
   reliance on set-operation atomicity.
2. **Split the helper by hook point** (default):
   - `_interrupt_scheduling(monkeypatch, *, scheduled)` patches the loop's `call_soon_threadsafe` for the
     workflow's own scheduling only, then raises KeyboardInterrupt without blocking. It raises before the real
     call when `scheduled=False`, or right after it when `scheduled=True`.
   - The new `_interrupt_after_start(monkeypatch, *, after)` wraps `async_utils._Workflow.start`. It runs the
     real `start()`, then waits for `after` with the lock released and raises KeyboardInterrupt, firing once.

   The alternative was a pre-run barrier that lets `_settled` finish first. It is insufficient: any other
   bridge-thread lock taker, such as a finalizer's `unregister_shutdown`, would deadlock the same way.
3. **Deterministic lock contention** (default). Before it arms, `_interrupt_after_start` queues a bridge-loop
   callback that briefly blocks and then takes `bridge.lock`, standing in for the previous workflow's
   `_settled`. Every use then exercises the race that hung CI. The old helper would hang under it
   deterministically.
4. **Strengthen the existing bounded-overall test rather than add a near-duplicate** (default). Its
   single-producer workflow is a racy subset of the new test.
5. **Deterministic never-empty caller queue** (codex). Two concurrent producers can still let the caller drain
   the queue before the bridge refills it. Instead, the workflow captures its caller queue
   (`async_utils._caller_calls.get()`). Each caller-thread call puts the next call straight onto that queue
   before it returns, so the caller always finds work.
6. **Tight bound** (codex). The test records when the interrupt fires and asserts the caller returns within
   `_CLEANUP_SECONDS + 1 s` of it. The 1 s covers the one 0.05 s call already running plus scheduler slack.
   The originally planned `< 5 s` check would have passed the observed 1.81 s overrun.
7. **Prompt negative case, no leftover thread** (codex). The self-requeueing chain stops requeueing after a
   test-local cap of 3 s from the interrupt. Without fix B, the caller therefore returns after about 3 s and
   fails the tight-bound assertion, instead of hitting the 15 s `_bounded` timeout and lingering past the
   monkeypatch teardown. The test's `finally` sets `stop`, and the workflow's own loop ends on `stop`.
8. **Cover schedule-then-raise before the workflow runs** (codex). Splitting the helper means the
   `after=started` tests no longer raise from inside `start()`. A new test,
   `test_an_interrupt_after_scheduling_before_the_workflow_runs_closes_it_unstarted`, covers that path.
   It blocks the bridge loop on an event and calls `_interrupt_scheduling(scheduled=True)`. It then asserts:
   - the interrupt reaches the caller promptly;
   - once the bridge is released, `_run` finds the workflow cancelled, so the coroutine is closed (`cr_frame` is
     None) and never ran;
   - the next `run_sync` succeeds.
9. **`skip_specs: true`** (default). Fix B restores conformance with the existing `database/execution`
   requirement. No specified behaviour changes.

## Risks / Trade-offs

- [The tight bound flakes on a heavily loaded CI runner] → The 1 s slack is 20× the in-flight call. The negative
  case fails by about 2.5 s, so the bound can be loosened without losing the guard.
- [The tests depend on private internals: `_Workflow.start`, `_caller_calls`, the `(func, future)` queue entry] →
  The test module already patches `_Workflow` and `_CLEANUP_SECONDS`. These are the bridge's own unit tests.
