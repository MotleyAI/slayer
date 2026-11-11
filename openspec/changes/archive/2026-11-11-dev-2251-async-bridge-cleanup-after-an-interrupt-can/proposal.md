## Why

`test_cleanup_after_an_interrupt_is_bounded_overall` hangs intermittently in full CI shards on Python 3.14. Both
causes reproduce with a process pinned to one CPU. The test helper deadlocks against the bridge loop on
`bridge.lock`. Separately, the production bounded cleanup keeps serving caller-thread calls past its deadline.
That breaks the `database/execution` requirement that an interrupted synchronous call returns "once that work's
cleanup has run or a bounded wait has passed".

## What Changes

- The production bound in `_serve_caller_calls` comes from DEV-2231's `_next_call`, merged in; this change adds no
  production code.
- `tests/test_async_utils_bridge.py`: the interrupt-injection helpers never block while `_Workflow.start()` holds
  `bridge.lock`. The test that interrupts after the workflow starts raises the interrupt once `start()` has
  returned, and deterministically exercises bridge-loop contention for the lock.
- The bounded-overall test uses a deterministically never-empty caller queue and a tight bound. A new test covers
  an interrupt that lands after scheduling but before the workflow runs.

## Capabilities

### New Capabilities

### Modified Capabilities

None: the fix restores conformance with the existing `database/execution` requirement "Synchronous calls reuse
connections". The change sets `skip_specs: true`.

## Impact

- Code: `tests/test_async_utils_bridge.py`.
- No API, dependency, architecture-model or arc42 change.
