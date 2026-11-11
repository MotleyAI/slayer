## 1. Tests (pr-tests stage) — `tests/test_async_utils_bridge.py`

- [x] 1.1 Replace `_interrupt_scheduling(monkeypatch, *, after)` with `_interrupt_scheduling(monkeypatch, *, scheduled)`. It patches the loop's `call_soon_threadsafe` for the workflow's own scheduling only and raises KeyboardInterrupt without ever blocking: before the real call when `scheduled=False`, after it when `scheduled=True` (design D2). Verify `test_an_interrupt_before_submission_closes_the_workflow_unstarted` uses `scheduled=False` and passes unchanged in substance.
- [x] 1.2 Add `_interrupt_after_start(monkeypatch, *, after)`. It wraps `async_utils._Workflow.start`: runs the real `start()`, then, with the lock released, waits for `after` and raises KeyboardInterrupt, once. Before arming, it queues a bridge-loop callback that blocks briefly and then takes `async_utils._bridge.lock` (design D2, D3). Verify `test_an_interrupt_just_after_submission_cancels_the_workflow` uses it and passes 50/50 pinned to one CPU (`taskset -c <n>`).
- [x] 1.3 Rewrite `test_cleanup_after_an_interrupt_is_bounded_overall` (design D4–D7):
  - The workflow captures its caller queue (`async_utils._caller_calls.get()`). Each caller-thread call sleeps 0.05 s and puts the next `(func, Future)` entry straight onto that queue, until a test-local cap of 3 s after the interrupt or until `stop` is set.
  - The workflow suppresses cancellation and ends on `stop`.
  - `_interrupt_after_start` fires after `started`, with `_CLEANUP_SECONDS=0.5`.
  - Assert the caller returns within `_CLEANUP_SECONDS + 1` s of the interrupt.

  Verify it FAILS on the unfixed `slayer/async_utils.py` in about 3 s (not 15 s).
- [x] 1.4 Add `test_an_interrupt_after_scheduling_before_the_workflow_runs_closes_it_unstarted` (design D8):
  - Block the bridge loop on an event and call `_interrupt_scheduling(scheduled=True)`.
  - Assert the KeyboardInterrupt reaches the caller promptly.
  - Release the bridge, then assert the coroutine is closed (`cr_frame is None`) and never ran, and the next `run_sync` returns.

  Verify it passes.

## 2. Implementation (pr-implement stage) — `slayer/async_utils.py`

- [x] 2.1 In `_serve_caller_calls(bounded=True)`, compute `remaining = deadline - time.monotonic()` before each `get`. Return when `remaining <= 0`, otherwise `get(timeout=remaining)`. Unbounded mode is unchanged. Verify task 1.3's test passes, and that it passes 50/50 pinned to one CPU. Delivered by the merged DEV-2231 `_next_call`.

## 3. Verification

- [x] 3.1 Run `tests/test_async_utils_bridge.py` 30× pinned to one CPU on Python 3.14 (`taskset -c <n>`) with no failure.
- [x] 3.2 Run the full unit suite (`poetry run pytest -m "not integration"`), `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` and `poetry run la-arch-check`; all green.
