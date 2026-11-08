## Context

See proposal.md — Why. The failing interleaving, reproduced with a scratchpad-only probe (a pytest plugin delaying `spool.write` and stalling the first sender right after `spool.claim`; the test then fails `assert 0 == 1` after the 30 s `wait_for`):

1. `quiet_config(last_sent=_T0 + 30 days)` precedes `_spool_one()`, so the setup's `start()` sees `is_due` (future last send) and spawns sender A.
2. A acquires the lease (expires = injected now + 5 min) and claims the spool before `end_process()`'s flush has written it — an empty claim, so nothing is posted and `last_sent` is not updated.
3. `end_process()` calls `reset()`, which forgets but does not join A (a daemon thread).
4. The act step's `start()` is still due and spawns sender B; B's `acquire_lease` sees A's live lease and returns `None`.
5. No request reaches the capture server.

Without the probe the window is narrow (128 parallel runs under 32 CPU burners: 0 failures), consistent with a single observed failure.

The issue's two hypotheses are both wrong: `wait_for`'s 30 s is ample, and the injected clock is not involved — two senders race for the lease.

Production behaviour is correct and specified (`openspec/specs/usage-reporting/delivery/spec.md`: a future last send counts as due; one sender at a time; an unsent spool goes out with the next due send).

Arc42: `python.usage_reporting.arc42.md` principles 1–5 are untouched (no production change); `system.arc42.md` §3 has no test-structure principle this change touches.

## Goals / Non-Goals

**Goals:**
- The flaky test is deterministic: exactly one sender runs.
- A delivery-test setup step that is accidentally due fails deterministically, not under load only.

**Non-Goals:**
- No production change to `recorder`, `sender` or `spool` (the lease-skip is designed behaviour).
- No change to `end_process()` / `cli()` semantics (they model a real process exit, which abandons its sender after the bounded wait; joining senders there would cost ~10 s per timed-out send in the timeout-style tests).
- No meta-test of the `_spool_one()` guard.

## Decisions

1. **Fix is test-only.** Production matches the delivery spec; the lease loser skipping is correct when processes overlap. (default)
2. **Arrange the flaky test not-due, then move `last_sent` into the future.** `quiet_config(last_sent=_T0)`, `set_now(_T0)`, `_spool_one()`, then `write_config(last_sent=_T0 + 30 days)`, then `start()` — mirrors `test_clock_jump_forward_is_due`. Alternative (keep the due setup and wait for its sender) rejected: the test would no longer pin which `start()` sent. (default)
3. **`_spool_one()` asserts it spooled without sending.** After `start()` + `record()`, before `end_process()`: `recorder._recorder.sending is None`; its docstring states the contract. Options weighed: fix only the one test vs. add the guard (plus fix `test_install_id_rotation`'s due setup). Guard taken: it turns the root cause — an implicitly assumed not-due setup — into a checked precondition. (user)
4. **Guard reads the private `_recorder.sending`** rather than scanning `threading.enumerate()` for `slayer-usage-reporting-send`: daemon senders left by earlier tests in the same xdist worker would give false failures. `start()` sets `sending` synchronously, and `record()`'s `_maybe_send()` cannot fire when `last_sent` equals the injected now. basedpyright runs in `basic` mode (no private-usage report). (default)
5. **`test_install_id_rotation` arranges `last_sent = _T0 + age_days`** (the new now), so its setup is not due; rotation depends on `install_id_created` vs. today, not `last_sent`, so its intent is preserved. (default)
6. **No meta-test for the guard**; its failure on the old arrangement is demonstrated once during implementation. (default)
7. **Assert "exactly one" on the full capture list**: after `end_process()`, `len(usage_reporting_env.capture.requests) == 1`, since `wait_for(count=1)` returns as soon as one request arrives. (codex)

## Risks / Trade-offs

- [The guard reaches into a private attribute; a recorder refactor could rename it] → the guard fails loudly (AttributeError) at the rename, pointing straight at it.
- [Other delivery tests may later arrange a due state on purpose before `_spool_one()`] → they must spool first and make due afterwards (the pattern `test_request_shape` already uses), which the guard enforces.
