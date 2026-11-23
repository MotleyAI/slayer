## Context

See proposal.md (Why) for the symptom. Evidence from the four timed-out jobs:

| Job | State at the kill | Reading |
|---|---|---|
| 88a48d0, 3.14 shard 3 | 100%; killed 1 s into the warnings summary | overrun |
| 59dd096, 3.11 shard 2 | ≤2 tests left; progress 8 s before the kill; 3 of 4 workers gone | overrun |
| 8d1aef9, 3.14 shard 2 | 98% at 16.5 min; previous line took 2 min | overrun (likely) |
| abccc20, 3.14 shard 1 | 99.4% at 13 min, then 6 min silent; 4 workers and a test subprocess alive; no `F` | unexplained |

Across main runs 45033597470, 45027708884, 44996098478 and 44916409383, one shard ranges 1660–3644 worker-seconds (runner hardware) and unit jobs take 8–20 min. Every log has 1.5–4 min gaps at 98–99%: the slowest modules (`test_osi_roundtrip` 300–518 s, `test_release_gate` 250–405 s, `test_dev2120_library_matrix` 115–234 s) sort late in collection order.

Constraints: `--timeout=120` uses pytest-timeout's signal method (graceful, but blind to a thread blocked in C code without returning to the interpreter). A worker's fd 2 is captured while a test runs. xdist's controller-side `pytest_runtest_logstart` carries no worker identity; `report.node` on each report does. pytest-timeout 2.4.0 exposes the `firstresult` hooks `pytest_timeout_set_timer(item, settings)` / `pytest_timeout_cancel_timer(item)` and the public `Settings`.

Architecture: `tests/` and `.github/` lie outside the LikeC4 model (`index.yaml` root `slayer`); no element principles apply and no harness changes. System §3 conventions apply (Pydantic, never dataclasses; imports at the top; keyword arguments).

## Goals / Non-Goals

**Goals:**
- Headroom under the step limit that absorbs runner variance and suite growth.
- No long module in a shard's tail.
- Every future hang leaves stack evidence for every process before the step limit, and a test SIGALRM cannot interrupt fails alone instead of timing out its shard.

**Non-Goals:**
- An automatic, per-run shard plan (the owner refreshes `tests/shard_durations.json` by hand).
- Speeding up the slow tests themselves (`test_osi_roundtrip`'s 600 s marker is DEV-2236's).
- Acting on child processes a test leaves behind.
- Hard-bounding a single-process (non-xdist) run.

## Decisions

1. **4 shards and a 25-minute step limit** (was 3 and 20). Balanced, a shard is ~2050 worker-s at average speed (~11 min on a slow runner). Alternatives: 3 shards plus rebalance (≈3 min headroom on a slow runner, eroding with growth); only a longer limit (slower feedback, a real hang burns longer). (user)
2. **Rebalance once from the four complete main runs above** (all 24 `shard-durations-*` files, per-module mean); later refreshes by hand. Alternative: a `shard-plan` job building each run's plan from recent main artifacts. (user)
3. **Longest-first order within a shard** (part of decision 1's option). Under `--shard`, after deselection, items are stably sorted by their module's weight descending — the weight `assign_shards` uses (stored duration, else the median) — keeping in-module order, ties by module path, so every worker collects the identical order. Without `--shard` the order is unchanged (default: local runs keep their order). (user; scope default)
4. **Watchdog plugin `tests/_hang_watchdog.py`**, registered in `tests/conftest.py`, inert unless its options are given:
   - `--stall-report-after S` (xdist controller): prints (a) every in-flight test that has been running ≥ S (again every further S), and (b) every live worker with no report for ≥ S. In-flight tests are keyed by nodeid (unique per session under `--dist load`), added on `pytest_runtest_logstart`, removed on `pytest_runtest_logfinish` or on a crashed item's report; worker activity comes from `report.node`. (codex: worker identity, stale entries, per-worker stall)
   - `--stack-dump-after S` with `--hang-report-dir DIR`: the controller and every worker arm `faulthandler.dump_traceback_later` (all threads) for the moment S after the controller's session start, which reaches workers through `workerinput`; each writes `DIR/<controller|gwN>-<pid>.txt`, the controller adding its in-flight list. A worker started after the deadline dumps once immediately. (codex: replacement after the deadline) A worker re-arms its dump after pytest's faulthandler plugin cancels it on a test failure (`pytest_exception_interact`), and shares the one faulthandler timer with the hard bound (decision 5). (coderabbit)
   - After the deadline dump the run continues and the step limit kills it — aborting from the controller would wait on the same hung workers. (default)
   - Dumps go to files, not stderr, because a worker's fd 2 is captured during a test. (default)
5. **Hard bound** (xdist workers only, enabled by `--hard-timeout-grace G`). A `tryfirst` `pytest_timeout_set_timer` impl returning `None` arms a deadline of `settings.timeout + G` exactly where pytest-timeout arms its own (so `func_only`, marker keyword forms, `timeout=0`, env and ini precedence follow pytest-timeout by construction), only when the method is `signal`; `pytest_timeout_cancel_timer` cancels it, except the cancel pytest-timeout issues from `pytest_exception_interact` on a test failure, so a failed test's teardown stays under the same deadline. faulthandler's C watchdog thread enforces it without the GIL: the worker arms its one `faulthandler.dump_traceback_later` timer for the earlier of the session deadline (decision 4, dump only) and the running test's deadline (`exit=True`, into `DIR/hard-timeout-<gwN>-<pid>.txt` after the nodeid is written there); between tests, the session deadline. On fire: every thread's stack, then exit 1; xdist reports the test as crashed and starts a replacement worker. A test still running at the session deadline loses its hard bound (under 3 min remain before the step limit, and its stacks are dumped). Alternatives: a Python watchdog thread (cannot run while a test holds the GIL in C); watchdog only (a signal-proof hang still costs the shard); `timeout_method=thread` everywhere (hard-kills on every timeout, losing the graceful signal path, and is itself a Python thread). (user; hook integration: codex; GIL independence and the failure-path cancel: coderabbit, user)
   - Workers only: in a single-process run `os._exit` would lose the whole session. (default)
6. **CI wiring.** Both unit steps pass `--stall-report-after 120 --stack-dump-after 1320 --hang-report-dir "$RUNNER_TEMP/hang-reports" --hard-timeout-grace 30` (dump at 22 min of session time, ~2 min before the 25-min kill); an `if: failure()` step prints every report file into the log, and the directory is uploaded as an artifact with `if: ${{ !cancelled() }}`. Thresholds: 120 s = the per-test timeout; 30 s grace. `latest-deps.yml` gets the same shards and flags (it shares the module groups). (default; artifact upload: codex)
7. **`faulthandler_timeout` (the issue's suggestion) is not used.** It is per test, redundant with the 120 s pytest-timeout, and cannot diagnose an overrun or a stall outside a test. (default)
8. **The unexplained `abccc20` stall:** ship this plan and keep DEV-2391 open through a soak — 3 consecutive green full CI runs on the PR before merge, then about a week of main runs whose `hang-reports` artifacts are checked; any dump is analysed and fixed under this issue before it closes. Alternative: a local reproduction hunt at `abccc20` before implementing (one occurrence in dozens of CI runs; runner-bound timing). (user, after codex)
9. **Tests the watchdog does not need:** a leftover child process — nothing in this change acts on one. (rejected (codex))
10. **No spec delta** (`skip_specs: true`): test and CI tooling, no product behaviour. (default)

## Risks / Trade-offs

- [Longest-first exposes hidden cross-module order dependence] → fix each such test; never restore the old order to hide it.
- [A hard-bound kill skips the worker's teardown: its pytest-postgresql server and temp dirs are orphaned] → runner cleanup removes them; it only happens after the graceful signal path has already failed.
- [Replacement workers re-collect (~30–50 s)] → only on a hard-bound kill.
- [Stall reports print during passing runs at the slow tail] → a few lines per report, and exactly the information the tail needs.
- [Shard membership changes with the rebalance] → one-time; tests that only pass in their old shard are fixed.
- [Two more jobs per CI run] → ~6 runner-minutes per run on a public repo.
