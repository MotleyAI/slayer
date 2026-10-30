## Context

See proposal.md — Why. Root cause, reproduced locally with a scratchpad-only probe (a `sitecustomize` on the child's `PYTHONPATH` timestamping process start and every `atexit` handler; a pytest plugin wrapping the test's `run_slayer` to record spawn/reap): on 32 cores with 64 CPU burners and 16 parallel pytest runs, 11/48 runs of the old test failed, with gaps of up to 4.5 s.

- The `_at_exit` hook (flush + `join(EXIT_WAIT)`) took 1.000–1.012 s in every stalled run.
- The gap came from the runs' own variance: the command's work phase (1.1–4.2 s on, 0.9–4.3 s off) and interpreter teardown after the last `atexit` handler (0.3–1.4 s, in off runs too).
- Parent-observed reap minus the child's "work finished" would still exceed 2.0 s in 8/48 runs.

Production matches `openspec/specs/usage-reporting/delivery/spec.md` ("Background send with a bounded exit wait"). Exit order (CPython): the main module's code returns → `threading._shutdown` (joins non-daemon threads) → `atexit` handlers, LIFO (`recorder._at_exit` → `shutdown()` → `join(1.0)`) → teardown, which never waits for the daemon sender.

Arc42: `python.usage_reporting.arc42.md` principle 1 (fail-silent; the one bounded send wait, enforced by this test file) stays enforced and unchanged; `system.arc42.md` §3 has no principle this test-only change touches.

`test_stdio_mcp_broken_pipe_spools` races two shutdown paths. The MCP SDK cancels in-flight handlers when stdin hits EOF (`Server.run`), so if EOF is read before request 99's response is written, no write fails and the server takes the clean end-of-input path (exit 0, `cli:mcp` ok = 1). Probe (a `-c` wrapper delaying the second `list_datasources` call; no production edit): the old protocol fails at a 0 s and a 1 s delay. The server does not exit on a broken pipe while stdin stays open (its stdin reader blocks in a worker thread), so the test must keep sending lines rather than wait.

## Goals / Non-Goals

**Goals:**
- The test bounds the interval the spec scenario names — "exits within about one second of finishing its work" — and is robust to runner load.
- It still fails when the send is not capped (unbounded join) or when the sender would block exit another way (non-daemon thread).

**Non-Goals:**
- No production change; no delivery-spec change.
- Interpreter teardown after the last `atexit` handler is not measured (decision 3).
- The sibling in-process `shutdown()` timing tests in `tests/test_usage_reporting_delivery.py` (`test_timed_out_send_keeps_spool`, `test_no_wait_at_exit_without_send_in_flight`) are untouched: they passed 64/64 under the same load.
- No lower bound on the wait.

## Decisions

1. **Fix is test-only.** Production matches the delivery spec; the probe shows the wait is exactly the 1.0 s cap. (default)
2. **Measure inside the CLI subprocess, not against a fastest-of-N off baseline.** The on run's own work phase varies by ~3 s under load, so no baseline absorbs it. (default)
3. **The window ends at the last `atexit` handler, in the child — not at the parent's reap.** Options weighed: A (last atexit handler: robust, 1.00–1.01 s under load; excludes teardown, which does not wait for the daemon sender) vs. B (parent reap: covers teardown, but its 0.3–1.4 s load noise would need a ~3 s bound). A taken. (user)
4. **The window starts in a `finally` around `runpy.run_module("slayer", run_name="__main__", alter_sys=True)`**, run via `[sys.executable, "-c", WRAPPER, *args]`; the wrapper registers its exit-stamp `atexit` handler before any slayer import, so it runs last (LIFO). Starting at the module's return rather than at the first exit hook keeps `threading._shutdown` (where a non-daemon sender would block) inside the window. (default)
5. **The metric is named for what it measures:** `exit_wait`, "seconds from the command's return to the last exit hook"; teardown exclusion is recorded here. (codex)
6. **Two stamp files under the test's `tmp_path`, each written once** (`work_done`, `exit`), not stdout/stderr, so the CLI's streams are untouched; the helper requires both to exist and parse as a float, failing with the subprocess's stderr otherwise. (codex)
7. **Bound stays `< 2.0` s** — "about one second" plus margin, now on a quantity measured at 1.00–1.01 s under load. (default)
8. **No lower bound**: the spec says "at most about one second"; a lower bound would pin the implementation. (default)
9. **The helper is private to the test module** (single caller). (default)
10. **Rejected (codex): keep an end-to-end parent-observed exit check against several unstalled samples** — it reintroduces the two-noisy-samples comparison (teardown noise 0.3–1.4 s) that decision 3 rules out. rejected (codex)
11. **`test_stdio_mcp_broken_pipe_spools` never sends EOF while a response is pending.** After request 99 it sends a `ping` request every 0.1 s until the server exits (30 s deadline, stopping on `BrokenPipeError`); each ping forces another response write, and the next line read lets the cancelled stdin reader return. Assertions unchanged. (user: scope; default: approach)

## Risks / Trade-offs

- [A regression that delays exit only during interpreter teardown would not be caught] → the sender is a daemon thread and teardown does not wait for it; a non-daemon sender is caught in `threading._shutdown`, inside the window (verified by mutation probe, tasks 2.2).
- [The `-c` + `runpy` entry differs from `-m slayer` (e.g. `sys.argv[0]`)] → `run_module(..., run_name="__main__", alter_sys=True)` runs `slayer/__main__.py` the same way; only argparse's program name could differ, and the test asserts no help text.
