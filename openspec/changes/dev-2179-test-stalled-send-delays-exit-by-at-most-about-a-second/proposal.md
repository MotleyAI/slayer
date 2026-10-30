## Why

`tests/test_usage_reporting_isolation.py::test_stalled_send_delays_exit_by_at_most_about_a_second` flakes on the parallel CI unit job: it compares two whole-process wall-clock samples (usage reporting off vs. a stalled send), and each sample's own work and interpreter teardown drift by seconds under load, while the bounded exit wait it is meant to check is exactly the 1 s cap.

`tests/test_usage_reporting_delivery.py::test_stdio_mcp_broken_pipe_spools` flakes under the same load: it closes stdin right after a request whose response should hit the broken stdout, and when the server reads end of input first it cancels that request and exits cleanly.

## What Changes

- The test measures, inside the CLI subprocess, the time from the command's return to the last exit hook (`exit_wait`), and asserts it stays under 2 s; the usage-reporting-off baseline run and the two-sample comparison are removed.
- The warm-up run, the stalled-send setup, the exit-code and arrival checks, and the final "unsent data stays in the spool" check are unchanged.
- `test_stdio_mcp_broken_pipe_spools` keeps stdin open after its last request and sends `ping` requests until the server exits, so the broken pipe is the server's only exit path; its assertions are unchanged.
- No production code changes: production already matches the delivery spec's bounded exit wait.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — test-only change; `skip_specs: true`.

## Impact

- `tests/test_usage_reporting_isolation.py` and `tests/test_usage_reporting_delivery.py` only.
