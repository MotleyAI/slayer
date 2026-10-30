## Why

`tests/test_usage_reporting_isolation.py::test_stalled_send_delays_exit_by_at_most_about_a_second` flakes on the parallel CI unit job: it compares two whole-process wall-clock samples (usage reporting off vs. a stalled send), and each sample's own work and interpreter teardown drift by seconds under load, while the bounded exit wait it is meant to check is exactly the 1 s cap.

## What Changes

- The test measures, inside the CLI subprocess, the time from the command's return to the last exit hook (`exit_wait`), and asserts it stays under 2 s; the usage-reporting-off baseline run and the two-sample comparison are removed.
- The warm-up run, the stalled-send setup, the exit-code and arrival checks, and the final "unsent data stays in the spool" check are unchanged.
- No production code changes: production already matches the delivery spec's bounded exit wait.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — test-only change; `skip_specs: true`.

## Impact

- `tests/test_usage_reporting_isolation.py` only.
