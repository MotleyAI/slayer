## Why

`tests/test_usage_reporting_delivery.py::test_last_send_in_the_future_counts_as_due` flakes under full-suite `-n auto` load. Its setup step is already due, so the setup process's own background sender can claim the still-empty spool and hold the send lease when the test's `start()` sends; that second sender then skips on the live lease, and nothing reaches the capture server.

## What Changes

- `test_last_send_in_the_future_counts_as_due` spools in a not-due state, and only then moves the last-send time into the future, so the act step's `start()` is the only sender; it asserts exactly one request on the capture's full list after `end_process()`.
- `_spool_one()` (the delivery tests' setup helper) asserts that it spooled without launching a send, so a setup that is accidentally due fails deterministically instead of racing.
- `test_install_id_rotation` spools in a not-due state (its setup was due, which the new guard rejects); its rotation assertions are unchanged.
- No production code changes: production behaviour already matches the delivery spec (the loser of the send lease skips; its unsent spool goes out with the next due send).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None — test-only change; `skip_specs: true`.

## Impact

- `tests/test_usage_reporting_delivery.py` only.
