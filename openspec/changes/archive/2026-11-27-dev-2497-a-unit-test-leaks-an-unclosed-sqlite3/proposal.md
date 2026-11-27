## Why

On the Python 3.14 CI jobs an unrelated unit test intermittently fails with
`Exception ignored while finalizing database connection <sqlite3.Connection …>`
(DEV-2497; three sightings, all on shard 4, in both `ci.yml` and `latest-deps.yml`).
Some connection is left unclosed, and it is finalized at whatever garbage collection
comes next, so the failure lands on a test that has nothing to do with it and names
no source.

Two ways a disposed engine leaves a connection open were reproduced in isolation:

- a connection still checked out when its engine is disposed is returned to a pool
  nobody owns any more, and stays open until it is collected. A blocking call
  abandoned by cancellation, followed by an eviction, invalidation or cache reset,
  reaches this;
- an engine keeps working after it is disposed, so a holder of a disposed engine
  keeps opening connections that nothing will ever close.

Neither is proven to be what failed in CI: the failure never reproduced locally
(five shard runs on 3.14). So the change also makes the next leak name its source.

## What Changes

- A disposed synchronous engine closes every connection it opened: idle ones at
  once, one in use when it is returned, and any it opens afterwards when that one is
  returned. A connection in use keeps working until it is returned.
- One disposal routine in the engine factory serves every synchronous disposal: the
  factory's own (eviction, invalidation, cache reset, lost build race) and the
  client's private in-memory engine.
- The test harness records where every `sqlite3` connection was opened. A connection
  that dies unclosed fails the run on every Python version, naming the test and the
  code that opened it; Python's own 3.13+ warning carries the same opener.
- `tests/test_engine_factory.py::TestResetCacheDisposal::test_checked_out_connection_finishes_after_reset`
  is aligned with the new behaviour (user-approved).

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `database/execution`: "Engines have one disposing owner" gains the rule that
  disposing a synchronous engine closes every connection it opened, with scenarios
  for a late return, a blocking call abandoned by cancellation, and use after
  disposal.

## Impact

- `slayer/database/engine_factory.py`, `slayer/database/client.py` — the disposal
  routine and its callers.
- `tests/_sqlite_connection_law.py` (new pytest plugin), `tests/conftest.py`,
  `tests/test_law_resource_ownership.py`, `tests/test_engine_factory.py`, plus new
  tests for the spec scenarios.
- No public API, documentation, CI workflow, arc42 or model change. Element
  principles that apply: `python.database` §3.3 "One engine owner" and
  `python.storage` §3.1 "One sqlite door".
