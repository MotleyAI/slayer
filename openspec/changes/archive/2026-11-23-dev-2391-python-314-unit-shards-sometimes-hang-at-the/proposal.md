## Why

Unit shards on both Python legs hit the 20-minute step limit at 98–100%. Three of the four timed-out logs show a run that simply ran out of time: the step has no headroom for the ~2× runner-speed variance, the shard plan is stale (one shard +47%), and the longest modules start last. The fourth (`abccc20`, shard 1: silent for 6 min with every worker alive) is unexplained, and nothing today tells a hang from an overrun or bounds a test that SIGALRM cannot interrupt.

## What Changes

- CI unit tests run as 4 duration-balanced shards per Python version (was 3), with a 25-minute step limit (was 20); `latest-deps.yml` follows the same 4 shards.
- `tests/shard_durations.json` is rebalanced from four complete main runs.
- Under `--shard`, each shard runs its heaviest modules first (longest-first), so no long module starts in the tail.
- A test-suite watchdog plugin, inert unless its options are given, reports long-running in-flight tests and silent workers, dumps every process's thread stacks before the step limit, and hard-bounds a test that outlives its pytest-timeout plus a grace (stack dump, then the worker exits; xdist reports the test as crashed).
- CI prints and uploads the watchdog's hang reports.

## Capabilities

### New Capabilities

None — test and CI tooling only (`skip_specs: true`).

### Modified Capabilities

None.

## Impact

- `.github/workflows/ci.yml`, `.github/workflows/latest-deps.yml`
- `tests/_sharding.py`, `tests/shard_durations.json`, `tests/test_sharding.py`
- New `tests/_hang_watchdog.py` (registered in `tests/conftest.py`) and `tests/test_hang_watchdog.py`
- Two more unit jobs per CI run; no production code, dependency or user-facing change.
