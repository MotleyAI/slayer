## Why

CI is the bottleneck for PRs: the org's 20-job runner cap is shared across MotleyAI, a SLayer PR push alone starts ~22 jobs, 42% of jobs wait over 2 min for a runner (p90 10.6 min), and a push takes a median 17.8 min to finish (p90 36.3). Runs for superseded pushes keep running and take runners from the run that counts (5% of PR job-minutes, 23–26 Nov), the Python 3.14 jobs are 37% of PR job-minutes, and unit shard 1 is the slowest on both Pythons.

## What Changes

- Every workflow with a `pull_request` trigger (`ci.yml`, the five `integration-*.yml`, `pg-facade-e2e.yml`, `examples.yml`, `latest-deps.yml`) declares one workflow-level `concurrency` block: a new PR run cancels the same PR's run of that workflow still in progress; runs of any other event never cancel, queue behind, or replace one another. `bigquery-example` keeps its cross-PR job-level queue.
- The Python 3.14 jobs move to their own jobs (`unit-tests-py314`, `integration-tests-py314`) behind the expensive-job gate; the 3.11 jobs keep running on every push without waiting for the gate.
- The expensive-job gate takes several job names, runs when any of them never finished, last failed, or had archive files added since the commit it last finished on; its run history is matched to the PR. The BigQuery and Snowflake gates move to the multi-name input.
- The unit step runs with `--dist worksteal`, and `tests/shard_durations.json` is refreshed from a batch of recent runs, so the four unit shards take about the same time.

## Capabilities

### New Capabilities

None — CI and test tooling only (`skip_specs: true`).

### Modified Capabilities

None.

## Impact

- `.github/workflows/ci.yml`, `expensive-job-gate.yml`, `integration-{clickhouse,mysql,snowflake,sqlserver,trino}.yml`, `pg-facade-e2e.yml`, `examples.yml`, `latest-deps.yml`
- `scripts/expensive_ci_gate.py`, `tests/test_expensive_ci_gate.py`
- `tests/shard_durations.json`, `tests/test_hang_watchdog.py`
- New structural test over `.github/workflows/*.yml`
- No production code, dependency or user-facing change.
