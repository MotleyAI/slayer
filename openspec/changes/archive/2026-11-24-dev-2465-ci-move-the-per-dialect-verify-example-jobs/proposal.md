## Why

Four per-dialect workflows each run a `verify-example` job that is path-gated on `slayer/**`, so almost every PR
builds and boots four Docker stacks. The sibling pytest suites already cover the dialect behaviour these jobs check;
what the jobs alone protect is example rot (the root `Dockerfile`, each quickstart's compose / `start.sh` / seed
wiring, the SQL Server image's ODBC install), which a daily run catches well enough.

## What Changes

- New `.github/workflows/examples.yml`: one `verify-example` job, a `fail-fast: false` matrix over `mysql`,
  `clickhouse`, `sqlserver`, `trino` and `postgres`, triggered daily, on manual dispatch, and on PRs touching only
  `Dockerfile`, `examples/**` or the workflow itself (never `slayer/**`); a concurrency group cancels a superseded PR
  run.
- Postgres joins as a new leg (never ran in CI before), with `examples/postgres/slayer_data/.gitignore`.
- The four `integration-{mysql,clickhouse,sqlserver,trino}.yml` lose their `verify-example` job and the path gates
  only it needed (`examples/<db>/**`, `examples/seed.py`, `examples/verify_common.py`); their comments describe only
  the pytest suite.
- The stale comment block in `ci.yml` about the old example placement points at `examples.yml`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. CI placement is tooling, not specified behaviour (`skip_specs: true`).

## Impact

- `.github/workflows/examples.yml` (new), `integration-{mysql,clickhouse,sqlserver,trino}.yml`, `ci.yml` (comment).
- `examples/postgres/slayer_data/.gitignore` (new).
- Unchanged: every `examples/<db>/verify.py`, `examples/verify_common.py`, `bigquery-example`, the pytest suites,
  `slayer/`, docs.
- Trade-off: a `slayer/` refactor that breaks an example's `start.sh` surfaces in the next daily run, not on its PR.
