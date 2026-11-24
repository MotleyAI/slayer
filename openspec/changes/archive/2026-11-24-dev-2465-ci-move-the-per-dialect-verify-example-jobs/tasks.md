## 1. Tests (pr-tests stage)

- [x] 1.1 Add no test files: by design decision 2 the verification is this PR's own CI run of `examples.yml` (all
      five legs). Mark this done in pr-tests with no test diff; pr-implement then follows from tasks 2-5.

## 2. New examples workflow

- [x] 2.1 Create `.github/workflows/examples.yml` per design decisions 3-6 and 9: triggers, concurrency, and one
      `verify-example` matrix job (`include` over `mysql`/`clickhouse`/`trino`/`postgres` at 180 and `sqlserver` at
      300) carrying the existing `integration-mysql.yml` `verify-example` steps unchanged except the parameterised
      directory and `--wait-timeout`, with a terse header comment and the general "drivers live inside the image"
      comment; verify it parses (`poetry run python -c "import yaml,sys; yaml.safe_load(open(sys.argv[1]))" <file>`)
      and its `on.pull_request.paths` are exactly `Dockerfile`, `examples/**`, `.github/workflows/examples.yml`,
      and that no step carries `continue-on-error` or an `|| true`, so a failed build / seed / `start.sh` ingest /
      `slayer serve` startup / `verify.py` still fails the leg.
- [x] 2.2 Add `examples/postgres/slayer_data/.gitignore` as a byte copy of `examples/mysql/slayer_data/.gitignore`;
      verify with `cmp` and that `git check-ignore` leaves the `.gitignore` itself tracked.

## 3. Per-dialect workflows

- [x] 3.1 In each of `integration-{mysql,clickhouse,sqlserver,trino}.yml`: delete `jobs.verify-example`; drop
      `examples/seed.py`, `examples/verify_common.py` and `examples/<db>/**` from both the `pull_request` and `push`
      path lists; reword the path-list comment to the suite alone and the header comment to the pytest suite only,
      pointing at `examples.yml` for the Docker example (keep SQL Server's "first Tier-1 CI" note); verify each parses
      and has exactly one job, `pytest`.
- [x] 3.2 Reword the SQL Server pytest ODBC-step comment (design decision 11) to contrast with the Docker example in
      `examples.yml`; verify `grep -n verify .github/workflows/integration-*.yml` shows no stale reference.

## 4. ci.yml comment

- [x] 4.1 Replace the stale `integration-examples` comment block above `bigquery-gate` in `ci.yml` with a one-line
      pointer to `examples.yml`; verify `git diff .github/workflows/ci.yml` touches only that comment and
      `bigquery-example` / `examples/verify_common.py` are unchanged.

## 5. Gates and live proof

- [x] 5.1 Run `poetry run pytest tests/test_architecture_wiring.py`, `poetry run ruff check slayer/ tests/`,
      `poetry run la-arch-check` and `la-typecheck`; all green.
- [x] 5.2 After push, every `examples.yml` leg on the PR is green and no `verify-example` job runs in the four
      per-dialect workflows. If the Postgres leg fails, fix only `examples/postgres/{docker-compose.yml,start.sh,
      slayer_data/}` (design decision 12); a failure needing a change to `examples/postgres/verify.py`,
      `examples/verify_common.py` or `slayer/` stops the flow for the user's call.
