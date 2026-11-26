## 1. Tests (pr-tests stage; all fail before implementation)

- [x] 1.1 `tests/test_expensive_ci_gate.py`, several jobs: runs when any listed job never finished; runs when any last finished with a failure; skips when all last succeeded, the reason naming each job's last conclusion; the run reason names the deciding job; `cancelled` / `skipped` / unfinished are looked past — verify the new tests fail now
- [ ] 1.2 Gate sequences (design decision 7): success → gated skip (jobs `skipped`) → ordinary push skips; success → a run cancelled before its gate decided (jobs `cancelled`) → ordinary push skips; success → archive push whose jobs are `cancelled` → ordinary push runs (archive files added since the last-finished head SHA); an unfetchable last-finished SHA runs — verify they fail now
- [x] 1.3 Gate history scoping (decision 8): a run whose `pull_requests` lists another PR number is ignored; an empty `pull_requests` keeps the head-repository match; a success older than the 30-run lookback counts as never finished — verify the new ones fail now
- [x] 1.4 `main()` with repeated `--job` writes one `run=` line and logs the decision — verify it fails now
- [x] 1.5 New structural test over `.github/workflows/*.yml` (PyYAML; `on:` parses as key `True`): every workflow with a `pull_request` trigger has exactly the decision-3 block at workflow level; `bigquery-example` keeps its job-level group (`bigquery-shared-gcp-project`, `cancel-in-progress: false`, `queue: max`); `unit-tests` / `integration-tests` have no `needs` and only `python-version: ["3.11"]`; `unit-tests-py314` / `integration-tests-py314` need `py314-gate` and carry `if: needs.py314-gate.outputs.run == 'true'`; the names passed to `py314-gate` equal the API names derived from those two jobs' matrices; every caller of `expensive-job-gate.yml` passes `jobs`; the unit pytest commands in `ci.yml` and `latest-deps.yml` use `--dist worksteal`; no workflow uses a YAML merge key (`<<:`) — verify it fails now
- [x] 1.6 `tests/test_hang_watchdog.py`: the stall report, the deadline stack dump and the hard bound also hold under `--dist worksteal`; plus a `-n 2` worksteal case with enough queued tests where one worker is hard-killed, the remaining tests are redistributed, the replacement worker keeps the session deadline, and every other test passes — verify (they may already pass; the multi-worker case is new)

## 2. Gate

- [ ] 2.1 `scripts/expensive_ci_gate.py`: repeated `--job`; per-job decision (never finished / last failed / archive added since the last-finished head SHA, fetched; fetch failure → run); history filtered by `pull_requests` PR number when non-empty; one reason line per decision; module docstring updated — verify 1.1–1.4 pass
- [ ] 2.2 `.github/workflows/expensive-job-gate.yml`: input `job` → `jobs` (newline-separated), passed as repeated `--job`; header comment updated; `ci.yml` `bigquery-gate` and `integration-snowflake.yml` `snowflake-gate` pass `jobs` — verify by 1.5

## 3. Workflows

- [ ] 3.1 `ci.yml`: split the 3.14 work into `unit-tests-py314` (matrix `shard`) and `integration-tests-py314`, add `py314-gate` (`permissions: contents: read, actions: read`) with the five API names; 3.11 jobs keep their names; shared steps via plain anchors/aliases — verify by 1.5
- [ ] 3.2 Add the decision-3 `concurrency` block to `ci.yml`, `integration-{clickhouse,mysql,snowflake,sqlserver,trino}.yml`, `pg-facade-e2e.yml`, and replace the blocks in `examples.yml` / `latest-deps.yml` (and their "cancels its run in flight" comments) — verify by 1.5
- [ ] 3.3 `--dist worksteal` on the unit pytest step of `ci.yml` (both jobs) and `latest-deps.yml` — verify by 1.5 and 1.6
- [ ] 3.4 Run `actionlint` locally over `.github/workflows/` — verify no findings in the changed workflows
- [ ] 3.5 `docs/database-support.md` gate paragraph: still accurate for BigQuery / Snowflake; update only if the wording no longer matches — verify by review

## 4. Shard rebalance

- [ ] 4.1 Rebuild `tests/shard_durations.json` from the `shard-durations-*` artifacts of ≥ 20 recent successful CI runs (both Pythons): `gh run download <id> -p 'shard-durations-*' -D <dir>/<id>` per run, then `poetry run python -m tests._sharding tests/shard_durations.json <dir>/*/*/shard-durations.json` — verify the file lists every module the runs measured and the 4-shard plan's stored loads are within 2% of each other
- [ ] 4.2 Fix any test that fails only under worksteal or the new shard membership (never fall back to `--dist load`) — verify the PR's CI is green

## 5. Gates and CI evidence

- [ ] 5.1 Lint, `la-typecheck` (no new errors vs the baseline), `la-arch-check` — verify all pass
- [ ] 5.2 On this PR's first CI run, compare the gate's five names with the job names `gh api repos/MotleyAI/slayer/actions/runs/<id>/jobs` reports — verify they match exactly
- [ ] 5.3 On this PR: a second push cancels the first push's `CI` run still in progress (and no other workflow's run); an ordinary push after a green 3.14 run skips all five 3.14 jobs with the reason in the `py314-gate` log; the 3.11 jobs start without waiting for the gate — verify from the runs
- [ ] 5.4 Shard balance evidence from this PR's own CI runs (pytest step per shard) — verify and record in the PR description; the 20-run post-merge median check is recorded on DEV-2489
