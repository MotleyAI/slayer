## Context

See proposal.md (Why). Current state:

- Only `examples.yml` and `latest-deps.yml` declare `concurrency` (`<name>-${{ github.ref }}`, cancel always — so their scheduled and manual runs on `main` cancel each other). `ci.yml`, the `integration-*.yml` workflows and `pg-facade-e2e.yml` declare none. `bigquery-example` has a job-level group `bigquery-shared-gcp-project` (`cancel-in-progress: false`, `queue: max`).
- `unit-tests` and `integration-tests` are matrices over `python-version: ["3.11", "3.14"]` (unit × `shard: [1..4]`).
- `expensive-job-gate.yml` (reusable) runs `scripts/expensive_ci_gate.py --workflow <file> --job <name>`. On a PR push the job runs iff the PR was just opened/reopened, the push (`before..after`) adds OpenSpec archive files absent from the base, the job never finished on the PR, or its last finished run failed (`cancelled`, `skipped`, unfinished are looked past). History: the workflow's last 30 `pull_request` runs on the head branch from the head repository.
- Unit shards: pytest step medians over 37 runs — 3.11: 14.0 / 12.2 / 12.6 / 12.8 min; 3.14: 15.4 / 12.8 / 11.7 / 13.0. Scoring today's plan against 40 fresh `shard-durations-*` artifacts, shard 1 carries 2,677 summed test-seconds vs 2,898 / 2,547 / 2,878, so it is *not* the heaviest. Cause: `--dist load` sends each worker one contiguous block (`items_per_node // 4`, ~800 items) first, and heaviest-module-first order (DEV-2391) puts a shard's heaviest modules into `gw0`'s block — for shard 1, both ~215 s `test_osi_roundtrip::test_same_results_after_round_trip` tests. Shard 2 has `test_release_gate`'s 170 s test the same way.
- No required status checks on `main` (`.../branches/main/protection/required_status_checks` → 404), so skipped or renamed jobs block nothing.

Architecture: `.github/`, `scripts/` and `tests/` lie outside the LikeC4 model (`index.yaml` root `slayer`); no element principles apply and no harness changes. `system.arc42.md` §3 conventions apply to the gate script and tests (Pydantic, never dataclasses; imports at the top; keyword arguments).

## Goals / Non-Goals

**Goals:**
- A superseded PR run frees its runners as soon as the PR's next run of the same workflow starts.
- Every commit on `main` (and every scheduled / manual run) keeps its own complete result, with no added latency.
- The 3.14 jobs run on a PR only when there is a reason to (gate), and no reason is lost to a cancellation.
- The four unit shards' wall times track their summed test-seconds, and those sums are balanced.

**Non-Goals:**
- Cancelling a path-filtered run when the next push does not trigger that workflow (its gated paths are unchanged, so its result still counts).
- Cleaning up BigQuery / Snowflake objects a cancelled run leaves behind (each run's are uniquely named; the janitor is DEV-2200).
- Paginating the gate's run history past 30 runs.
- A CI `actionlint` step (workflow syntax errors already surface on the PR).
- Changing the integration jobs' `--dist loadscope`.

## Decisions

1. **Shard rebalance = `--dist worksteal` + a refreshed `tests/shard_durations.json`.** The unit pytest command gets `--dist worksteal` (idle workers take half the busiest worker's queue, so a shard's heavy head spreads across workers while module locality mostly holds). The durations file is rebuilt with `python -m tests._sharding tests/shard_durations.json <files>` from the `shard-durations-*` artifacts of ≥ 20 recent successful CI runs, both Pythons. Alternative: refresh only (the issue's agent note) — rejected: shard 1 is not the heaviest by summed seconds today, so new weights leave `gw0`'s contiguous block. (user) `latest-deps.yml`'s sharded unit step, which uses the same shard plan, gets `--dist worksteal` too. (default)
2. **One cancellation block for every workflow with a `pull_request` trigger** — `ci.yml`, `integration-{clickhouse,mysql,snowflake,sqlserver,trino}.yml`, `pg-facade-e2e.yml`, and `examples.yml` / `latest-deps.yml` (their blocks replaced). Alternatives: add `pg-facade-e2e.yml` only; keep the issue's list — both leave two forms and an allowlist in the structural test. (user)
3. **The block:**
   ```yaml
   concurrency:
     group: ${{ github.workflow }}-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}
     cancel-in-progress: ${{ github.event_name == 'pull_request' }}
   ```
   `github.workflow` keeps workflows from cancelling each other. Non-PR runs get a group unique to the run. Alternative: `github.ref` for non-PR runs (the issue's proposed block) — rejected: it serializes `main` pushes, and GitHub keeps one pending run per group, so a third quick push cancels the second. (default)
4. **One gate (`py314-gate`) for all five 3.14 jobs;** any listed job's reason runs all five. (default — issue rule)
5. **Gate input `job` → `jobs`** (newline-separated) in `expensive-job-gate.yml`; the script takes `--job` repeatedly; `bigquery-gate` and `snowflake-gate` migrate. (default)
6. **Job layout.** 3.11 keeps `unit-tests` (matrix `python-version: ["3.11"]`, `shard: [1..4]`) and `integration-tests` (matrix `python-version: ["3.11"]`): API names unchanged, no `needs`. 3.14 gets `unit-tests-py314` (matrix over `shard`) and `integration-tests-py314`, both `needs: py314-gate`, `if: needs.py314-gate.outputs.run == 'true'`. Steps are shared through plain YAML anchors/aliases only — never merge keys (`<<:`), which Actions rejects. Artifact names stay `hang-reports-<py>-<shard>` / `shard-durations-<py>-<shard>`. (default; anchors: codex)
7. **Archive reason is measured from the commit each job last finished on**, not from the push's `before`. Per listed job: run iff it never finished on the PR, its last finished run failed, or archive files absent from the base were added between the head SHA of the run that last finished it and the current head. `cancelled` / `skipped` / unfinished stay non-decisive. So a cancelled archive-push run is re-run by the next push, and a run cancelled before its gate decided causes no extra run. Fetching that SHA fails → run (as for any undeterminable case). Applies to BigQuery and Snowflake too. Alternatives: `cancelled` is decisive → run (needless 3.14 runs whenever a run is cancelled while its gate is queued); keep `before..after` (a cancelled archive push loses its 3.14 run). (user; finding: codex)
8. **Run history is matched to the PR:** a run whose `pull_requests` list is non-empty counts only if it contains this PR's number; an empty list (fork PRs) keeps the head-repository match. (codex)
9. **Gate log:** one line per decision naming the job that triggered the run and why; on skip, each job's last conclusion. (default)
10. **BigQuery / Snowflake cancelled mid-run: accepted.** SIGINT runs pytest's fixture teardown (drops the dataset); a leftover is uniquely named, harmless, and DEV-2200's janitor will clear it. (default)
11. **Integration jobs keep `--dist loadscope`;** only the unit step moves to worksteal. (default)
12. **Shard-median acceptance:** pre-merge evidence from this PR's own CI runs; the 20-run post-merge check is recorded on DEV-2489. (default)

Rejected Codex findings:
- *Path-filtered integration runs are not cancelled by an unrelated push* — rejected (codex): the newer push does not change their gated paths, so the older run's result still counts; cancelling it would discard a valid result.
- *Treating `skipped` as unfinished oscillates* — premise rejected (codex): the gate already looks past `skipped`; its sequence tests are folded into tasks.
- *Paginate history past 30 runs* — rejected (codex): the worst case is one extra 3.14 run per 30 pushes; a test pins the bound.
- *Cancelled BigQuery runs strand datasets* — rejected (codex): datasets are `slayer_test_<uuid>`, created only once the job runs; a leftover cannot block or contaminate another run.

## Risks / Trade-offs

- [GitHub's API names for the split jobs differ from what the gate is given → the gate never sees a finished 3.14 job and always runs] → a structural test derives the names from `ci.yml`, and pr-implement checks them against `gh api` on this PR's first run.
- [worksteal changes worker assignment under the hang watchdog] → watchdog tests run under `--dist worksteal`, including a multi-worker hard kill with redistribution.
- [worksteal exposes hidden cross-module order dependence] → fix each such test; never fall back to `load` to hide it.
- [A shared-runner speed swing (~2×) dwarfs a 1-min shard skew in any single run] → judge balance on medians over many runs, not one.
- [The last-finished SHA is unreachable (force push, GC)] → the fetch fails and the gate runs the jobs.
