## Context

See proposal.md - Why. The four `verify-example` jobs are identical except for the example directory, SQL Server's
seed `--wait-timeout 300` (180 elsewhere) and one SQL Server comment. `latest-deps.yml` is the trigger-shape model
(narrow-path `pull_request` + `schedule` + `workflow_dispatch` + cancelling `concurrency`). No arc42 principle or
LikeC4 element covers `.github/`, and no spec claims where the examples run, so no harness changes.

## Goals / Non-Goals

**Goals:**
- One matrix job carrying the existing steps unchanged, per-dialect values parameterised.
- A PR touching only `slayer/**` triggers no Docker-example build.
- Each leg fails on: image build, seed, `start.sh` ingest, `slayer serve` startup, or a `verify.py` failure.

**Non-Goals:**
- Notification on a failed scheduled run beyond GitHub's default.
- Changing the per-dialect pytest suites, any `verify.py`, or `examples/verify_common.py`.
- Publishing the Docker image.
- A unit test pinning the workflow layout (user decision 2).
- A new doc sentence about `examples.yml` (decision 7).

## Decisions

1. **Postgres joins the matrix** as a fifth leg, with `examples/postgres/slayer_data/.gitignore` (byte copy of the
   MySQL one) so the chown step finds the directory; vs keeping four legs. Its quickstart is linked from the docs and
   otherwise unverified. (user)
2. **Verification = this PR's own CI run** (it touches `examples.yml`, so all five legs run), no structural YAML unit
   test; vs a test pinning triggers/matrix/paths. "don't overengineer it". (user)
3. **Triggers:** `pull_request` (`branches: ['**']`, paths `Dockerfile`, `examples/**`,
   `.github/workflows/examples.yml`), `schedule`, `workflow_dispatch`; no `push: main` - the daily run makes it
   redundant, per the issue and `latest-deps.yml`. (default)
4. **Cron `0 6 * * *`**, the `latest-deps.yml` slot; each job gets its own runner, so no contention. (default)
5. **Matrix via `include` entries `{db, wait-timeout}`** (180; 300 for `sqlserver`), with
   `defaults.run.working-directory: examples/${{ matrix.db }}`; the `verify.py` step runs from the repo root as
   today. One place per leg, vs a lookup expression. (default)
6. **`fail-fast: false`**, so one broken example doesn't hide the others; per the issue. (default)
7. **No new doc sentence**: the issue asks only to fix stale placement claims, and none exist outside workflow
   comments. (default)
8. **`skip_specs: true`**: CI placement is tooling, not specified behaviour. (default)
9. **Concurrency `examples-${{ github.ref }}`, `cancel-in-progress: true`**, as in `latest-deps.yml`. (default)
10. **Per-dialect workflows keep their pytest path gates** minus `examples/<db>/**`, `examples/seed.py`,
    `examples/verify_common.py` - no suite under `tests/` nor code under `slayer/` reads them (re-checked on
    `b2646db6b`). Their header and path-list comments describe only the pytest suite and point at `examples.yml`.
    (default)
11. **The SQL Server pytest ODBC-step comment** (which contrasts itself with the co-located `verify-example`) is
    reworded to contrast with the Docker example in `examples.yml`; otherwise it would still describe the old
    placement. (codex)
12. **Postgres remediation scope**: if the new leg fails, this PR may fix only the Postgres example's own setup
    (`docker-compose.yml`, `start.sh`, `slayer_data/`). A failure needing a change to `examples/postgres/verify.py`,
    `examples/verify_common.py` or `slayer/` stops the flow as a suspected unrelated gap for the user's call. (codex)

## Risks / Trade-offs

- [A `slayer/` refactor breaking an example's `start.sh` surfaces in the next daily run, not on its PR] → a daily
  run limits the suspects to one day's merges; accepted in the issue.
- [The Postgres leg has never run and may fail first time] → decision 12 bounds what this PR fixes.
- [Scheduled-run failures notify only whoever last edited the `cron` line] → accepted non-goal; possible follow-up.
