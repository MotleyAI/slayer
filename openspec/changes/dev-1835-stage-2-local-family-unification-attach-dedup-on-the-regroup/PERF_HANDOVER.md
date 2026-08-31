# DEV-1835 — perf-corpus re-record (task 3.8), run in parallel

The code + tests are done and merged into the PR; this is the one deferred wrap-up
item. Run it alongside the `/process-reviews` loop and fold the result into the PR
description. Nothing here blocks review.

## What the migration changes, perf-wise (design D3)

- **Windowed producers are now self-contained.** A bare/partitioned windowed measure
  used to render a `_wm_` CTE rooted on the host `_base`; it now renders as a `_cm_`
  producer that derives its own grain rows inline. Cost: **one extra source scan per
  windowed producer group** (the producer no longer shares `_base`'s scan). This is the
  one expected regression.
- **Ranked (`first`/`last`) producers** come out near-identical modulo the CTE name
  (`_rk_` → `_cm_`, internal `_rk_rn`/`_rk_src` → `_ranked_rn`/`_ranked_src`).
- **Executed values are unchanged** (274 executed-value tests + the full non-integration
  suite are green), so this is a timing-only delta — correctness arbitration should find
  nothing.

## How to run

From the repo root inside the poetry venv:

```bash
poetry run python tests/perf/compare/compare.py                 # full A/B audit (~30 min, needs network)
poetry run python tests/perf/compare/compare.py --skip-timing   # correctness only (fast)
poetry run python tests/perf/compare/compare.py --backends sqlite --entries <ids>  # narrow
```

The audit seeds identical DBs, runs the corpus under this branch AND under a
pip-installed baseline (`motley-slayer==0.9.12`, the `PYPI_PIN` in
`tests/perf/compare/audit_params.py`), classifies per-entry value differences with the
pandas oracle, times both sides ABBA×7, and writes `tests/perf/compare/out/` +
`report.md`. Regenerate the human report with `render_report.py`; update
`tests/perf/compare/RESULTS.md`.

## What to record

- Commit the refreshed `tests/perf/compare/out/` + `RESULTS.md` (specific `git add`).
- In the PR description, note the windowed-scan regression (expected, per D3) and confirm
  correctness parity. The audit is vs PyPI `0.9.12`, so its numbers are cumulative since
  that release, not DEV-1835-only — attribute the windowed delta to this change explicitly.

## Cleanup

Delete this file once the perf result is recorded (it is a working note, like the
implementation HANDOVER that preceded it).
