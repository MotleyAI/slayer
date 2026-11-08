# Engine A/B audit: branch vs pinned PyPI release

Plain scripts (never collected by pytest, never run in CI) that empirically
compare this checkout's query engine against `motley-slayer==0.9.12` (the
latest PyPI release at pin time): result values, error behavior, generated
SQL (recorded, informational), and performance.

## Run

```bash
poetry run python tests/perf/compare/compare.py                 # full audit
poetry run python tests/perf/compare/compare.py --skip-timing   # correctness only
poetry run python tests/perf/compare/compare.py --backends sqlite --entries bench_simple_count
poetry run python tests/perf/compare/compare.py --db-url postgresql://u:p@host/slayer_bench --db-type postgres
```

`--db-url` reseeds destructively; the URL must contain `bench`.

### Profile the whole corpus at every scale (overnight)

```bash
poetry run python tests/perf/compare/compare.py --profile-all --subprocess-timeout 21600
```

`--profile-all` times the **entire** corpus (not just the `subset_100k` entries) at
every scale — 10k, 40k, 100k, 1m, 10m — on both backends, and drops the subset pass.
Correctness still runs once at 10k + adversarial. Raise `--subprocess-timeout` for the
big scales, and pass `--retime` to redo timing artifacts that already exist. Seeded DBs
and the baseline venv in `out/` are reused across runs, so a re-profile needs no reseed.

Everything lands in `out/` (gitignored): `report.md`, `correctness.json`,
`perf-flags.json`, `timings.csv`, `run-manifest.json`, per-run JSON artifacts,
seeded DB files, and the reusable baseline venv (`out/pypi-venv`).

## Pieces

- `corpus.py` — ~100 shared queries (pure data), model dicts, adversarial dataset
- `runner.py` — executes the corpus under either interpreter (baseline venv / poetry venv)
- `classify.py` — slayer-free comparison logic (taxonomy, tolerance, perf flags)
- `oracle.py` — pandas ground truth from explicit per-entry specs, arbitrates who is wrong
- `compare.py` — orchestrator: seeding, ABBA timing, classification, report
- `audit_params.py` — scales, repeats, thresholds, backends

## Unit tests for the tooling

```bash
poetry run pytest tests/perf/compare/test_logic.py -o addopts=""
```
